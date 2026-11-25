using Messaging.Abstractions.Handlers;
using Messaging.Abstractions.Idempotency;
using Messaging.Abstractions.Producers;
using Messaging.RabbitMQ.Configuration;
using Messaging.RabbitMQ.Connections;
using Messaging.RabbitMQ.Consumers;
using Messaging.RabbitMQ.Dispatching;
using Messaging.RabbitMQ.Idempotency;
using Messaging.RabbitMQ.Producers;
using Messaging.RabbitMQ.Retry;
using Messaging.RabbitMQ.Rpc;
using Messaging.RabbitMQ.Topology;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
namespace Messaging.RabbitMQ.DependencyInjection;

public static class ServiceCollectionExtensions
{
    private const string ListenerKey = "Messaging.Listeners";

    public static IServiceCollection AddRabbitMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<RabbitMessagingOptions>()
            .Bind(configuration)
            .Validate(o => !string.IsNullOrWhiteSpace(o.Host), "RabbitMQ Host is required.")
            .Validate(o => o.Port is > 0 and <= 65535, "Invalid RabbitMQ port.")
            .Validate(o => o.Rpc.DefaultTimeout > TimeSpan.Zero, "RPC timeout must be positive.")
            .Validate(o => o.Consumer.Credit > 0 && o.Consumer.MaxConcurrency > 0, "Consumer limits must be positive.")
            .Validate(o => Enum.IsDefined(o.Consumer.QueueType), "Invalid consumer queue type.")
            .Validate(o => !o.Consumer.DeadLetter.Enabled || !string.IsNullOrWhiteSpace(o.Consumer.DeadLetter.Suffix), "DLQ suffix is required when dead-lettering is enabled.")
            .Validate(o => o.Consumer.Retry.MaxAttempts >= 1, "Retry MaxAttempts must be >=1.")
            .Validate(o => o.Consumer.Retry.InitialDelay > TimeSpan.Zero &&
                o.Consumer.Retry.MaxDelay >= o.Consumer.Retry.InitialDelay &&
                o.Consumer.Retry.MaxDelay <= TimeSpan.FromMilliseconds(uint.MaxValue - 1), "Retry delays must be positive, ordered, and within the timer range.")
            .Validate(o => double.IsFinite(o.Consumer.Retry.BackoffMultiplier) && o.Consumer.Retry.BackoffMultiplier >= 1, "Retry backoff multiplier must be finite and at least 1.")
            .Validate(o => o.Idempotency.ProcessingTimeout >= TimeSpan.FromMilliseconds(3) &&
                o.Idempotency.ProcessingTimeout <= TimeSpan.FromDays(365), "Idempotency processing timeout must be between 3 milliseconds and 365 days.")
            .Validate(o => o.Idempotency.RetentionPeriod > TimeSpan.Zero &&
                o.Idempotency.RetentionPeriod <= TimeSpan.FromDays(36500), "Idempotency retention must be positive and at most 36,500 days.")
            .Validate<IServiceProviderIsKeyedService>((o, registrations) =>
                !string.IsNullOrWhiteSpace(o.Idempotency.Provider) &&
                registrations.IsKeyedService(typeof(IMessageDeduplicationStore), o.Idempotency.Provider),
                "Idempotency.Provider must match an explicitly registered keyed deduplication store.")
            .ValidateOnStart();

        services.AddSingleton<RabbitConnectionManager>();

        services.AddSingleton<RabbitTopologyManager>();

        services.AddSingleton<RpcTimeoutManager>();

        services.AddSingleton<RabbitRpcClient>();

        services.TryAddSingleton<RabbitMessageProducer>();
        services.AddSingleton<IMessageProducer>(provider => provider.GetRequiredService<RabbitMessageProducer>());
        services.TryAddSingleton<IEventPublisher>(provider => provider.GetRequiredService<RabbitMessageProducer>());

        services.AddSingleton<IRetryExceptionClassifier, DefaultRetryExceptionClassifier>();

        services.TryAddKeyedSingleton<IMessageDeduplicationStore, InMemoryMessageDeduplicationStore>("InMemory");
        services.TryAddSingleton<IMessageDeduplicationStore>(ResolveDeduplicationStore);

        return services;
    }
    public static IServiceCollection AddRabbitMessageContracts(this IServiceCollection services, Action<MessageTypeRegistry> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var registry = new MessageTypeRegistry();

        configure(registry);

        services.AddSingleton(registry);

        return services;
    }
    public static IServiceCollection AddRabbitListener(this IServiceCollection services, Action<RabbitListenerBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new RabbitListenerBuilder();

        configure(builder);

        return RegisterListeners(services, builder.Build());
    }

    public static IServiceCollection AddRabbitEventListener(this IServiceCollection services, Action<RabbitEventListenerBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new RabbitEventListenerBuilder();
        configure(builder);
        return RegisterListeners(services, builder.Build());
    }

    private static IServiceCollection RegisterListeners(IServiceCollection services, ListenerRegistration registration)
    {
        services.AddKeyedSingleton(ListenerKey, registration);
        services.TryAddSingleton<ListenerRegistration>(provider =>
        {
            var combined = new ListenerRegistration();
            var names = new HashSet<string>(StringComparer.Ordinal);
            var subscriptions = new HashSet<string>(StringComparer.Ordinal);
            foreach (var listener in provider.GetKeyedServices<ListenerRegistration>(ListenerKey))
            {
                foreach (var queue in listener.Queues)
                {
                    if (!names.Add(queue.Name) || (queue.EventBinding is { } binding && !subscriptions.Add(binding.StableName)))
                    {
                        throw new InvalidOperationException($"Duplicate RabbitMQ listener for '{queue.Name}'.");
                    }

                    combined.Queues.Add(queue);
                }
            }

            return combined;
        });

        foreach (var queue in registration.Queues)
        {
            foreach (var handler in queue.Handlers)
            {
                services.TryAddScoped(handler.HandlerType);

                if (queue.EventBinding is not null)
                {
                    continue;
                }

                var serviceType = handler.ResponseType is null ? typeof(ICommandHandler<>).MakeGenericType(handler.MessageType) : typeof(IRequestHandler<,>).MakeGenericType(handler.MessageType, handler.ResponseType);

                services.AddScoped(serviceType, handler.HandlerType);
            }
        }

        services.TryAddSingleton<IMessageDispatcher, MessageDispatcher>();

        services.TryAddSingleton<RabbitQueueConsumer>(provider => new RabbitQueueConsumer(
            provider.GetRequiredService<RabbitConnectionManager>(),
            provider.GetRequiredService<IMessageDispatcher>(),
            provider.GetRequiredService<IOptions<RabbitMessagingOptions>>(),
            provider.GetRequiredService<ILogger<RabbitQueueConsumer>>(),
            provider.GetRequiredService<ILoggerFactory>(),
            provider.GetRequiredService<IRetryExceptionClassifier>(),
            ResolveDeduplicationStore(provider)));

        services.TryAddSingleton<RabbitRpcResponder>();

        services.TryAddSingleton<MessageListenerService>();
        services.AddHostedService(provider => provider.GetRequiredService<MessageListenerService>());

        return services;
    }

    private static IMessageDeduplicationStore ResolveDeduplicationStore(IServiceProvider provider)
    {
        var options = provider.GetRequiredService<IOptions<RabbitMessagingOptions>>().Value;
        return provider.GetRequiredKeyedService<IMessageDeduplicationStore>(options.Idempotency.Provider);
    }
}
