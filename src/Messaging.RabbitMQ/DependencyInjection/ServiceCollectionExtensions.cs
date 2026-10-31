using Messaging.Abstractions.Handlers;
using Messaging.Abstractions.Producers;
using Messaging.RabbitMQ.Configuration;
using Messaging.RabbitMQ.Connections;
using Messaging.RabbitMQ.Consumers;
using Messaging.RabbitMQ.Dispatching;
using Messaging.RabbitMQ.Producers;
using Messaging.RabbitMQ.Rpc;
using Messaging.RabbitMQ.Topology;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
namespace Messaging.RabbitMQ.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddRabbitMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<RabbitMessagingOptions>()
            .Bind(configuration)
            .Validate(o => !string.IsNullOrWhiteSpace(o.Host), "RabbitMQ Host is required.")
            .Validate(o => o.Port is> 0 and <= 65535, "Invalid RabbitMQ port.")
            .Validate(o => o.Rpc.DefaultTimeout > TimeSpan.Zero, "RPC timeout must be positive.")
            .Validate(o => o.Consumer.Credit > 0 && o.Consumer.MaxConcurrency > 0, "Consumer limits must be positive.")
            .Validate(o => o.Consumer.Retry.MaxAttempts >= 1, "Retry MaxAttempts must be >=1.")
            .ValidateOnStart();

        services.AddSingleton<RabbitConnectionManager>();

        services.AddSingleton<RabbitTopologyManager>();

        services.AddSingleton<RpcTimeoutManager>();

        services.AddSingleton<RabbitRpcClient>();

        services.AddSingleton<IMessageProducer, RabbitMessageProducer>();

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

        var registration = builder.Build();

        services.AddSingleton(registration);

        foreach (var queue in registration.Queues)
        {
            foreach (var handler in queue.Handlers)
            {
                var serviceType = handler.ResponseType is null ? typeof(ICommandHandler<>).MakeGenericType(handler.MessageType) : typeof(IRequestHandler<, >).MakeGenericType(handler.MessageType, handler.ResponseType);

                services.AddScoped(serviceType, handler.HandlerType);
            }
        }

        services.AddSingleton<IMessageDispatcher, MessageDispatcher>();

        services.AddSingleton<RabbitQueueConsumer>();

        services.AddSingleton<RabbitRpcResponder>();

        services.AddHostedService<MessageListenerService>();

        return services;
    }
}
