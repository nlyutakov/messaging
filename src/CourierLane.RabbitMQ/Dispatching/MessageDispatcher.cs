using System.Reflection;
using System.Text.Json;
using CourierLane.Abstractions.Handlers;
using CourierLane.RabbitMQ.Configuration;
using Microsoft.Extensions.DependencyInjection;
namespace CourierLane.RabbitMQ.Dispatching;

public class MessageDispatcher : IMessageDispatcher
{
    private delegate Task<object?> HandlerInvoker(IServiceProvider services, Type handlerType, object message, CancellationToken cancellationToken);

    private readonly IServiceScopeFactory scopeFactory;

    private readonly Dictionary<(string Queue, string MessageTypeId), (HandlerRegistration Registration, HandlerInvoker Invoke)> handlers = new();

    private readonly JsonSerializerOptions jsonOptions = new(JsonSerializerDefaults.Web);
    public MessageDispatcher(IServiceScopeFactory scopeFactory, ListenerRegistration registration)
    {
        this.scopeFactory = scopeFactory;

        foreach (var queue in registration.Queues)
        {
            foreach (var handler in queue.Handlers)
            {
                if (!this.handlers.TryAdd((queue.Name, handler.MessageTypeId), (handler, CreateInvoker(handler))))
                {
                    throw new InvalidOperationException($"Duplicate handler {queue.Name}/{handler.MessageTypeId}");
                }
            }
        }
    }
    public async Task<object?> DispatchAsync(string queueName, string messageTypeId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
    {
        if (!this.handlers.TryGetValue((queueName, messageTypeId), out var registration))
        {
            throw new UnknownMessageTypeException(queueName, messageTypeId);
        }

        var message = JsonSerializer.Deserialize(payload.Span, registration.Registration.MessageType, this.jsonOptions) ?? throw new JsonException($"Message '{messageTypeId}' deserialized to null.");

        await using var scope = this.scopeFactory.CreateAsyncScope();

        return await registration.Invoke(scope.ServiceProvider, registration.Registration.HandlerType, message, cancellationToken);
    }
    private static HandlerInvoker CreateInvoker(HandlerRegistration registration)
    {
        var isCommand = registration.ResponseType is null;

        var method = typeof(MessageDispatcher).GetMethod(isCommand ? nameof(InvokeCommandAsync) : nameof(InvokeRequestAsync), BindingFlags.Static | BindingFlags.NonPublic)!;

        var args = isCommand
            ? new[] { registration.MessageType }
            : new[] { registration.MessageType, registration.ResponseType! };

        return method.MakeGenericMethod(args)
            .CreateDelegate<HandlerInvoker>();
    }
    private static async Task<object?> InvokeCommandAsync<TCommand>(IServiceProvider services, Type handlerType, object message, CancellationToken ct)
    {
        var handler = (ICommandHandler<TCommand>?)services.GetService(handlerType) ?? services.GetRequiredService<ICommandHandler<TCommand>>();
        await handler.HandleAsync((TCommand)message, ct);

        return null;
    }
    private static async Task<object?> InvokeRequestAsync<TRequest, TResponse>(IServiceProvider services, Type handlerType, object message, CancellationToken ct)
    {
        var handler = (IRequestHandler<TRequest, TResponse>?)services.GetService(handlerType) ?? services.GetRequiredService<IRequestHandler<TRequest, TResponse>>();
        return await handler.HandleAsync((TRequest)message, ct);
    }
}
