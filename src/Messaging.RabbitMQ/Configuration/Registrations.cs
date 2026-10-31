using Messaging.Abstractions.Handlers;
namespace Messaging.RabbitMQ.Configuration;

public record HandlerRegistration(string MessageTypeId, Type MessageType, Type HandlerType, Type? ResponseType);
public class QueueRegistration
{
    public string Name { get; }
    public QueueOptions Options { get; }
    public List<HandlerRegistration> Handlers { get; } = [];
    public QueueRegistration(string name, QueueOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        Name = name;
        Options = options ?? new();
    }
}

public class ListenerRegistration
{
    public List<QueueRegistration> Queues { get; } = [];
}

public class QueueListenerBuilder
{
    private readonly QueueRegistration queue;
    internal QueueListenerBuilder(QueueRegistration queue) => this.queue = queue;
    public QueueListenerBuilder Handle<TCommand, THandler>(string messageTypeId)
        where THandler : class, ICommandHandler<TCommand>
    {
        AddHandler(new(messageTypeId, typeof(TCommand), typeof(THandler), null));

        return this;
    }
    public QueueListenerBuilder Handle<TRequest, TResponse, THandler>(string messageTypeId)
        where THandler : class, IRequestHandler<TRequest, TResponse>
    {
        AddHandler(new(messageTypeId, typeof(TRequest), typeof(THandler), typeof(TResponse)));

        return this;
    }
    private void AddHandler(HandlerRegistration h)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(h.MessageTypeId);

        if (this.queue.Handlers.Any(x => x.MessageTypeId == h.MessageTypeId || x.MessageType == h.MessageType))
        {
            throw new InvalidOperationException($"Duplicate handler in queue '{this.queue.Name}'.");
        }

        this.queue.Handlers.Add(h);
    }
}

public class RabbitListenerBuilder
{
    private readonly ListenerRegistration registration = new();
    public QueueListenerBuilder Listen(string queueName, Action<QueueOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        if (this.registration.Queues.Any(q => q.Name == queueName))
        {
            throw new InvalidOperationException($"Duplicate queue '{queueName}'.");
        }

        var options = new QueueOptions();

        configure?.Invoke(options);

        var queue = new QueueRegistration(queueName, options);

        this.registration.Queues.Add(queue);

        return new(queue);
    }
    internal ListenerRegistration Build()
    {
        foreach (var q in this.registration.Queues)
        {
            if (q.Handlers.Count == 0)
            {
                throw new InvalidOperationException($"Queue '{q.Name}' has no handlers.");
            }

            if (q.Handlers.Any(h => h.ResponseType is null) && q.Handlers.Any(h => h.ResponseType is not null))
            {
                throw new InvalidOperationException($"Queue '{q.Name}' mixes command and RPC handlers.");
            }
        }

        return registration;
    }
}
