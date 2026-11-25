using System.Security.Cryptography;
using System.Text;
using Messaging.Abstractions.Handlers;

namespace Messaging.RabbitMQ.Configuration;

public enum EventDeliveryMode
{
    Broadcast,
    CompetingConsumers
}

public class RabbitEventListenerBuilder
{
    private readonly List<EventSubscriptionBuilder> subscriptions = [];

    public EventSubscriptionBuilder Subscribe(string exchange, string subscriber)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(exchange);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscriber);

        if (this.subscriptions.Any(item => item.Exchange == exchange && item.Subscriber == subscriber))
        {
            throw new InvalidOperationException($"Duplicate subscription '{subscriber}' for '{exchange}'.");
        }

        var subscription = new EventSubscriptionBuilder(exchange, subscriber);
        this.subscriptions.Add(subscription);
        return subscription;
    }

    internal ListenerRegistration Build()
    {
        var registration = new ListenerRegistration();
        foreach (var subscription in this.subscriptions)
        {
            registration.Queues.Add(subscription.Build());
        }

        return registration;
    }
}

public class EventSubscriptionBuilder
{
    private readonly List<HandlerRegistration> handlers = [];
    private EventDeliveryMode mode;

    internal EventSubscriptionBuilder(string exchange, string subscriber)
    {
        this.Exchange = exchange;
        this.Subscriber = subscriber;
    }

    internal string Exchange { get; }

    internal string Subscriber { get; }

    public EventSubscriptionBuilder WithDeliveryMode(EventDeliveryMode mode)
    {
        if (!Enum.IsDefined(mode))
        {
            throw new ArgumentOutOfRangeException(nameof(mode));
        }

        this.mode = mode;
        return this;
    }

    public EventSubscriptionBuilder Handle<TEvent, THandler>(string messageTypeId) where THandler : class, IEventHandler<TEvent>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageTypeId);
        if (messageTypeId.IndexOfAny(['*', '#']) >= 0)
        {
            throw new ArgumentException("Event type IDs cannot contain topic wildcard characters.", nameof(messageTypeId));
        }

        if (this.handlers.Any(item => item.MessageTypeId == messageTypeId || item.MessageType == typeof(TEvent)))
        {
            throw new InvalidOperationException($"Duplicate event handler for subscription '{this.Subscriber}'.");
        }

        this.handlers.Add(new HandlerRegistration(messageTypeId, typeof(TEvent), typeof(THandler), null));
        return this;
    }

    internal QueueRegistration Build()
    {
        if (this.handlers.Count == 0)
        {
            throw new InvalidOperationException($"Subscription '{this.Subscriber}' has no handlers.");
        }

        var identity = Encoding.UTF8.GetBytes($"{this.Exchange.Length}:{this.Exchange}{this.Subscriber}");
        var stableName = $"messaging.events.{Convert.ToHexStringLower(SHA256.HashData(identity))}";
        var broadcast = this.mode == EventDeliveryMode.Broadcast;
        var queue = new QueueRegistration(broadcast ? $"{stableName}.{Guid.NewGuid():N}" : stableName)
        {
            EventBinding = new EventQueueBinding(this.Exchange, stableName, broadcast)
        };
        queue.Handlers.AddRange(this.handlers);
        return queue;
    }
}

internal record EventQueueBinding(string Exchange, string StableName, bool Broadcast);
