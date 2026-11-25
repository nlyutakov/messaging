namespace Messaging.Abstractions.Producers;

public interface IEventPublisher
{
    Task PublishAsync<TEvent>(string exchange, TEvent message, CancellationToken cancellationToken = default);
    Task PublishAsync<TEvent>(string exchange, TEvent message, SendOptions options, CancellationToken cancellationToken) =>
        throw new NotSupportedException("This publisher does not support custom message metadata.");
}
