namespace Messaging.Abstractions;

public interface IConsumer<in TMessage>
    where TMessage : class
{
    Task ConsumeAsync(
        TMessage message,
        CancellationToken cancellationToken = default);
}