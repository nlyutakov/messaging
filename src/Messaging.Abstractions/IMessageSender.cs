namespace Messaging.Abstractions;

public interface IMessageSender
{
    Task SendAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
        where TCommand : class;
}