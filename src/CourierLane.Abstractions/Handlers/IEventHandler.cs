namespace Messaging.Abstractions.Handlers;

public interface IEventHandler<TEvent> : ICommandHandler<TEvent>
{
}
