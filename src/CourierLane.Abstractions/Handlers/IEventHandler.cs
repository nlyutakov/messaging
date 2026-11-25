namespace CourierLane.Abstractions.Handlers;

public interface IEventHandler<TEvent> : ICommandHandler<TEvent>
{
}
