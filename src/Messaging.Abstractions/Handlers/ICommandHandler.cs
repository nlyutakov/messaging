namespace Messaging.Abstractions.Handlers;
public interface ICommandHandler<in TCommand> { Task HandleAsync(TCommand command, CancellationToken cancellationToken); }
