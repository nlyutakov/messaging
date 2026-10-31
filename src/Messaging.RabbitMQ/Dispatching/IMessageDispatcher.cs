namespace Messaging.RabbitMQ.Dispatching;
public interface IMessageDispatcher {Task<object?> DispatchAsync(string queueName,string messageTypeId,ReadOnlyMemory<byte> payload,CancellationToken cancellationToken=default);}
