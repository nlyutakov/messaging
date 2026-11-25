namespace CourierLane.RabbitMQ.Dispatching;

public class UnknownMessageTypeException : Exception
{
    public UnknownMessageTypeException(string queueName, string messageTypeId)
        : base($"Message type '{messageTypeId}' is not registered for queue '{queueName}'.")
    {
    }
}