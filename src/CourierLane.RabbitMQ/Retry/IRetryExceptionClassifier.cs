namespace CourierLane.RabbitMQ.Retry;

public interface IRetryExceptionClassifier
{
    bool ShouldRetry(Exception exception);
}