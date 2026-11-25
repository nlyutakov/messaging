using System.Text.Json;
using CourierLane.Abstractions.Exceptions;
using CourierLane.RabbitMQ.Dispatching;

namespace CourierLane.RabbitMQ.Retry;

public class DefaultRetryExceptionClassifier : IRetryExceptionClassifier
{
    public bool ShouldRetry(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        if (exception is NonRetryableMessageException)
        {
            return false;
        }

        if (exception is UnknownMessageTypeException)
        {
            return false;
        }

        if (exception is JsonException)
        {
            return false;
        }

        return true;
    }
}