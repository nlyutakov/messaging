using System.Text.Json;
using Messaging.Abstractions.Exceptions;
using Messaging.RabbitMQ.Dispatching;

namespace Messaging.RabbitMQ.Retry;

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