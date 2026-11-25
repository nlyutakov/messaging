namespace CourierLane.Abstractions.Exceptions;

public class NonRetryableMessageException : Exception
{
    public NonRetryableMessageException(string message) : base(message)
    {
    }

    public NonRetryableMessageException(string message, Exception innerException) : base(message, innerException)
    {
    }
}