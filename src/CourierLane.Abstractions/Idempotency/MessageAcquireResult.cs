namespace CourierLane.Abstractions.Idempotency;

public enum MessageAcquireResult
{
    Acquired,
    AlreadyCompleted,
    InProgress
}