namespace Messaging.Abstractions.Idempotency;

public enum MessageAcquireResult
{
    Acquired,
    AlreadyCompleted,
    InProgress
}