namespace CourierLane.Abstractions.Idempotency;

public record MessageAcquireResponse(MessageAcquireResult Result, MessageLease? Lease = null);