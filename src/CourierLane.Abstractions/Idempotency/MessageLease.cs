namespace Messaging.Abstractions.Idempotency;

public record MessageLease(string Key, string Token);