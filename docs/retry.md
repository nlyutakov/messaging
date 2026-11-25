# Retry

The consumer retries dispatch in the same process and under the same lease. By default it attempts dispatch three times, with delays of 2 s and 4 s. `MaxDelay` caps exponential backoff. `NonRetryableMessageException`, unknown message types, and invalid JSON skip retries. Register a custom `IRetryExceptionClassifier` through DI to change classification.

Honor cancellation in handlers and make side effects idempotent. Shutdown and lease failures requeue the delivery. [Dead-letter queues](dead-letter-queues.md) · [Idempotency](idempotency.md)
