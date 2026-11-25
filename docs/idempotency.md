# Idempotency

Enable `Consumer.EnableIdempotency` and supply a stable MessageId. The deduplication scope includes the queue. `InProgress` waits for the current lease. `AlreadyCompleted` acknowledges the delivery without dispatching the handler. Renewal continues through retries and backoff.

If the lease is lost, the handler receives a cancellation signal. A handler that ignores cancellation can continue producing side effects. A missing MessageId bypasses deduplication. The in-memory store loses state on restart. For critical side effects, use business idempotency or a transactional inbox. [Redis](redis-provider.md) · [PostgreSQL](postgresql-provider.md) · [SQLite](sqlite-provider.md)
