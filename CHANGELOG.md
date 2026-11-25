# Changelog

## Unreleased

- Use `CourierLane.*` for NuGet package IDs.

### 17–19 — Integration, lifecycle, and documentation

- Added RPC coverage for timeouts, cancellation, remote errors, and concurrent replies.
- Enabled topology recovery explicitly with automatic declaration. A TCP test verifies broadcast delivery after recovery.
- Added W3C propagation, latency and outcome metrics, outbox backlog, readiness, AMQPS settings, and a limit on active consumer callbacks.
- Added English guides, an offline PDF, Mermaid diagrams, an English Management Web UI, and an integration environment configuration.

### 16 — Transactional Outbox

- Added `CourierLane.Outbox.PostgreSql` to record business changes and outbound commands or events atomically.
- The background publisher uses claim tokens, publisher confirms, bounded backoff, and retention cleanup for published records.
- The integration test covers rollback and republishing with the same MessageId after simulated claim loss between broker confirmation and the database update.

### 15 — SendOptions & Message Metadata

- Commands, events, and RPC requests accept caller-supplied IDs, source, timestamps, and string headers. Routing and transport properties remain protected.
- The DLQ archive and replay preserve supported application headers and metadata.

### 13–14 — SQL Idempotency Providers

- Added PostgreSQL and SQLite packages with migrations, atomic lease operations, expiration, and bounded cleanup.
- InMemory, Redis, PostgreSQL, and SQLite run against the same conformance test.

### 12 — DLQ Web UI

- Added a standalone app with bearer authentication, status filtering, paging, details, counts, and action confirmations.

### 11 — DLQ Management

- Added a SQLite archive with opt-in ingestion and a REST API for inspection, replay, archiving, and discard.
- Replay preserves `MessageId`, sends directly to the allowed original queue, and records success after broker confirmation.
- The API grants separate permissions for metadata, payloads, and mutations. SQLite records status changes for audit.

### 11 — DLQ metadata

- Terminal failures on quorum queues attach an error code in `x-opt-messaging-failure`.
- Tests check the original queue, preserved MessageId, and available DLQ reasons in broadcast and competing modes.

### 10 — Events

- Added `IEventPublisher`, `IEventHandler<TEvent>`, and `AddRabbitEventListener`.
- Topic routing supports broadcast to active instances and shared-queue competing consumers. The event DLQ is stable for the logical subscriber.
- Integration tests cover both modes across independent AMQP connections.

### 9.4.5 — Redis Provider

- Added the separate `CourierLane.Idempotency.Redis` package with `AddRedisIdempotency`, Redis TTLs, and atomic lease operations.
- Opt-in tests against Redis cover concurrent clients, client restart, expiry, and namespace isolation.

### 9.4.4 — Provider Selection & Validation

- Configuration selects a deduplication store through keyed DI. `InMemory` is the built-in default. Missing registrations fail startup validation.
- Added validation for idempotency, retries, consumer settings, and queue overrides.
- The in-memory store cleans expired entries in small batches during acquire operations.

### 9.4.3 — Automatic Lease Renewal

- Command processing renews leases during handler execution, retries, and backoff.
- The consumer requeues on lease loss or a storage error. It waits for any in-flight renewal before completion and `Accept`.
- Added xUnit coverage for concurrent duplicates, retries, lease loss, shutdown, completion races, and stale tokens.
