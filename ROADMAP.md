# Roadmap to v1.0.0

See [AGENTS.md](AGENTS.md) for the full acceptance criteria.

| Step | Status |
| --- | --- |
| Through 9.4.2 | Commands, RPC, retry, DLQ topology, and an in-memory lease store. Manual scenarios are recorded in AGENTS.md. |
| 9.4.3 | Automatic lease renewal covered by nine tests. Local RabbitMQ handled a 5 s command with a 3 s lease and responded to RPC. |
| 9.4.4 | Provider selection, validation, and bounded cleanup. Ten tests cover DI and configuration. |
| 9.4.5 | Redis provider tested against local Redis for concurrent DI providers, client restart, TTL, namespace isolation, and stale tokens. Separate worker processes still need coverage. |
| 10 | Event broadcast and competing consumers verified across two independent AMQP connections. See step 17 for TCP recovery. |
| 11 | SQLite archive, opt-in ingestion, paging, replay, archive/discard, counts, authorization, and audit. Tests cover direct replay, the replay limit, a missing target, and REST permissions. |
| 12 | Standalone DLQ Web UI with status filtering, details, counts, and confirmations. Local HTTP and JavaScript checks passed. Browser testing with a real identity provider remains open. |
| 13–14 | PostgreSQL and SQLite providers share lease conformance tests with InMemory and Redis. |
| 15 | SendOptions for commands, events, and RPC request metadata. A real RabbitMQ test verifies metadata survives DLQ replay. |
| 16 | PostgreSQL outbox. An integration test covers atomic commit and rollback, conditional claiming, and simulated restart after broker confirmation without a database update. |
| 17 | Verified RPC timeout and cancellation, broadcast TCP reconnect, DLQ, outbox, and four lease providers. The Compose configuration still needs execution in an environment with Docker Compose. |
| 18 | Trace propagation, metrics, readiness, MaxConcurrency, and AMQPS configuration are implemented. The TLS handshake test and cache sizing review remain open. |
| 19 | English documentation, Mermaid diagrams, offline PDF, and Management Web UI. A standalone event sample remains open. |
| 20–21 | Seven CourierLane packages and their symbol packages were built locally as `1.0.0-preview.1`. A clean .NET 10 consumer restored the packages and resolved the default store through DI. CI setup and public publishing remain open. |

The baseline build used .NET SDK 10.0.115 and RabbitMQ AMQP Client 1.5.0. The solution initially had two libraries and two samples without automated tests. The temporary worker test code had already been removed. The samples use a 5 min processing timeout and 7-day retention, and the command producer generates a GUID MessageId.

After 9.4.4, the build reported zero warnings and errors. 19 of 19 tests passed.
After 9.4.5, the build reported zero warnings and errors. 22 of 22 tests passed with local Redis.
After step 10, the build reported zero warnings and errors. 24 of 24 tests passed with local Redis and RabbitMQ.
After adding DLQ metadata, the build reported zero warnings and errors. 26 of 26 tests passed with local Redis and RabbitMQ.
After adding DLQ management and the Web UI, the build reported zero warnings and errors. 29 of 29 tests passed with local Redis and RabbitMQ.
PostgreSQL was verified in an isolated PostgreSQL 15 container. After SQLite and metadata, the build reported zero warnings and errors and 39 of 39 tests passed with PostgreSQL, SQLite, Redis, and RabbitMQ.
After adding the outbox, the build reported zero warnings and errors. 40 of 40 tests passed with all local services.
After RPC, telemetry, and transport recovery, the build reported zero warnings and errors. 43 of 43 tests passed with all local services.

```bash
dotnet build Messaging.slnx
dotnet test Messaging.slnx --no-restore
MESSAGING_TEST_REDIS=127.0.0.1:6379 dotnet test Messaging.slnx --no-restore
MESSAGING_TEST_REDIS=127.0.0.1:6379 MESSAGING_TEST_RABBITMQ=localhost dotnet test Messaging.slnx --no-restore
```

For PostgreSQL conformance, set `MESSAGING_TEST_POSTGRESQL` to a connection string for a dedicated test database. Tests for external services are skipped when the relevant environment variable is absent.

For a local RabbitMQ smoke test, run `dotnet run --no-build -- --RabbitMessaging:Idempotency:ProcessingTimeout=00:00:03` from `samples/Messaging.ConnectionTest` and then `dotnet run --no-build` from `samples/Messaging.ProducerTest`.
