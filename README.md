# CourierLane for .NET 10

A messaging library for .NET applications using RabbitMQ 4.x and AMQP 1.0. It supports commands, RPC, events, retries, dead-letter queues, lease-based idempotency, and a transactional outbox.

## Quick start

Install the .NET 10 SDK and start a local RabbitMQ broker. The samples use localhost and the local guest account.

```bash
dotnet build Messaging.slnx
(cd samples/Messaging.ConnectionTest && dotnet run --no-build)
# In a separate terminal
(cd samples/Messaging.ProducerTest && dotnet run --no-build)
```

The worker handles `orders.create.v1` on `orders.commands.v2` and answers `orders.get.v1` on `orders.queries`. [Getting started](docs/getting-started.md)

## Packages

| Package ID | Purpose |
| --- | --- |
| `CourierLane.Abstractions` | Contracts for handlers, producers, RPC, and deduplication stores |
| `CourierLane.RabbitMQ` | AMQP 1.0 transport and in-memory idempotency |
| `CourierLane.Idempotency.Redis` | Shared Redis store |
| `CourierLane.Idempotency.PostgreSql` | Shared PostgreSQL store |
| `CourierLane.Idempotency.Sqlite` | Persistent local store |
| `CourierLane.Outbox.PostgreSql` | Outbound messages written within a business transaction |
| `CourierLane.Management` | DLQ REST API and SQLite archive |

`Messaging.Management.Web` is the ASP.NET Core host for the API and the operator interface. Package IDs use `CourierLane` and C# namespaces use `Messaging`. [Install from a local NuGet feed](docs/deployment.md)

## Guides

- [Architecture](docs/architecture.md) · [Configuration](docs/configuration.md) · [Commands](docs/commands.md) · [RPC](docs/rpc.md) · [Events](docs/events.md)
- [Retry](docs/retry.md) · [Idempotency](docs/idempotency.md) · [Redis](docs/redis-provider.md) · [PostgreSQL](docs/postgresql-provider.md) · [SQLite](docs/sqlite-provider.md)
- [DLQ](docs/dead-letter-queues.md) · [DLQ Management](docs/dlq-management.md) · [Transactional Outbox](docs/transactional-outbox.md)
- [Deployment](docs/deployment.md) · [Troubleshooting](docs/troubleshooting.md) · [Diagrams](docs/diagrams/README.md)
- [Offline PDF guide](docs/CourierLane-guide-en.pdf) · [Roadmap](ROADMAP.md) · [Changelog](CHANGELOG.md)
