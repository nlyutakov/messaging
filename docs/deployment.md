# Deployment

Use separate accounts for publishers, consumers, and DLQ management. Restrict RabbitMQ permissions to the exchanges and queues each service needs. `AutoDeclare` needs configure permissions. Supply credentials through a secret store, use AMQPS, and serve management over HTTPS. The DLQ archive and SQLite idempotency store require persistent local storage.

## Integration environment

```bash
export MESSAGING_TEST_RABBITMQ_PASSWORD="$(cat /proc/sys/kernel/random/uuid)"
export MESSAGING_TEST_POSTGRES_PASSWORD="$(cat /proc/sys/kernel/random/uuid)"
docker compose -p messaging-test -f compose.integration.yaml up -d --wait
export MESSAGING_TEST_RABBITMQ=127.0.0.1 MESSAGING_TEST_RABBITMQ_PORT=15673
export MESSAGING_TEST_RABBITMQ_USER=messaging_test MESSAGING_TEST_REDIS=127.0.0.1:16379
export MESSAGING_TEST_POSTGRESQL="Host=127.0.0.1;Port=15432;Database=messaging_test;Username=messaging_test;Password=$MESSAGING_TEST_POSTGRES_PASSWORD"
dotnet test Messaging.slnx
docker compose -p messaging-test -f compose.integration.yaml down
```

## Local NuGet feed

Create a local feed with `dotnet pack Messaging.slnx -c Release -o ./artifacts/nuget`. In the consumer project, add the feed with `dotnet nuget add source /absolute/path/to/artifacts/nuget --name courierlane-local`. Install `CourierLane.RabbitMQ` with an explicit local version such as `1.0.0-preview.1`. Keep NuGet.org or an approved internal mirror enabled for third-party dependencies.

## Observability

Register the OpenTelemetry activity source and meter named `Messaging.RabbitMQ`. The outbox backlog gauge is under `Messaging.Outbox.PostgreSql`. Readiness can use `RabbitConnectionManager.IsConnected` and `MessageListenerService.IsReady`. [Architecture and limitations](../ARCHITECTURE.md)
