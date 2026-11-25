# Transactional Outbox

`AddPostgreSqlOutbox` uses a connection string for the same PostgreSQL database as the business transaction. `Scope` identifies the logical publisher. `EnqueueCommandAsync` and `EnqueueEventAsync` write to the caller's active transaction.

```csharp
await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
await SaveBusinessStateAsync(transaction, cancellationToken);
await outbox.EnqueueEventAsync(transaction, "business.events", orderCreated, cancellationToken: cancellationToken);
await transaction.CommitAsync(cancellationToken);
```

The publisher retries with a stable MessageId and records success after broker confirmation. A crash between confirmation and that update can produce a duplicate. Published rows are cleaned up after the retention period. [Integration example](../tests/Messaging.RabbitMQ.Tests/OutboxTests.cs)
