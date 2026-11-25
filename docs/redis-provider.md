# Redis provider

Set `RabbitMessaging:Idempotency:Provider=Redis`, enable idempotency, and register the provider.

```csharp
builder.Services.AddRedisIdempotency(rabbit.GetSection("Idempotency:Redis"));
```

Supply `ConnectionString` through your secret store. The default `KeyPrefix` is `messaging:idempotency:`. Use the same prefix across instances of one deployment. Lua scripts compare the lease token and update the TTL atomically. Eviction, failover, and loss of unpersisted state can cause duplicate processing. [Architecture](../ARCHITECTURE.md)
