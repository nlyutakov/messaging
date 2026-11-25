# Events

`IEventPublisher` publishes to a topic exchange. The registered event type ID is the routing key.

```csharp
builder.Services.AddRabbitEventListener(events =>
    events.Subscribe("business.events", "orders-projection")
        .Handle<OrderCreatedEvent, OrderCreatedHandler>("orders.created.v1"));
await publisher.PublishAsync("business.events", new OrderCreatedEvent(orderId), cancellationToken);
```

Broadcast creates a separate queue for each active instance. A subscriber can miss events during a disconnect. `WithDeliveryMode(EventDeliveryMode.CompetingConsumers)` uses one durable queue for the same exchange and subscriber. Declare the exchange before the first publish. [Integration example](../tests/Messaging.RabbitMQ.Tests/EventDeliveryTests.cs) · [Diagrams](diagrams/README.md)
