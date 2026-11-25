# Commands

A command sends work to a queue. Consumer instances on that queue share the work. Each dispatch activates its handler in a DI scope.

```csharp
builder.Services.AddRabbitListener(listener => listener.Listen("orders.commands.v2")
    .Handle<CreateOrderCommand, CreateOrderHandler>("orders.create.v1"));
await producer.SendAsync("orders.commands.v2", new CreateOrderCommand(orderId), cancellationToken);
```

The publisher checks the AMQP outcome. Supply a stable MessageId through `SendOptions` for replay and outbox scenarios. Use a versioned type ID for the payload contract. [Worker sample](../samples/Messaging.ConnectionTest/Program.cs) · [Producer sample](../samples/Messaging.ProducerTest/Program.cs)
