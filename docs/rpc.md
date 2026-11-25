# RPC

Register `IRequestHandler<TRequest,TResponse>` on a dedicated RPC queue. `IMessageProducer.RequestAsync` accepts an optional `RequestOptions.Timeout`.

```csharp
var result = await producer.RequestAsync<GetOrderRequest, GetOrderResponse>(
    "orders.queries", new GetOrderRequest(orderId), new RequestOptions { Timeout = TimeSpan.FromSeconds(5) }, cancellationToken);
```

The reply timeout starts after requester acquisition and raises `RpcTimeoutException`. Caller cancellation stops the local wait. The remote handler may continue running. A terminal remote failure raises `RpcRemoteException`. `RequestOptions.Metadata.CorrelationId` identifies the application operation. [Producer sample](../samples/Messaging.ProducerTest/Program.cs)
