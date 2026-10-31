using Messaging.Abstractions.Handlers;
using Messaging.RabbitMQ.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddRabbitMessaging(builder.Configuration.GetSection("RabbitMessaging"));

builder.Services.AddRabbitListener(listener =>
{
    listener.Listen("orders.commands.v2")
        .Handle<CreateOrderCommand, CreateOrderHandler>("orders.create.v1");

    listener.Listen("orders.queries")
        .Handle<GetOrderRequest, GetOrderResponse, GetOrderHandler>("orders.get.v1");
});

using var host = builder.Build();

Console.WriteLine("Worker: commands + RPC. Press Ctrl+C to stop.");

await host.RunAsync();

public record CreateOrderCommand(Guid OrderId);

public record GetOrderRequest(Guid OrderId);

public record GetOrderResponse(Guid OrderId, string Status);

public class CreateOrderHandler : ICommandHandler<CreateOrderCommand>
{
    public Task HandleAsync(CreateOrderCommand command, CancellationToken ct)
    {
        Console.WriteLine($"Processing order: {command.OrderId}; simulated failure for retry/DLQ");

        throw new InvalidOperationException("Simulated database failure.");
    }
}

public class GetOrderHandler : IRequestHandler<GetOrderRequest, GetOrderResponse>
{
    public Task<GetOrderResponse> HandleAsync(GetOrderRequest request, CancellationToken ct)
    {
        Console.WriteLine($"RPC GetOrder: {request.OrderId}");

        return Task.FromResult(new GetOrderResponse(request.OrderId, "Completed"));
    }
}
