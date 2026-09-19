using Messaging.Abstractions;
using Sample.Contracts.Commands;

namespace Sample.Worker.Consumers;

public sealed class CreateOrderConsumer
    : IConsumer<CreateOrderCommand>
{
    public Task ConsumeAsync(
        CreateOrderCommand message,
        CancellationToken cancellationToken = default)
    {
        Console.WriteLine(
            $"Received order {message.OrderId}");

        Console.WriteLine(
            $"Customer: {message.CustomerName}");

        Console.WriteLine(
            $"Total: {message.Total}");

        return Task.CompletedTask;
    }
}