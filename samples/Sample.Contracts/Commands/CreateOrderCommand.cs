namespace Sample.Contracts.Commands;

public record CreateOrderCommand(Guid OrderId, string CustomerName, decimal Total);