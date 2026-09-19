using Messaging.Abstractions;
using Messaging.RabbitMQ.DependencyInjection;
using Sample.Contracts.Commands;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRabbitMqMessaging(options =>
{
    options.HostName = "localhost";
    options.Port = 5672;
    options.UserName = "messaging";
    options.Password = "messaging";
    options.VirtualHost = "/";
});

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapPost("/orders", async (
    CreateOrderRequest request,
    IMessageSender sender,
    CancellationToken cancellationToken) =>
{
    var command = new CreateOrderCommand(
        Guid.NewGuid(),
        request.CustomerName,
        request.Total);

    await sender.SendAsync(
        command,
        cancellationToken);

    return Results.Accepted(
        $"/orders/{command.OrderId}",
        new
        {
            command.OrderId
        });
});

app.Run();

public sealed record CreateOrderRequest(
    string CustomerName,
    decimal Total);
