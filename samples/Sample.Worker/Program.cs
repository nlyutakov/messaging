using Messaging.RabbitMQ.DependencyInjection;
using Sample.Contracts.Commands;
using Sample.Worker.Consumers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddRabbitMqMessaging(options =>
{
    options.HostName = "localhost";
    options.Port = 5672;
    options.UserName = "messaging";
    options.Password = "messaging";
    options.VirtualHost = "/";
});

builder.Services.AddRabbitMqConsumer<
    CreateOrderCommand,
    CreateOrderConsumer>("sample.orders");

var host = builder.Build();

host.Run();