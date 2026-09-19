using System.Text.Json;
using Messaging.Abstractions;
using Messaging.Core.Naming;
using Messaging.RabbitMQ.Connection;
using Messaging.RabbitMQ.Topology;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Messaging.RabbitMQ.Consumers;

internal sealed class RabbitMqConsumerHostedService : BackgroundService
{
    private readonly IRabbitMqConnection _connection;
    private readonly RabbitMqConsumerRegistry _registry;
    private readonly IMessageNameFormatter _nameFormatter;
    private readonly IServiceScopeFactory _scopeFactory;

    private readonly List<IChannel> _channels = [];

    public RabbitMqConsumerHostedService(
        IRabbitMqConnection connection,
        RabbitMqConsumerRegistry registry,
        IMessageNameFormatter nameFormatter,
        IServiceScopeFactory scopeFactory)
    {
        _connection = connection;
        _registry = registry;
        _nameFormatter = nameFormatter;
        _scopeFactory = scopeFactory;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var connection =
            await _connection.GetConnectionAsync(stoppingToken);

        foreach (var definition in _registry.Definitions)
        {
            var channel = await connection.CreateChannelAsync(
                cancellationToken: stoppingToken);

            _channels.Add(channel);

            await ConfigureConsumerAsync(
                channel,
                definition,
                stoppingToken);
        }

        await Task.Delay(
            Timeout.Infinite,
            stoppingToken);
    }

    private async Task ConfigureConsumerAsync(
        IChannel channel,
        RabbitMqConsumerDefinition definition,
        CancellationToken cancellationToken)
    {
        await channel.ExchangeDeclareAsync(
            exchange: RabbitMqTopology.CommandsExchange,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        await channel.QueueDeclareAsync(
            queue: definition.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken);

        var routingKey =
            _nameFormatter.Format(definition.MessageType);

        await channel.QueueBindAsync(
            queue: definition.QueueName,
            exchange: RabbitMqTopology.CommandsExchange,
            routingKey: routingKey,
            cancellationToken: cancellationToken);

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (_, eventArgs) =>
        {
            await HandleMessageAsync(
                channel,
                definition,
                eventArgs,
                cancellationToken);
        };

        await channel.BasicConsumeAsync(
            queue: definition.QueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: cancellationToken);
    }

    private async Task HandleMessageAsync(
        IChannel channel,
        RabbitMqConsumerDefinition definition,
        BasicDeliverEventArgs eventArgs,
        CancellationToken cancellationToken)
    {
        try
        {
            var message = JsonSerializer.Deserialize(
                eventArgs.Body.Span,
                definition.MessageType);

            if (message is null)
            {
                throw new JsonException(
                    $"Could not deserialize message as {definition.MessageType.Name}.");
            }

            await using var scope =
                _scopeFactory.CreateAsyncScope();

            var consumer = scope.ServiceProvider.GetRequiredService(
                definition.ConsumerType);

            var consumerInterface = typeof(IConsumer<>)
                .MakeGenericType(definition.MessageType);

            var consumeMethod = consumerInterface.GetMethod(
                nameof(IConsumer<object>.ConsumeAsync))
                ?? throw new InvalidOperationException(
                    "ConsumeAsync method was not found.");

            var task = (Task?)consumeMethod.Invoke(
                consumer,
                [message, cancellationToken]);

            if (task is null)
            {
                throw new InvalidOperationException(
                    "Consumer did not return a Task.");
            }

            await task;

            await channel.BasicAckAsync(
                deliveryTag: eventArgs.DeliveryTag,
                multiple: false,
                cancellationToken: cancellationToken);
        }
        catch
        {
            await channel.BasicNackAsync(
                deliveryTag: eventArgs.DeliveryTag,
                multiple: false,
                requeue: false,
                cancellationToken: cancellationToken);

            throw;
        }
    }

    public override async Task StopAsync(
        CancellationToken cancellationToken)
    {
        foreach (var channel in _channels)
        {
            await channel.DisposeAsync();
        }

        _channels.Clear();

        await base.StopAsync(cancellationToken);
    }
}