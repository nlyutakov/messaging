using System.Text.Json;
using Messaging.Abstractions;
using Messaging.Core.Naming;
using Messaging.RabbitMQ.Connection;
using Messaging.RabbitMQ.Topology;
using RabbitMQ.Client;

namespace Messaging.RabbitMQ;

internal sealed class RabbitMqMessageSender : IMessageSender
{
    private readonly IRabbitMqConnection connection;
    private readonly IMessageNameFormatter nameFormatter;

    public RabbitMqMessageSender(
        IRabbitMqConnection connection,
        IMessageNameFormatter nameFormatter)
    {
        this.connection = connection;
        this.nameFormatter = nameFormatter;
    }

    public async Task SendAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
        where TCommand : class
    {
        ArgumentNullException.ThrowIfNull(command);

        var connection = await this.connection.GetConnectionAsync(cancellationToken);

        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        await channel.ExchangeDeclareAsync(exchange: RabbitMqTopology.CommandsExchange, type: ExchangeType.Direct, durable: true, autoDelete: false, cancellationToken: cancellationToken);

        var routingKey = this.nameFormatter.Format<TCommand>();

        var body = JsonSerializer.SerializeToUtf8Bytes(command);

        var properties = new BasicProperties
        {
            ContentType = "application/json",
            MessageId = Guid.NewGuid().ToString()
        };

        await channel.BasicPublishAsync(exchange: RabbitMqTopology.CommandsExchange, routingKey: routingKey, mandatory: true, basicProperties: properties, body: body, cancellationToken: cancellationToken);
    }
}