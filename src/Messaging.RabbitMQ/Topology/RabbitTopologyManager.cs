using Messaging.RabbitMQ.Configuration;
using Messaging.RabbitMQ.Connections;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.AMQP.Client;
namespace Messaging.RabbitMQ.Topology;

public class RabbitTopologyManager
{
    private readonly RabbitConnectionManager connections;

    private readonly ListenerRegistration registration;

    private readonly RabbitMessagingOptions options;

    private readonly ILogger<RabbitTopologyManager> logger;
    public RabbitTopologyManager(RabbitConnectionManager connections, ListenerRegistration registration, IOptions<RabbitMessagingOptions> options, ILogger<RabbitTopologyManager> logger)
    {
        this.connections = connections;
        this.registration = registration;
        this.options = options.Value;
        this.logger = logger;
    }
    public async Task DeclareQueuesAsync(CancellationToken cancellationToken = default)
    {
        if (!this.options.Consumer.AutoDeclare)
        {
            this.logger.LogInformation("Automatic queue declaration disabled");

            return;
        }

        var connection = await this.connections.GetConnectionAsync(cancellationToken);

        IManagement management = connection.Management();

        foreach (var queue in this.registration.Queues)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var queueType = queue.Options.QueueType ?? this.options.Consumer.QueueType;

            var amqpQueueType = queueType switch
            {
                RabbitQueueType.Classic => QueueType.CLASSIC,
                RabbitQueueType.Quorum => QueueType.QUORUM,
                _ => throw new ArgumentOutOfRangeException(nameof(queueType))
            };

            var queueSpec = management.Queue(queue.Name)
                .Type(amqpQueueType)
                .Exclusive(false)
                .AutoDelete(false);

            var isCommandQueue = queue.Handlers.All(h => h.ResponseType is null);

            if (this.options.Consumer.DeadLetter.Enabled && isCommandQueue)
            {
                var dlqName = queue.Name + this.options.Consumer.DeadLetter.Suffix;

                var dlxName = queue.Name + ".dlx";

                await management.Exchange(dlxName)
                    .Type(ExchangeType.DIRECT)
                    .DeclareAsync();

                await management.Queue(dlqName)
                    .Type(amqpQueueType)
                    .Exclusive(false)
                    .AutoDelete(false)
                    .DeclareAsync();

                await management.Binding()
                    .SourceExchange(dlxName)
                    .DestinationQueue(dlqName)
                    .Key(dlqName)
                    .BindAsync();

                queueSpec.DeadLetterExchange(dlxName)
                    .DeadLetterRoutingKey(dlqName);
            }

            await queueSpec.DeclareAsync();

            this.logger.LogInformation("Queue {QueueName} declared", queue.Name);
        }
    }
}
