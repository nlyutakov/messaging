using CourierLane.RabbitMQ.Configuration;
using CourierLane.RabbitMQ.Connections;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.AMQP.Client;
namespace CourierLane.RabbitMQ.Topology;

public class RabbitTopologyManager
{
    private readonly RabbitConnectionManager connections;

    private readonly ListenerRegistration registration;

    private readonly CourierLaneOptions options;

    private readonly ILogger<RabbitTopologyManager> logger;
    public RabbitTopologyManager(RabbitConnectionManager connections, ListenerRegistration registration, IOptions<CourierLaneOptions> options, ILogger<RabbitTopologyManager> logger)
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
            if (this.registration.Queues.Any(queue => queue.EventBinding?.Broadcast == true))
            {
                throw new InvalidOperationException("Broadcast subscriptions require automatic queue declaration.");
            }

            this.logger.LogInformation("Automatic queue declaration disabled");

            return;
        }

        var connection = await this.connections.GetConnectionAsync(cancellationToken);

        IManagement management = connection.Management();

        foreach (var queue in this.registration.Queues)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var eventBinding = queue.EventBinding;
            var broadcast = eventBinding?.Broadcast == true;
            var queueType = broadcast ? RabbitQueueType.Classic : queue.Options.QueueType ?? this.options.Consumer.QueueType;

            var amqpQueueType = queueType switch
            {
                RabbitQueueType.Classic => QueueType.CLASSIC,
                RabbitQueueType.Quorum => QueueType.QUORUM,
                _ => throw new ArgumentOutOfRangeException(nameof(queueType))
            };

            var queueSpec = management.Queue(queue.Name)
                .Type(amqpQueueType)
                .Exclusive(broadcast)
                .AutoDelete(broadcast);

            var isCommandQueue = queue.Handlers.All(h => h.ResponseType is null);

            if (this.options.Consumer.DeadLetter.Enabled && isCommandQueue)
            {
                var deadLetterBase = eventBinding?.StableName ?? queue.Name;
                var dlqName = deadLetterBase + this.options.Consumer.DeadLetter.Suffix;

                var dlxName = deadLetterBase + ".dlx";

                await management.Exchange(dlxName)
                    .Type(ExchangeType.DIRECT)
                    .DeclareAsync();

                await management.Queue(dlqName)
                    .Type(eventBinding is null ? amqpQueueType : QueueType.QUORUM)
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

            if (eventBinding is not null)
            {
                await management.Exchange(eventBinding.Exchange).Type(ExchangeType.TOPIC).DeclareAsync();
                foreach (var handler in queue.Handlers)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await management.Binding()
                        .SourceExchange(eventBinding.Exchange)
                        .DestinationQueue(queue.Name)
                        .Key(handler.MessageTypeId)
                        .BindAsync();
                }
            }

            this.logger.LogInformation("Queue {QueueName} declared", queue.Name);
        }
    }
}
