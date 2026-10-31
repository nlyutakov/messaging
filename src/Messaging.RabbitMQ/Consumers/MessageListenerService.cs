using Messaging.RabbitMQ.Configuration;
using Messaging.RabbitMQ.Rpc;
using Messaging.RabbitMQ.Topology;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.AMQP.Client;
namespace Messaging.RabbitMQ.Consumers;

public class MessageListenerService : BackgroundService
{
    private readonly RabbitTopologyManager topology;

    private readonly RabbitQueueConsumer queueConsumer;

    private readonly RabbitRpcResponder rpcResponder;

    private readonly ListenerRegistration registration;

    private readonly ILogger<MessageListenerService> logger;

    private readonly List<IConsumer> consumers = [];

    private readonly List<IResponder> responders = [];
    public MessageListenerService(RabbitTopologyManager topology, RabbitQueueConsumer queueConsumer, RabbitRpcResponder rpcResponder, ListenerRegistration registration, ILogger<MessageListenerService> logger)
    {
        this.topology = topology;
        this.queueConsumer = queueConsumer;
        this.rpcResponder = rpcResponder;
        this.registration = registration;
        this.logger = logger;
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await this.topology.DeclareQueuesAsync(stoppingToken);

            foreach (var queue in this.registration.Queues)
            {
                stoppingToken.ThrowIfCancellationRequested();

                var hasCommands = queue.Handlers.Any(h => h.ResponseType is null);

                var hasRequests = queue.Handlers.Any(h => h.ResponseType is not null);

                if (hasCommands && hasRequests)
                {
                    throw new InvalidOperationException($"Queue '{queue.Name}' mixes commands and RPC.");
                }

                if (hasRequests)
                {
                    this.responders.Add(await this.rpcResponder.StartAsync(queue.Name, stoppingToken));
                }
                else
                {
                    this.consumers.Add(await this.queueConsumer.StartAsync(queue, stoppingToken));
                }
            }

            this.logger.LogInformation("Rabbit listener started: {Consumers} consumers, {Responders} responders", this.consumers.Count, this.responders.Count);

            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        }
        catch (OperationCanceledException)when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            foreach (var responder in this.responders)
            {
                try
                {
                    await responder.CloseAsync();

                    responder.Dispose();
                }
                catch (Exception ex)
                {
                    this.logger.LogWarning(ex, "Error closing RPC responder");
                }
            }

            this.responders.Clear();

            foreach (var consumer in this.consumers)
            {
                try
                {
                    consumer.Pause();

                    await consumer.CloseAsync();

                    consumer.Dispose();
                }
                catch (Exception ex)
                {
                    this.logger.LogWarning(ex, "Error closing consumer");
                }
            }

            this.consumers.Clear();
        }
    }
}
