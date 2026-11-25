using System.Collections.Concurrent;
using Messaging.Abstractions.Exceptions;
using Messaging.Abstractions.Handlers;
using Messaging.Abstractions.Producers;
using Messaging.Management;
using Messaging.RabbitMQ.Configuration;
using Messaging.RabbitMQ.Connections;
using Messaging.RabbitMQ.Consumers;
using Messaging.RabbitMQ.DependencyInjection;
using Messaging.RabbitMQ.Topology;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.AMQP.Client;
using Xunit;

namespace Messaging.RabbitMQ.Tests;

public class EventDeliveryTests
{
    private const string HostVariable = RabbitIntegrationSettings.HostVariable;
    private const string EventType = "orders.created.v1";

    [RabbitTheory]
    [InlineData(EventDeliveryMode.Broadcast)]
    [InlineData(EventDeliveryMode.CompetingConsumers)]
    public async Task RoutingAcrossIndependentConnections(EventDeliveryMode mode)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var exchange = $"messaging.tests.{Guid.NewGuid():N}";
        var deliveries = new ConcurrentQueue<(int Instance, int Value)>();
        await using var first = CreateServices(exchange, mode, 1, deliveries);
        await using var second = CreateServices(exchange, mode, 2, deliveries);
        var firstQueue = Assert.Single(first.GetRequiredService<ListenerRegistration>().Queues);
        var secondQueue = Assert.Single(second.GetRequiredService<ListenerRegistration>().Queues);
        var connection = await first.GetRequiredService<RabbitConnectionManager>().GetConnectionAsync(deadline.Token);
        var management = connection.Management();
        IConsumer? firstConsumer = null;
        IConsumer? secondConsumer = null;

        try
        {
            await first.GetRequiredService<RabbitTopologyManager>().DeclareQueuesAsync(deadline.Token);
            firstConsumer = await first.GetRequiredService<RabbitQueueConsumer>().StartAsync(firstQueue, deadline.Token);
            await second.GetRequiredService<RabbitTopologyManager>().DeclareQueuesAsync(deadline.Token);
            secondConsumer = await second.GetRequiredService<RabbitQueueConsumer>().StartAsync(secondQueue, deadline.Token);
            var publisher = first.GetRequiredService<IEventPublisher>();
            const int count = 6;
            for (var value = 0; value < count; value++)
            {
                await publisher.PublishAsync(exchange, new OrderCreated(value), deadline.Token);
            }

            var expected = mode == EventDeliveryMode.Broadcast ? count * 2 : count;
            while (deliveries.Count < expected)
            {
                await Task.Delay(25, deadline.Token);
            }

            await Task.Delay(250, deadline.Token);
            Assert.Equal(expected, deliveries.Count);
            for (var value = 0; value < count; value++)
            {
                var instances = deliveries.Where(item => item.Value == value).Select(item => item.Instance).Order().ToArray();
                if (mode == EventDeliveryMode.Broadcast)
                {
                    Assert.Equal(new[] { 1, 2 }, instances);
                }
                else
                {
                    Assert.Single(instances);
                }
            }
        }
        finally
        {
            await CloseAsync(secondConsumer);
            await CloseAsync(firstConsumer);
            if (mode == EventDeliveryMode.CompetingConsumers && firstConsumer is not null)
            {
                await management.Queue(firstQueue.Name).DeleteAsync();
            }

            await management.Exchange(exchange).DeleteAsync();
        }
    }

    [RabbitTheory]
    [InlineData(EventDeliveryMode.Broadcast)]
    [InlineData(EventDeliveryMode.CompetingConsumers)]
    public async Task FailingEventGoesToTheDurableSubscriberDeadLetterQueue(EventDeliveryMode mode)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var exchange = $"messaging.tests.{Guid.NewGuid():N}";
        await using var provider = CreateServices(exchange, mode, 1, new(), deadLetterEnabled: true);
        var queue = Assert.Single(provider.GetRequiredService<ListenerRegistration>().Queues);
        var binding = Assert.IsType<EventQueueBinding>(queue.EventBinding);
        var connection = await provider.GetRequiredService<RabbitConnectionManager>().GetConnectionAsync(deadline.Token);
        var management = connection.Management();
        var dlq = binding.StableName + ".dlq";
        var result = new TaskCompletionSource<(object? Queue, object? Failure, object? Id)>(TaskCreationOptions.RunContinuationsAsynchronously);
        IConsumer? source = null;
        IConsumer? deadLetter = null;

        try
        {
            await provider.GetRequiredService<RabbitTopologyManager>().DeclareQueuesAsync(deadline.Token);
            source = await provider.GetRequiredService<RabbitQueueConsumer>().StartAsync(queue, deadline.Token);
            deadLetter = await connection.ConsumerBuilder().Queue(dlq).MessageHandler((context, message) =>
            {
                var metadata = (message.Annotation("x-first-death-queue"), message.Annotation("x-opt-messaging-failure"), message.MessageId());
                context.Accept();
                result.TrySetResult(metadata);
                return Task.CompletedTask;
            }).BuildAndStartAsync(deadline.Token);

            await provider.GetRequiredService<IEventPublisher>().PublishAsync(exchange, new OrderCreated(-1), deadline.Token);
            var received = await result.Task.WaitAsync(deadline.Token);
            Assert.Equal(queue.Name, received.Queue?.ToString());
            Assert.NotNull(received.Id);
            Assert.Equal(mode == EventDeliveryMode.Broadcast ? null : "handler-failed", received.Failure?.ToString());
        }
        finally
        {
            await CloseAsync(deadLetter);
            await CloseAsync(source);
            if (mode == EventDeliveryMode.CompetingConsumers && source is not null)
            {
                await management.Queue(queue.Name).DeleteAsync();
            }

            await management.Queue(dlq).DeleteAsync();
            await management.Exchange(binding.StableName + ".dlx").DeleteAsync();
            await management.Exchange(exchange).DeleteAsync();
        }
    }

    [RabbitTheory]
    [InlineData(EventDeliveryMode.CompetingConsumers)]
    public async Task ArchiveReplayTargetsOriginalQueueAndCannotLoop(EventDeliveryMode mode)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var exchange = $"messaging.tests.{Guid.NewGuid():N}";
        var folder = Directory.CreateTempSubdirectory("messaging-replay-");
        var observerQueue = exchange + ".observer";
        await using var provider = CreateServices(exchange, mode, 1, new(), deadLetterEnabled: true);
        var queue = Assert.Single(provider.GetRequiredService<ListenerRegistration>().Queues);
        var binding = Assert.IsType<EventQueueBinding>(queue.EventBinding);
        var connection = await provider.GetRequiredService<RabbitConnectionManager>().GetConnectionAsync(deadline.Token);
        var management = connection.Management();
        var dlq = binding.StableName + ".dlq";
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Management:DatabasePath"] = Path.Combine(folder.FullName, "archive.db"),
            [$"Management:Sources:{dlq}:0"] = queue.Name
        }).Build();
        configuration.AddRabbitSettings("Rabbit");
        await using var archiveHost = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddRabbitMessaging(configuration.GetSection("Rabbit"));
                services.AddDeadLetterManagement(configuration.GetSection("Management"), "test");
            })
            .Build() as IAsyncDisposable ?? throw new InvalidOperationException("Host must support async disposal.");
        var host = (IHost)archiveHost;
        IConsumer? source = null;
        IConsumer? observer = null;
        var targetDeleted = false;
        var observed = 0;
        var firstObservation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            await provider.GetRequiredService<RabbitTopologyManager>().DeclareQueuesAsync(deadline.Token);
            source = await provider.GetRequiredService<RabbitQueueConsumer>().StartAsync(queue, deadline.Token);
            await management.Queue(observerQueue).Type(QueueType.CLASSIC).Exclusive(true).AutoDelete(true).DeclareAsync();
            await management.Binding().SourceExchange(exchange).DestinationQueue(observerQueue).Key(EventType).BindAsync();
            observer = await connection.ConsumerBuilder().Queue(observerQueue).MessageHandler((context, _) =>
            {
                Interlocked.Increment(ref observed);
                context.Accept();
                firstObservation.TrySetResult();
                return Task.CompletedTask;
            }).BuildAndStartAsync(deadline.Token);

            await host.StartAsync(deadline.Token);
            await provider.GetRequiredService<IEventPublisher>().PublishAsync(exchange, new OrderCreated(-1), new SendOptions
            {
                MessageId = "replay-original",
                CorrelationId = "trace-test",
                CausationId = "creation-test",
                Source = "orders-test",
                Headers = new Dictionary<string, string> { ["tenant"] = "test-tenant" }
            }, deadline.Token);
            await firstObservation.Task.WaitAsync(deadline.Token);
            var manager = host.Services.GetRequiredService<IDeadLetterManager>();
            var original = await WaitForDeadLetterAsync(manager, 0, deadline.Token);
            Assert.Equal(DeadLetterResult.Applied, await manager.ApplyAsync(original.Id, DeadLetterAction.Replay, "test", deadline.Token));
            var replayed = await WaitForDeadLetterAsync(manager, original.Id, deadline.Token);
            Assert.Equal(original.MessageId, replayed.MessageId);
            Assert.Equal("replay-original", replayed.MessageId);
            Assert.Equal(queue.Name, replayed.OriginalQueue);
            Assert.Equal(1, replayed.ReplayCount);
            var details = Assert.IsType<DeadLetterDetails>(await manager.InspectAsync(replayed.Id, deadline.Token));
            Assert.Equal("trace-test", details.CorrelationId);
            Assert.Equal("creation-test", details.Headers["messaging-causation-id"]);
            Assert.Equal("orders-test", details.Headers["messaging-source"]);
            Assert.Equal("test-tenant", details.Headers["tenant"]);
            Assert.Equal(DeadLetterResult.Rejected, await manager.ApplyAsync(replayed.Id, DeadLetterAction.Replay, "test", deadline.Token));
            Assert.Equal(1, Volatile.Read(ref observed));

            await provider.GetRequiredService<IEventPublisher>().PublishAsync(exchange, new OrderCreated(-2), deadline.Token);
            var orphaned = await WaitForDeadLetterAsync(manager, replayed.Id, deadline.Token);
            await CloseAsync(source);
            await management.Queue(queue.Name).DeleteAsync();
            targetDeleted = true;
            Assert.Equal(DeadLetterResult.Rejected, await manager.ApplyAsync(orphaned.Id, DeadLetterAction.Replay, "test", deadline.Token));
            Assert.Equal(DeadLetterStatus.Pending, (await manager.InspectAsync(orphaned.Id, deadline.Token))?.Summary.Status);
        }
        finally
        {
            await host.StopAsync();
            await CloseAsync(observer);
            if (source is not null)
            {
                if (!targetDeleted)
                {
                    await CloseAsync(source);
                    await management.Queue(queue.Name).DeleteAsync();
                }

                await management.Queue(dlq).DeleteAsync();
                await management.Exchange(binding.StableName + ".dlx").DeleteAsync();
                await management.Exchange(exchange).DeleteAsync();
            }

            folder.Delete(recursive: true);
        }
    }

    private static async Task<DeadLetterSummary> WaitForDeadLetterAsync(IDeadLetterManager manager, long after, CancellationToken cancellationToken)
    {
        while (true)
        {
            var rows = await manager.ListAsync(after, cancellationToken: cancellationToken);
            if (rows.Count > 0)
            {
                return Assert.Single(rows);
            }

            await Task.Delay(25, cancellationToken);
        }
    }

    private static async Task CloseAsync(IConsumer? consumer)
    {
        if (consumer is not null)
        {
            using (consumer)
            {
                await consumer.CloseAsync();
            }
        }
    }

    private static ServiceProvider CreateServices(string exchange, EventDeliveryMode mode, int instance, ConcurrentQueue<(int Instance, int Value)> deliveries, bool deadLetterEnabled = false)
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Rabbit:Consumer:EnableIdempotency"] = "true",
            ["Rabbit:Consumer:DeadLetter:Enabled"] = deadLetterEnabled.ToString()
        }).Build();
        config.AddRabbitSettings("Rabbit");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMessaging(config.GetSection("Rabbit"));
        services.AddRabbitMessageContracts(contracts => contracts.Register<OrderCreated>(EventType));
        services.AddRabbitEventListener(events => events.Subscribe(exchange, "orders-projection").WithDeliveryMode(mode).Handle<OrderCreated, OrderCreatedHandler>(EventType));
        services.AddSingleton(new TestDelivery(instance, deliveries));
        return services.BuildServiceProvider();
    }

    public record OrderCreated(int Value);

    public record TestDelivery(int Instance, ConcurrentQueue<(int Instance, int Value)> Deliveries);

    public class OrderCreatedHandler(TestDelivery delivery) : IEventHandler<OrderCreated>
    {
        public Task HandleAsync(OrderCreated command, CancellationToken cancellationToken = default)
        {
            if (command.Value < 0)
            {
                throw new NonRetryableMessageException("Test handler rejected the event.");
            }

            delivery.Deliveries.Enqueue((delivery.Instance, command.Value));
            return Task.CompletedTask;
        }
    }

    private class RabbitTheoryAttribute : TheoryAttribute
    {
        public RabbitTheoryAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(HostVariable)))
            {
                this.Skip = $"Set {HostVariable} to a local RabbitMQ host.";
            }
        }
    }
}
