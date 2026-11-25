using System.Net;
using System.Net.Sockets;
using CourierLane.Abstractions.Handlers;
using CourierLane.Abstractions.Producers;
using CourierLane.RabbitMQ.Connections;
using CourierLane.RabbitMQ.Consumers;
using CourierLane.RabbitMQ.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;
using CourierLane.RabbitMQ.Tests.Infrastructure;

namespace CourierLane.RabbitMQ.Tests;

[Collection(IntegrationTestCollection.Name)]
[Trait("Category", "Integration")]
public class RabbitRecoveryTests
{
    private readonly IntegrationTestFixture fixture;

    public RabbitRecoveryTests(IntegrationTestFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task BroadcastSubscriptionRecoversAfterTransportDisconnect()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var ct = deadline.Token;
        var exchange = $"courierlane.recovery.{Guid.NewGuid():N}";
        var publisherBuilder = Host.CreateApplicationBuilder();
        publisherBuilder.Configuration.AddRabbitSettings("Rabbit", this.fixture);
        publisherBuilder.Services.AddCourierLane(publisherBuilder.Configuration.GetSection("Rabbit"));
        publisherBuilder.Services.AddCourierLaneContracts(contracts => contracts.Register<RecoveryEvent>("test.recovery.v1"));
        var publisherHost = publisherBuilder.Build();
        await using var ownedPublisher = (IAsyncDisposable)publisherHost;

        await using var proxy = new TcpProxy(this.fixture.RabbitMqHost, this.fixture.RabbitMqPort);
        var listenerBuilder = Host.CreateApplicationBuilder();
        listenerBuilder.Configuration.AddRabbitSettings("Rabbit", this.fixture);
        listenerBuilder.Configuration["Rabbit:Host"] = "127.0.0.1";
        listenerBuilder.Configuration["Rabbit:Port"] = proxy.Port.ToString();
        listenerBuilder.Configuration["Rabbit:Consumer:DeadLetter:Enabled"] = "false";
        listenerBuilder.Services.AddCourierLane(listenerBuilder.Configuration.GetSection("Rabbit"));
        listenerBuilder.Services.AddCourierLaneEventListener(events =>
            events.Subscribe(exchange, "recovery").Handle<RecoveryEvent, RecoveryHandler>("test.recovery.v1"));
        var deliveries = new DeliverySignals();
        listenerBuilder.Services.AddSingleton(deliveries);
        var listenerHost = listenerBuilder.Build();
        await using var ownedListener = (IAsyncDisposable)listenerHost;
        var listener = listenerHost.Services.GetRequiredService<MessageListenerService>();
        var connection = listenerHost.Services.GetRequiredService<RabbitConnectionManager>();
        var publisher = publisherHost.Services.GetRequiredService<IEventPublisher>();
        var management = (await publisherHost.Services.GetRequiredService<RabbitConnectionManager>().GetConnectionAsync(ct)).Management();

        try
        {
            await listenerHost.StartAsync(ct);
            await WaitUntilAsync(() => listener.IsReady && connection.IsConnected, ct);
            await publisher.PublishAsync(exchange, new RecoveryEvent(1), ct);
            await deliveries.First.Task.WaitAsync(ct);

            await proxy.InterruptAsync(ct);
            await WaitUntilAsync(() => !connection.IsConnected, ct);
            await proxy.AllowReconnectAsync(ct);
            await WaitUntilAsync(() => connection.IsConnected && listener.IsReady, ct);
            await publisher.PublishAsync(exchange, new RecoveryEvent(2), ct);
            await deliveries.Second.Task.WaitAsync(ct);

            await listenerHost.StopAsync(ct);
            Assert.False(listener.IsReady);
        }
        finally
        {
            await listenerHost.StopAsync();
            await management.Exchange(exchange).DeleteAsync();
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        while (!condition())
        {
            await Task.Delay(25, cancellationToken);
        }
    }

    public record RecoveryEvent(int Phase);

    public class DeliverySignals
    {
        public TaskCompletionSource First { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Second { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public class RecoveryHandler(DeliverySignals signals) : IEventHandler<RecoveryEvent>
    {
        public Task HandleAsync(RecoveryEvent message, CancellationToken cancellationToken = default)
        {
            (message.Phase == 1 ? signals.First : signals.Second).TrySetResult();
            return Task.CompletedTask;
        }
    }

    private class TcpProxy : IAsyncDisposable
    {
        private readonly TcpListener listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource stopping = new();
        private readonly TaskCompletionSource<TcpClient> first = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource reconnect = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Task traffic;

        public TcpProxy(string brokerHost, int brokerPort)
        {
            this.listener.Start();
            this.traffic = this.ForwardAsync(brokerHost, brokerPort);
        }

        public int Port => ((IPEndPoint)this.listener.LocalEndpoint).Port;

        public async Task InterruptAsync(CancellationToken cancellationToken)
        {
            var client = await this.first.Task.WaitAsync(cancellationToken);
            client.Dispose();
        }

        public Task AllowReconnectAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.reconnect.TrySetResult();
            return Task.CompletedTask;
        }

        private async Task ForwardAsync(string brokerHost, int brokerPort)
        {
            var ct = this.stopping.Token;
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    using var client = await this.listener.AcceptTcpClientAsync(ct);
                    using var broker = new TcpClient();
                    await broker.ConnectAsync(brokerHost, brokerPort, ct);
                    var upstream = client.GetStream().CopyToAsync(broker.GetStream(), ct);
                    var downstream = broker.GetStream().CopyToAsync(client.GetStream(), ct);
                    var firstConnection = this.first.TrySetResult(client);
                    await Task.WhenAny(upstream, downstream);
                    broker.Dispose();
                    client.Dispose();
                    try
                    {
                        await Task.WhenAll(upstream, downstream);
                    }
                    catch (IOException)
                    {
                    }
                    catch (ObjectDisposedException)
                    {
                    }

                    if (firstConnection)
                    {
                        await this.reconnect.Task.WaitAsync(ct);
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
            }
        }

        public async ValueTask DisposeAsync()
        {
            await this.stopping.CancelAsync();
            await this.traffic;
            this.listener.Stop();
            this.stopping.Dispose();
        }
    }
}
