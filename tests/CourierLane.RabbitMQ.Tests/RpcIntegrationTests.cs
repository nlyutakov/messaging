using CourierLane.Abstractions.Exceptions;
using CourierLane.Abstractions.Handlers;
using CourierLane.Abstractions.Producers;
using CourierLane.RabbitMQ.Connections;
using CourierLane.RabbitMQ.DependencyInjection;
using CourierLane.RabbitMQ.Rpc;
using CourierLane.RabbitMQ.Topology;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using CourierLane.RabbitMQ.Tests.Infrastructure;

namespace CourierLane.RabbitMQ.Tests;

[Collection(IntegrationTestCollection.Name)]
[Trait("Category", "Integration")]
public class RpcIntegrationTests
{
    private readonly IntegrationTestFixture fixture;

    public RpcIntegrationTests(IntegrationTestFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task RecoversAfterTimeoutAndCancellationAndPropagatesRemoteFailure()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var ct = deadline.Token;
        var queue = $"courierlane.rpc.{Guid.NewGuid():N}";
        var configuration = new ConfigurationBuilder().AddInMemoryCollection().Build();
        configuration.AddRabbitSettings("Rabbit", this.fixture);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddCourierLane(configuration.GetSection("Rabbit"));
        services.AddCourierLaneContracts(contracts => contracts.Register<Query>("test.rpc.v1"));
        services.AddCourierLaneListener(listener => listener.Listen(queue).Handle<Query, Result, Handler>("test.rpc.v1"));
        var gate = new TestGate();
        services.AddSingleton(gate);
        await using var provider = services.BuildServiceProvider();
        await provider.GetRequiredService<RabbitTopologyManager>().DeclareQueuesAsync(ct);
        var connection = await provider.GetRequiredService<RabbitConnectionManager>().GetConnectionAsync(ct);
        using var responder = await provider.GetRequiredService<RabbitRpcResponder>().StartAsync(queue, ct);
        var producer = provider.GetRequiredService<IMessageProducer>();
        try
        {
            var timeout = new RequestOptions { Timeout = TimeSpan.FromMilliseconds(100) };
            await Assert.ThrowsAsync<RpcTimeoutException>(() => producer.RequestAsync<Query, Result>(queue, new Query("slow"), timeout, ct));

            using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var pending = producer.RequestAsync<Query, Result>(queue, new Query("cancel"), cancellationToken: cancelled.Token);
            await gate.CancellationRequestReceived.Task.WaitAsync(ct);
            await cancelled.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
            gate.CancellationRequestRelease.TrySetResult();

            await Assert.ThrowsAsync<RpcRemoteException>(() => producer.RequestAsync<Query, Result>(queue, new Query("fail"), cancellationToken: ct));
            var metadata = new RequestOptions { Metadata = new SendOptions { MessageId = "stable-request", CorrelationId = "shared-logical-trace" } };
            var replies = await Task.WhenAll(
                producer.RequestAsync<Query, Result>(queue, new Query("left"), metadata, ct),
                producer.RequestAsync<Query, Result>(queue, new Query("right"), metadata, ct));
            Assert.Equal(new Result("left"), replies[0]);
            Assert.Equal(new Result("right"), replies[1]);
        }
        finally
        {
            gate.CancellationRequestRelease.TrySetResult();
            await responder.CloseAsync();
            await connection.Management().Queue(queue).DeleteAsync();
        }
    }

    public record Query(string Value);
    public record Result(string Value);

    public class TestGate
    {
        public TaskCompletionSource CancellationRequestReceived { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource CancellationRequestRelease { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public class Handler(TestGate gate) : IRequestHandler<Query, Result>
    {
        public async Task<Result> HandleAsync(Query request, CancellationToken cancellationToken = default)
        {
            if (request.Value == "slow")
            {
                await Task.Delay(500, cancellationToken);
            }
            else if (request.Value == "cancel")
            {
                gate.CancellationRequestReceived.TrySetResult();
                await gate.CancellationRequestRelease.Task.WaitAsync(cancellationToken);
            }
            else if (request.Value == "fail")
            {
                throw new InvalidOperationException("Test handler failure.");
            }

            return new Result(request.Value);
        }
    }
}
