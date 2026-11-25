using Messaging.Abstractions.Exceptions;
using Messaging.Abstractions.Handlers;
using Messaging.Abstractions.Producers;
using Messaging.RabbitMQ.Connections;
using Messaging.RabbitMQ.DependencyInjection;
using Messaging.RabbitMQ.Rpc;
using Messaging.RabbitMQ.Topology;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Messaging.RabbitMQ.Tests;

public class RpcIntegrationTests
{
    [RabbitFact]
    public async Task RecoversAfterTimeoutAndCancellationAndPropagatesRemoteFailure()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(25));
        var ct = deadline.Token;
        var queue = $"messaging.rpc.{Guid.NewGuid():N}";
        var configuration = new ConfigurationBuilder().AddInMemoryCollection().Build();
        configuration.AddRabbitSettings("Rabbit");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMessaging(configuration.GetSection("Rabbit"));
        services.AddRabbitMessageContracts(contracts => contracts.Register<Query>("test.rpc.v1"));
        services.AddRabbitListener(listener => listener.Listen(queue).Handle<Query, Result, Handler>("test.rpc.v1"));
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

    private class RabbitFactAttribute : FactAttribute
    {
        public RabbitFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("MESSAGING_TEST_RABBITMQ")))
            {
                this.Skip = "Set MESSAGING_TEST_RABBITMQ to a local RabbitMQ host.";
            }
        }
    }
}
