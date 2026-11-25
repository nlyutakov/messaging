using CourierLane.Abstractions.Idempotency;
using CourierLane.Idempotency.Redis;
using CourierLane.RabbitMQ.DependencyInjection;
using CourierLane.RabbitMQ.Tests.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace CourierLane.RabbitMQ.Tests;

[Collection(IntegrationTestCollection.Name)]
[Trait("Category", "Integration")]
public class RedisIdempotencyTests
{
    private readonly IntegrationTestFixture fixture;

    public RedisIdempotencyTests(IntegrationTestFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task ConcurrentOwnersAndRestartShareCompletedState()
    {
        var prefix = NewPrefix();
        const string key = "orders:test";

        using (var first = this.CreateProvider(prefix))
        using (var second = this.CreateProvider(prefix))
        {
            var left = first.GetRequiredService<IMessageDeduplicationStore>();
            var right = second.GetRequiredService<IMessageDeduplicationStore>();

            var results = await Task.WhenAll(left.TryAcquireAsync(key), right.TryAcquireAsync(key));

            var acquired = Assert.Single(results, result => result.Result == MessageAcquireResult.Acquired);

            Assert.Single(results, result => result.Result == MessageAcquireResult.InProgress);

            var lease = Assert.IsType<MessageLease>(acquired.Lease);

            Assert.True(await left.RenewAsync(lease));
            Assert.True(await right.MarkCompletedAsync(lease));

            await left.ReleaseAsync(lease);

            Assert.False(await right.RenewAsync(lease));
        }

        using var restarted = this.CreateProvider(prefix);

        var store = restarted.GetRequiredService<IMessageDeduplicationStore>();

        Assert.Equal(MessageAcquireResult.AlreadyCompleted, (await store.TryAcquireAsync(key)).Result);

        using var isolated = this.CreateProvider(prefix, virtualHost: "/other");

        var isolatedStore = isolated.GetRequiredService<IMessageDeduplicationStore>();

        Assert.Equal(MessageAcquireResult.Acquired, (await isolatedStore.TryAcquireAsync(key)).Result);
    }

    [Fact]
    public async Task ExpiredOwnerCannotMutateReplacementAndCompletedRecordExpires()
    {
        using var provider = this.CreateProvider(NewPrefix(), processingTimeout: "00:00:01", retention: "00:00:00.250");
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var store = provider.GetRequiredService<IMessageDeduplicationStore>();

        const string key = "orders:expiry";

        var original = Assert.IsType<MessageLease>((await store.TryAcquireAsync(key, deadline.Token)).Lease);
        var replacement = await AcquireWhenAvailableAsync(store, key, deadline.Token);

        Assert.NotEqual(original.Token, replacement.Token);
        Assert.False(await store.RenewAsync(original, deadline.Token));
        Assert.False(await store.MarkCompletedAsync(original, deadline.Token));

        await store.ReleaseAsync(original, deadline.Token);

        Assert.True(await store.MarkCompletedAsync(replacement, deadline.Token));

        var afterRetention = await AcquireWhenAvailableAsync(store, key, deadline.Token);

        Assert.NotEqual(replacement.Token, afterRetention.Token);

        await store.ReleaseAsync(afterRetention, deadline.Token);
    }

    private static async Task<MessageLease> AcquireWhenAvailableAsync(IMessageDeduplicationStore store, string key, CancellationToken cancellationToken)
    {
        while (true)
        {
            var result = await store.TryAcquireAsync(key, cancellationToken);

            if (result.Lease is { } lease)
            {
                return lease;
            }

            await Task.Delay(25, cancellationToken);
        }
    }

    private ServiceProvider CreateProvider(string prefix, string virtualHost = "/", string processingTimeout = "00:00:30", string retention = "00:00:30")
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["VirtualHost"] = virtualHost,
            ["Idempotency:Provider"] = "Redis",
            ["Idempotency:ProcessingTimeout"] = processingTimeout,
            ["Idempotency:RetentionPeriod"] = retention,
            ["Idempotency:Redis:ConnectionString"] = this.fixture.RedisConnectionString,
            ["Idempotency:Redis:KeyPrefix"] = prefix
        }).Build();

        return new ServiceCollection()
            .AddCourierLane(config)
            .AddRedisIdempotency(config.GetSection("Idempotency:Redis"))
            .BuildServiceProvider();
    }

    private static string NewPrefix() => $"courierlane:tests:{Guid.NewGuid():N}:";
}