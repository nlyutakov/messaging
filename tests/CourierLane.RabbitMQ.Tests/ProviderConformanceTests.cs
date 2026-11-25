using CourierLane.Abstractions.Idempotency;
using CourierLane.Idempotency.PostgreSql;
using CourierLane.Idempotency.Redis;
using CourierLane.Idempotency.Sqlite;
using CourierLane.RabbitMQ.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace CourierLane.RabbitMQ.Tests;

public class ProviderConformanceTests
{
    private const string PostgreSqlVariable = "COURIERLANE_TEST_POSTGRESQL";

    [Fact]
    public async Task InMemoryHonorsSharedLeaseContract()
    {
        var config = CreateConfiguration("InMemory");
        using var provider = new ServiceCollection().AddCourierLane(config).BuildServiceProvider();
        var store = provider.GetRequiredService<IMessageDeduplicationStore>();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await ExerciseAsync(store, store, deadline.Token);
    }

    [RedisFact]
    public async Task RedisHonorsSharedLeaseContract()
    {
        var config = CreateConfiguration("Redis", new Dictionary<string, string?>
        {
            ["Idempotency:Redis:ConnectionString"] = Environment.GetEnvironmentVariable("COURIERLANE_TEST_REDIS"),
            ["Idempotency:Redis:KeyPrefix"] = $"courierlane:tests:{Guid.NewGuid():N}:"
        });
        using var first = BuildRedisProvider(config);
        using var second = BuildRedisProvider(config);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await ExerciseAsync(first.GetRequiredService<IMessageDeduplicationStore>(), second.GetRequiredService<IMessageDeduplicationStore>(), deadline.Token);
    }

    [PostgreSqlFact]
    public async Task PostgreSqlHonorsSharedLeaseContract()
    {
        var configuration = CreateConfiguration("PostgreSql", new Dictionary<string, string?>
        {
            ["Idempotency:PostgreSql:ConnectionString"] = Environment.GetEnvironmentVariable(PostgreSqlVariable),
            ["Idempotency:PostgreSql:KeyPrefix"] = $"courierlane:tests:{Guid.NewGuid():N}:"
        });
        var first = BuildPostgreSqlHost(configuration);
        var second = BuildPostgreSqlHost(configuration);
        await using var left = (IAsyncDisposable)first;
        await using var right = (IAsyncDisposable)second;
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await Task.WhenAll(first.StartAsync(deadline.Token), second.StartAsync(deadline.Token));

        await ExerciseAsync(first.Services.GetRequiredService<IMessageDeduplicationStore>(), second.Services.GetRequiredService<IMessageDeduplicationStore>(), deadline.Token);
        await Task.WhenAll(first.StopAsync(deadline.Token), second.StopAsync(deadline.Token));
    }

    [Fact]
    public async Task SqliteHonorsSharedLeaseContractOnPersistentFile()
    {
        var folder = Directory.CreateTempSubdirectory("courierlane-sqlite-");
        try
        {
            var config = CreateConfiguration("Sqlite", new Dictionary<string, string?>
            {
                ["Idempotency:Sqlite:DatabasePath"] = Path.Combine(folder.FullName, "store.db")
            });
            var first = BuildSqliteHost(config);
            var second = BuildSqliteHost(config);
            await using var left = (IAsyncDisposable)first;
            await using var right = (IAsyncDisposable)second;
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await Task.WhenAll(first.StartAsync(deadline.Token), second.StartAsync(deadline.Token));
            await ExerciseAsync(first.Services.GetRequiredService<IMessageDeduplicationStore>(), second.Services.GetRequiredService<IMessageDeduplicationStore>(), deadline.Token);
            await Task.WhenAll(first.StopAsync(deadline.Token), second.StopAsync(deadline.Token));
        }
        finally
        {
            folder.Delete(recursive: true);
        }
    }

    private static IConfiguration CreateConfiguration(string provider, Dictionary<string, string?>? values = null)
    {
        values ??= new();
        values.Add("Idempotency:Provider", provider);
        values.Add("Idempotency:ProcessingTimeout", "00:00:02");
        values.Add("Idempotency:RetentionPeriod", "00:00:00.500");
        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    private static ServiceProvider BuildRedisProvider(IConfiguration config) => new ServiceCollection()
        .AddCourierLane(config)
        .AddRedisIdempotency(config.GetSection("Idempotency:Redis"))
        .BuildServiceProvider();

    private static IHost BuildSqliteHost(IConfiguration config)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddCourierLane(config);
        builder.Services.AddSqliteIdempotency(config.GetSection("Idempotency:Sqlite"));
        return builder.Build();
    }

    private static IHost BuildPostgreSqlHost(IConfiguration config)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddCourierLane(config);
        builder.Services.AddPostgreSqlIdempotency(config.GetSection("Idempotency:PostgreSql"));
        return builder.Build();
    }

    private static async Task ExerciseAsync(IMessageDeduplicationStore first, IMessageDeduplicationStore second, CancellationToken cancellationToken)
    {
        const string key = "orders:conformance";
        var contenders = await Task.WhenAll(
            Task.Run(() => first.TryAcquireAsync(key, cancellationToken), cancellationToken),
            Task.Run(() => second.TryAcquireAsync(key, cancellationToken), cancellationToken));
        var original = Assert.IsType<MessageLease>(Assert.Single(contenders, result => result.Result == MessageAcquireResult.Acquired).Lease);
        Assert.Single(contenders, result => result.Result == MessageAcquireResult.InProgress);
        Assert.True(await first.RenewAsync(original, cancellationToken));
        var replacement = await WaitForLeaseAsync(second, key, cancellationToken);
        Assert.NotEqual(original.Token, replacement.Token);
        Assert.False(await first.RenewAsync(original, cancellationToken));
        Assert.False(await first.MarkCompletedAsync(original, cancellationToken));
        await first.ReleaseAsync(original, cancellationToken);
        Assert.True(await second.MarkCompletedAsync(replacement, cancellationToken));
        await first.ReleaseAsync(replacement, cancellationToken);
        Assert.Equal(MessageAcquireResult.AlreadyCompleted, (await first.TryAcquireAsync(key, cancellationToken)).Result);
        var afterRetention = await WaitForLeaseAsync(first, key, cancellationToken);
        await first.ReleaseAsync(afterRetention, cancellationToken);
    }

    private static async Task<MessageLease> WaitForLeaseAsync(IMessageDeduplicationStore store, string key, CancellationToken cancellationToken)
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

    private class PostgreSqlFactAttribute : FactAttribute
    {
        public PostgreSqlFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(PostgreSqlVariable)))
            {
                this.Skip = $"Set {PostgreSqlVariable} to a disposable PostgreSQL database connection string.";
            }
        }
    }

    private class RedisFactAttribute : FactAttribute
    {
        public RedisFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("COURIERLANE_TEST_REDIS")))
            {
                this.Skip = "Set COURIERLANE_TEST_REDIS to a local Redis connection string.";
            }
        }
    }
}
