using System.Collections.Concurrent;
using System.Text.Json;
using CourierLane.Abstractions.Producers;
using CourierLane.Outbox.PostgreSql;
using CourierLane.RabbitMQ.Connections;
using CourierLane.RabbitMQ.DependencyInjection;
using CourierLane.RabbitMQ.Producers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using RabbitMQ.AMQP.Client;
using Xunit;

namespace CourierLane.RabbitMQ.Tests;

public class OutboxTests
{
    [OutboxFact]
    public async Task BusinessTransactionAndRestartPreserveStableMessageId()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(40));
        var ct = deadline.Token;
        var scope = $"test-{Guid.NewGuid():N}";
        var queueName = $"courierlane.outbox.{Guid.NewGuid():N}";
        var builder = Host.CreateApplicationBuilder();
        var postgres = Environment.GetEnvironmentVariable("COURIERLANE_TEST_POSTGRESQL")!;
        builder.Configuration.AddRabbitSettings("Rabbit");
        builder.Configuration["Outbox:ConnectionString"] = postgres;
        builder.Configuration["Outbox:Scope"] = scope;
        builder.Services.AddCourierLane(builder.Configuration.GetSection("Rabbit"));
        builder.Services.AddCourierLaneContracts(contracts => contracts.Register<OrderCreated>("orders.created.v1"));
        builder.Services.AddPostgreSqlOutbox(builder.Configuration.GetSection("Outbox"));
        var host = builder.Build();
        await using var ownedHost = (IAsyncDisposable)host;
        var storage = host.Services.GetRequiredService<OutboxStorage>();
        var writer = host.Services.GetRequiredService<PostgreSqlOutbox>();
        await storage.InitializeAsync(ct);

        await using var dataSource = NpgsqlDataSource.Create(postgres);
        await using var business = await dataSource.OpenConnectionAsync(ct);
        await using (var create = new NpgsqlCommand("CREATE TEMP TABLE business_orders (id integer PRIMARY KEY);", business))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        await using (var transaction = await business.BeginTransactionAsync(ct))
        {
            await WriteBusinessOrderAsync(transaction, 1, ct);
            await writer.EnqueueCommandAsync(transaction, queueName, new OrderCreated(1), cancellationToken: ct);
            await transaction.RollbackAsync(ct);
        }

        Assert.Null(await storage.ClaimAsync(ct));
        await using (var transaction = await business.BeginTransactionAsync(ct))
        {
            await WriteBusinessOrderAsync(transaction, 2, ct);
            await writer.EnqueueCommandAsync(transaction, queueName, new OrderCreated(2), new SendOptions { MessageId = "outbox-stable-id" }, ct);
            await using var observer = dataSource.CreateCommand("SELECT COUNT(*) FROM courierlane_outbox_v1 WHERE scope = @scope;");
            observer.Parameters.AddWithValue("scope", scope);
            Assert.Equal(0L, await observer.ExecuteScalarAsync(ct));
            await transaction.CommitAsync(ct);
        }

        await using (var read = new NpgsqlCommand("SELECT id FROM business_orders;", business))
        {
            Assert.Equal(2, await read.ExecuteScalarAsync(ct));
        }

        var rabbit = await host.Services.GetRequiredService<RabbitConnectionManager>().GetConnectionAsync(ct);
        var queue = rabbit.Management().Queue(queueName).Type(QueueType.CLASSIC).AutoDelete(true).Exclusive(true);
        await queue.DeclareAsync();
        var seen = new ConcurrentQueue<string>();
        using var consumer = await rabbit.ConsumerBuilder().Queue(queueName).MessageHandler((context, message) =>
        {
            seen.Enqueue(message.MessageId()?.ToString() ?? "");
            context.Accept();
            return Task.CompletedTask;
        }).BuildAndStartAsync(ct);

        try
        {
            var claim = Assert.IsType<OutboxClaim>(await storage.ClaimAsync(ct));
            Assert.Null(await storage.ClaimAsync(ct));
            var publisher = host.Services.GetRequiredService<RabbitMessageProducer>();
            await publisher.PublishSerializedAsync(claim.Destination, claim.Type, claim.Payload, JsonSerializer.Deserialize<SendOptions>(claim.Metadata), claim.IsEvent, ct);

            // Force claim expiry after a confirm without recording completion in PostgreSQL.
            await using var expire = dataSource.CreateCommand("UPDATE courierlane_outbox_v1 SET claimed_until = clock_timestamp() WHERE id = @id;");
            expire.Parameters.AddWithValue("id", claim.Id);
            await expire.ExecuteNonQueryAsync(ct);
            await host.StartAsync(ct);
            while (seen.Count < 2 || !await IsPublishedAsync(dataSource, claim.Id, ct))
            {
                await Task.Delay(25, ct);
            }

            Assert.Equal(new[] { "outbox-stable-id", "outbox-stable-id" }, seen.ToArray());
            Assert.False(await storage.CompleteAsync(claim, ct));
        }
        finally
        {
            await host.StopAsync();
            await consumer.CloseAsync();
        }
    }

    private static async Task WriteBusinessOrderAsync(NpgsqlTransaction transaction, int id, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("INSERT INTO business_orders VALUES (@id);", transaction.Connection, transaction);
        command.Parameters.AddWithValue("id", id);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<bool> IsPublishedAsync(NpgsqlDataSource source, Guid id, CancellationToken cancellationToken)
    {
        await using var command = source.CreateCommand("SELECT published_at IS NOT NULL FROM courierlane_outbox_v1 WHERE id = @id;");
        command.Parameters.AddWithValue("id", id);
        return await command.ExecuteScalarAsync(cancellationToken) is true;
    }

    public record OrderCreated(int Id);

    private class OutboxFactAttribute : FactAttribute
    {
        public OutboxFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("COURIERLANE_TEST_POSTGRESQL")) ||
                string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("COURIERLANE_TEST_RABBITMQ")))
            {
                this.Skip = "Set COURIERLANE_TEST_POSTGRESQL and COURIERLANE_TEST_RABBITMQ to local test services.";
            }
        }
    }
}
