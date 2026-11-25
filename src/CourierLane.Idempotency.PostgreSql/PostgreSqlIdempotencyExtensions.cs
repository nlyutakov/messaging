using CourierLane.Abstractions.Idempotency;
using CourierLane.RabbitMQ.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CourierLane.Idempotency.PostgreSql;

public static class PostgreSqlIdempotencyExtensions
{
    internal const string ProviderName = "PostgreSql";

    public static IServiceCollection AddPostgreSqlIdempotency(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<PostgreSqlOptions>().Bind(configuration)
            .Validate(options => HasConnectionString(options.ConnectionString), "Idempotency.PostgreSql.ConnectionString must configure a PostgreSQL host.")
            .Validate(options => !string.IsNullOrWhiteSpace(options.KeyPrefix), "Idempotency.PostgreSql.KeyPrefix is required.")
            .ValidateOnStart();

        services.TryAddKeyedSingleton<NpgsqlDataSource>(ProviderName, (provider, _) =>
            NpgsqlDataSource.Create(provider.GetRequiredService<IOptions<PostgreSqlOptions>>().Value.ConnectionString));
        services.TryAddKeyedSingleton<IMessageDeduplicationStore>(ProviderName, (provider, _) => new PostgreSqlMessageStore(
            provider.GetRequiredKeyedService<NpgsqlDataSource>(ProviderName),
            provider.GetRequiredService<IOptions<CourierLaneOptions>>().Value,
            provider.GetRequiredService<IOptions<PostgreSqlOptions>>().Value.KeyPrefix));
        services.AddHostedService<PostgreSqlMigration>();
        return services;
    }

    private static bool HasConnectionString(string value)
    {
        try
        {
            return !string.IsNullOrWhiteSpace(new NpgsqlConnectionStringBuilder(value).Host);
        }
        catch (ArgumentException)
        {
            return false;
        }
    }
}

internal class PostgreSqlOptions
{
    public string ConnectionString { get; set; } = "";
    public string KeyPrefix { get; set; } = "courierlane:idempotency:";
}

internal class PostgreSqlMigration(IServiceProvider services, IOptions<CourierLaneOptions> options) : IHostedLifecycleService
{
    public async Task StartingAsync(CancellationToken cancellationToken)
    {
        if (options.Value.Idempotency.Provider != PostgreSqlIdempotencyExtensions.ProviderName)
        {
            return;
        }

        var source = services.GetRequiredKeyedService<NpgsqlDataSource>(PostgreSqlIdempotencyExtensions.ProviderName);
        await using var connection = await source.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var command = new NpgsqlCommand("""
            SELECT pg_advisory_xact_lock(811435362916360161);
            CREATE TABLE IF NOT EXISTS courierlane_idempotency_v1 (
                scope text NOT NULL,
                message_key text NOT NULL,
                token text NOT NULL,
                completed boolean NOT NULL,
                expires timestamptz NOT NULL,
                PRIMARY KEY (scope, message_key)
            );
            CREATE INDEX IF NOT EXISTS courierlane_idempotency_v1_expiry ON courierlane_idempotency_v1(scope, expires);
            """, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
