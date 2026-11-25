using CourierLane.Abstractions.Idempotency;
using CourierLane.RabbitMQ.Configuration;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace CourierLane.Idempotency.Sqlite;

public static class SqliteIdempotencyExtensions
{
    internal const string ProviderName = "Sqlite";

    public static IServiceCollection AddSqliteIdempotency(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<SqliteOptions>().Bind(configuration)
            .Validate(options => !string.IsNullOrWhiteSpace(options.DatabasePath) && options.DatabasePath != ":memory:" &&
                !options.DatabasePath.StartsWith("file:", StringComparison.OrdinalIgnoreCase), "Idempotency.Sqlite.DatabasePath must be a persistent local database path.")
            .Validate(options => !string.IsNullOrWhiteSpace(options.KeyPrefix), "Idempotency.Sqlite.KeyPrefix is required.")
            .ValidateOnStart();

        services.TryAddKeyedSingleton<IMessageDeduplicationStore>(ProviderName, (provider, _) =>
        {
            var options = provider.GetRequiredService<IOptions<SqliteOptions>>().Value;
            var rabbit = provider.GetRequiredService<IOptions<CourierLaneOptions>>().Value;
            return new SqliteMessageStore(options.DatabasePath, options.KeyPrefix, rabbit);
        });
        services.AddHostedService<SqliteMigration>();
        return services;
    }
}

internal class SqliteOptions
{
    public string DatabasePath { get; set; } = "";
    public string KeyPrefix { get; set; } = "courierlane:idempotency:";
}

internal class SqliteMigration(IOptions<SqliteOptions> sqlite, IOptions<CourierLaneOptions> rabbit) : IHostedLifecycleService
{
    public async Task StartingAsync(CancellationToken cancellationToken)
    {
        if (rabbit.Value.Idempotency.Provider != SqliteIdempotencyExtensions.ProviderName)
        {
            return;
        }

        await using var connection = new SqliteConnection(SqliteMessageStore.MakeConnectionString(sqlite.Value.DatabasePath));
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS courierlane_idempotency_v1 (
                scope TEXT NOT NULL,
                message_key TEXT NOT NULL,
                token TEXT NOT NULL,
                completed INTEGER NOT NULL,
                expires REAL NOT NULL,
                PRIMARY KEY (scope, message_key)
            );
            CREATE INDEX IF NOT EXISTS courierlane_idempotency_v1_expiry ON courierlane_idempotency_v1(scope, expires);
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
