using Messaging.Abstractions.Idempotency;
using Messaging.RabbitMQ.Configuration;
using Microsoft.Data.Sqlite;

namespace Messaging.Idempotency.Sqlite;

internal class SqliteMessageStore(string databasePath, string prefix, RabbitMessagingOptions options) : IMessageDeduplicationStore
{
    private readonly string connectionString = MakeConnectionString(databasePath);
    private readonly string scope = $"{prefix.Length}:{prefix}{options.VirtualHost}";
    private readonly double processingSeconds = Math.Ceiling(options.Idempotency.ProcessingTimeout.TotalMilliseconds) / 1000;
    private readonly double retentionSeconds = Math.Ceiling(options.Idempotency.RetentionPeriod.TotalMilliseconds) / 1000;
    private int acquisitions;

    internal static string MakeConnectionString(string databasePath) => new SqliteConnectionStringBuilder
    {
        DataSource = databasePath,
        DefaultTimeout = 5
    }.ToString();

    public async Task<MessageAcquireResponse> TryAcquireAsync(string key, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(this.connectionString);
        await connection.OpenAsync(cancellationToken);
        if (Interlocked.Increment(ref this.acquisitions) % 64 == 0)
        {
            await this.CleanupAsync(connection, cancellationToken);
        }

        var lease = new MessageLease(key, Guid.NewGuid().ToString("N"));
        await using var command = this.CreateCommand(connection, """
            INSERT INTO messaging_idempotency_v1 (scope, message_key, token, completed, expires)
            VALUES ($scope, $key, $token, 0, unixepoch('subsec') + $ttl)
            ON CONFLICT (scope, message_key) DO UPDATE SET
                token = CASE WHEN expires <= unixepoch('subsec') THEN excluded.token ELSE token END,
                completed = CASE WHEN expires <= unixepoch('subsec') THEN 0 ELSE completed END,
                expires = CASE WHEN expires <= unixepoch('subsec') THEN excluded.expires ELSE expires END
            RETURNING token, completed;
            """, lease);
        command.Parameters.AddWithValue("$ttl", this.processingSeconds);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException("SQLite did not return an acquire result.");
        }

        if (reader.GetString(0) == lease.Token)
        {
            return new(MessageAcquireResult.Acquired, lease);
        }

        return new(reader.GetBoolean(1) ? MessageAcquireResult.AlreadyCompleted : MessageAcquireResult.InProgress);
    }

    public Task<bool> RenewAsync(MessageLease lease, CancellationToken cancellationToken = default) =>
        this.UpdateAsync(lease, this.processingSeconds, false, cancellationToken);

    public Task<bool> MarkCompletedAsync(MessageLease lease, CancellationToken cancellationToken = default) =>
        this.UpdateAsync(lease, this.retentionSeconds, true, cancellationToken);

    public async Task ReleaseAsync(MessageLease lease, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(this.connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = this.CreateCommand(connection, """
            DELETE FROM messaging_idempotency_v1 WHERE scope = $scope AND message_key = $key AND token = $token AND completed = 0;
            """, lease);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<bool> UpdateAsync(MessageLease lease, double ttl, bool completed, CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(this.connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = this.CreateCommand(connection, """
            UPDATE messaging_idempotency_v1 SET completed = $completed, expires = unixepoch('subsec') + $ttl
            WHERE scope = $scope AND message_key = $key AND token = $token AND completed = 0 AND expires > unixepoch('subsec');
            """, lease);
        command.Parameters.AddWithValue("$completed", completed);
        command.Parameters.AddWithValue("$ttl", ttl);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    private async Task CleanupAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM messaging_idempotency_v1 WHERE scope = $scope AND message_key IN (
                SELECT message_key FROM messaging_idempotency_v1 WHERE scope = $scope AND expires <= unixepoch('subsec')
                ORDER BY expires LIMIT 64
            );
            """;
        command.Parameters.AddWithValue("$scope", this.scope);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private SqliteCommand CreateCommand(SqliteConnection connection, string sql, MessageLease lease)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$scope", this.scope);
        command.Parameters.AddWithValue("$key", lease.Key);
        command.Parameters.AddWithValue("$token", lease.Token);
        return command;
    }
}
