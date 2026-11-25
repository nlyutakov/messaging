using CourierLane.Abstractions.Idempotency;
using CourierLane.RabbitMQ.Configuration;
using Npgsql;

namespace CourierLane.Idempotency.PostgreSql;

internal class PostgreSqlMessageStore(NpgsqlDataSource source, CourierLaneOptions options, string prefix) : IMessageDeduplicationStore
{
    private readonly string scope = $"{prefix.Length}:{prefix}{options.VirtualHost}";
    private readonly TimeSpan processing = options.Idempotency.ProcessingTimeout;
    private readonly TimeSpan retention = options.Idempotency.RetentionPeriod;
    private int acquisitions;

    public async Task<MessageAcquireResponse> TryAcquireAsync(string key, CancellationToken cancellationToken = default)
    {
        if (Interlocked.Increment(ref this.acquisitions) % 64 == 0)
        {
            await this.CleanupAsync(cancellationToken);
        }

        var token = Guid.NewGuid().ToString("N");
        await using var command = this.CreateCommand("""
            INSERT INTO courierlane_idempotency_v1 AS entries (scope, message_key, token, completed, expires)
            VALUES (@scope, @key, @token, false, clock_timestamp() + @ttl)
            ON CONFLICT (scope, message_key) DO UPDATE SET (token, completed, expires) = (
                SELECT CASE WHEN entries.expires <= observed.now THEN excluded.token ELSE entries.token END,
                       CASE WHEN entries.expires <= observed.now THEN false ELSE entries.completed END,
                       CASE WHEN entries.expires <= observed.now THEN observed.now + @ttl ELSE entries.expires END
                FROM (SELECT clock_timestamp() AS now) AS observed
            )
            RETURNING token, completed;
            """, new MessageLease(key, token));
        command.Parameters.AddWithValue("ttl", this.processing);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException("PostgreSQL did not return an acquire result.");
        }

        if (reader.GetString(0) == token)
        {
            return new(MessageAcquireResult.Acquired, new MessageLease(key, token));
        }

        return new(reader.GetBoolean(1) ? MessageAcquireResult.AlreadyCompleted : MessageAcquireResult.InProgress);
    }

    public Task<bool> RenewAsync(MessageLease lease, CancellationToken cancellationToken = default) =>
        this.UpdateAsync(lease, this.processing, false, cancellationToken);

    public Task<bool> MarkCompletedAsync(MessageLease lease, CancellationToken cancellationToken = default) =>
        this.UpdateAsync(lease, this.retention, true, cancellationToken);

    public async Task ReleaseAsync(MessageLease lease, CancellationToken cancellationToken = default)
    {
        await using var command = this.CreateCommand("""
            DELETE FROM courierlane_idempotency_v1 WHERE scope = @scope AND message_key = @key AND token = @token AND NOT completed;
            """, lease);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<bool> UpdateAsync(MessageLease lease, TimeSpan ttl, bool completed, CancellationToken cancellationToken)
    {
        await using var command = this.CreateCommand("""
            UPDATE courierlane_idempotency_v1 SET completed = @completed, expires = clock_timestamp() + @ttl
            WHERE scope = @scope AND message_key = @key AND token = @token AND NOT completed AND expires > clock_timestamp();
            """, lease);
        command.Parameters.AddWithValue("completed", completed);
        command.Parameters.AddWithValue("ttl", ttl);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    private async Task CleanupAsync(CancellationToken cancellationToken)
    {
        await using var command = source.CreateCommand("""
            WITH expired AS (
                SELECT message_key FROM courierlane_idempotency_v1 WHERE scope = @scope AND expires <= statement_timestamp()
                ORDER BY expires LIMIT 64 FOR UPDATE SKIP LOCKED
            )
            DELETE FROM courierlane_idempotency_v1 AS entries USING expired
            WHERE entries.scope = @scope AND entries.message_key = expired.message_key;
            """);
        command.Parameters.AddWithValue("scope", this.scope);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private NpgsqlCommand CreateCommand(string sql, MessageLease lease)
    {
        var command = source.CreateCommand(sql);
        command.Parameters.AddWithValue("scope", this.scope);
        command.Parameters.AddWithValue("key", lease.Key);
        command.Parameters.AddWithValue("token", lease.Token);
        return command;
    }
}
