using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CourierLane.Outbox.PostgreSql;

internal class OutboxStorage([FromKeyedServices(PostgreSqlOutboxExtensions.ConnectionKey)] NpgsqlDataSource source, IOptions<PostgreSqlOutboxOptions> options)
{
    internal async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var connection = await source.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var command = new NpgsqlCommand("""
            SELECT pg_advisory_xact_lock(811435362916360162);
            CREATE TABLE IF NOT EXISTS courierlane_outbox_v1 (
                id uuid PRIMARY KEY,
                scope text NOT NULL,
                destination text NOT NULL,
                type text NOT NULL,
                is_event boolean NOT NULL,
                payload bytea NOT NULL,
                metadata text NOT NULL,
                ready_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                attempts integer NOT NULL DEFAULT 0,
                claim_token uuid,
                claimed_until timestamptz,
                published_at timestamptz
            );
            CREATE INDEX IF NOT EXISTS courierlane_outbox_v1_ready ON courierlane_outbox_v1(scope, ready_at, id) WHERE published_at IS NULL;
            CREATE INDEX IF NOT EXISTS courierlane_outbox_v1_published ON courierlane_outbox_v1(scope, published_at) WHERE published_at IS NOT NULL;
            """, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    internal async Task<OutboxClaim?> ClaimAsync(CancellationToken cancellationToken)
    {
        var token = Guid.NewGuid();
        await using var command = this.CreateCommand("""
            WITH candidate AS (
                SELECT id FROM courierlane_outbox_v1
                WHERE scope = @scope AND published_at IS NULL AND ready_at <= statement_timestamp()
                  AND (claimed_until IS NULL OR claimed_until <= statement_timestamp())
                ORDER BY ready_at, id LIMIT 1 FOR UPDATE SKIP LOCKED
            )
            UPDATE courierlane_outbox_v1 AS entry SET
                claim_token = @token, claimed_until = clock_timestamp() + @lease, attempts = LEAST(entry.attempts, 30) + 1
            FROM candidate WHERE entry.id = candidate.id
            RETURNING entry.id, entry.destination, entry.type, entry.is_event, entry.payload, entry.metadata, entry.attempts;
            """);
        command.Parameters.AddWithValue("token", token);
        command.Parameters.AddWithValue("lease", options.Value.ClaimTimeout);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new OutboxClaim(reader.GetGuid(0), token, reader.GetString(1), reader.GetString(2), reader.GetBoolean(3), reader.GetFieldValue<byte[]>(4), reader.GetString(5), reader.GetInt32(6));
    }

    internal async Task<bool> CompleteAsync(OutboxClaim claim, CancellationToken cancellationToken)
    {
        await using var command = this.CreateCommand("""
            UPDATE courierlane_outbox_v1 SET published_at = clock_timestamp(), claim_token = NULL, claimed_until = NULL
            WHERE scope = @scope AND id = @id AND claim_token = @token AND published_at IS NULL;
            """, claim);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    internal async Task RetryAsync(OutboxClaim claim, CancellationToken cancellationToken)
    {
        await using var command = this.CreateCommand("""
            UPDATE courierlane_outbox_v1 SET ready_at = clock_timestamp() + @delay, claim_token = NULL, claimed_until = NULL
            WHERE scope = @scope AND id = @id AND claim_token = @token AND published_at IS NULL;
            """, claim);
        command.Parameters.AddWithValue("delay", TimeSpan.FromSeconds(Math.Min(300, Math.Pow(2, claim.Attempts - 1))));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    internal async Task CleanupAsync(CancellationToken cancellationToken)
    {
        await using var command = this.CreateCommand("""
            WITH expired AS (
                SELECT id FROM courierlane_outbox_v1 WHERE scope = @scope AND published_at <= statement_timestamp() - @retention
                ORDER BY published_at LIMIT 128 FOR UPDATE SKIP LOCKED
            )
            DELETE FROM courierlane_outbox_v1 AS entry USING expired WHERE entry.id = expired.id;
            """);
        command.Parameters.AddWithValue("retention", options.Value.RetentionPeriod);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    internal async Task<long> BacklogAsync(CancellationToken cancellationToken)
    {
        await using var command = this.CreateCommand("""
            SELECT COUNT(*) FROM courierlane_outbox_v1 WHERE scope = @scope AND published_at IS NULL;
            """);
        return (long)(await command.ExecuteScalarAsync(cancellationToken)
            ?? throw new InvalidOperationException("Outbox backlog query did not return a count."));
    }

    private NpgsqlCommand CreateCommand(string sql, OutboxClaim? claim = null)
    {
        var command = source.CreateCommand(sql);
        command.Parameters.AddWithValue("scope", options.Value.Scope);
        if (claim is not null)
        {
            command.Parameters.AddWithValue("id", claim.Id);
            command.Parameters.AddWithValue("token", claim.Token);
        }

        return command;
    }
}

internal record OutboxClaim(Guid Id, Guid Token, string Destination, string Type, bool IsEvent, byte[] Payload, string Metadata, int Attempts);
