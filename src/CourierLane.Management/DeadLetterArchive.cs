using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;

namespace CourierLane.Management;

internal class DeadLetterArchive(IOptions<DeadLetterOptions> options)
{
    private readonly string connectionString = new SqliteConnectionStringBuilder
    {
        DataSource = options.Value.DatabasePath,
        DefaultTimeout = 5
    }.ToString();

    internal async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var connection = await this.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS dead_letters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fingerprint TEXT UNIQUE,
                received TEXT NOT NULL,
                dlq TEXT NOT NULL,
                target TEXT,
                message_id TEXT,
                message_type TEXT,
                failure TEXT,
                replay_count INTEGER NOT NULL,
                status INTEGER NOT NULL DEFAULT 0,
                payload BLOB,
                content_type TEXT,
                correlation_id TEXT,
                headers TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS dead_letters_status_id ON dead_letters(status, id);
            CREATE TABLE IF NOT EXISTS dead_letter_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dead_letter_id INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                actor TEXT NOT NULL,
                status INTEGER NOT NULL
            );
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    internal async Task AddAsync(DeadLetterDetails entry, string? fingerprint, CancellationToken cancellationToken)
    {
        var row = entry.Summary;
        await using var connection = await this.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO dead_letters (fingerprint, received, dlq, target, message_id, message_type, failure, replay_count, payload, content_type, correlation_id, headers)
            VALUES ($fingerprint, $received, $dlq, $target, $messageId, $type, $failure, $count, $body, $contentType, $correlationId, $headers)
            ON CONFLICT(fingerprint) DO NOTHING;
            """;
        Add(command, "$fingerprint", fingerprint);
        Add(command, "$received", row.ReceivedAt.ToString("O"));
        Add(command, "$dlq", row.DeadLetterQueue);
        Add(command, "$target", row.OriginalQueue);
        Add(command, "$messageId", row.MessageId);
        Add(command, "$type", row.MessageType);
        Add(command, "$failure", row.Failure);
        Add(command, "$count", row.ReplayCount);
        Add(command, "$body", entry.Payload);
        Add(command, "$contentType", entry.ContentType);
        Add(command, "$correlationId", entry.CorrelationId);
        Add(command, "$headers", JsonSerializer.Serialize(entry.Headers));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    internal async Task<IReadOnlyList<DeadLetterSummary>> ListAsync(long after, int limit, DeadLetterStatus? status, CancellationToken cancellationToken)
    {
        await using var connection = await this.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT id, received, dlq, target, message_id, message_type, failure, replay_count, status
            FROM dead_letters WHERE id > $after AND ($status IS NULL OR status = $status)
            ORDER BY id LIMIT $limit;
            """;
        Add(command, "$after", after);
        Add(command, "$limit", limit);
        Add(command, "$status", (int?)status);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<DeadLetterSummary>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadSummary(reader));
        }

        return rows;
    }

    internal async Task<DeadLetterDetails?> InspectAsync(long id, CancellationToken cancellationToken)
    {
        await using var connection = await this.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT id, received, dlq, target, message_id, message_type, failure, replay_count, status, payload, content_type, correlation_id, headers
            FROM dead_letters WHERE id = $id;
            """;
        Add(command, "$id", id);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        var headers = JsonSerializer.Deserialize<Dictionary<string, string>>(reader.GetString(12))!;
        return new DeadLetterDetails(ReadSummary(reader), reader.IsDBNull(9) ? null : reader.GetFieldValue<byte[]>(9), ReadString(reader, 10), ReadString(reader, 11), headers);
    }

    internal async Task<IReadOnlyDictionary<DeadLetterStatus, long>> CountAsync(CancellationToken cancellationToken)
    {
        await using var connection = await this.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT status, COUNT(*) FROM dead_letters GROUP BY status;";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var totals = new Dictionary<DeadLetterStatus, long>();
        while (await reader.ReadAsync(cancellationToken))
        {
            totals.Add((DeadLetterStatus)reader.GetInt32(0), reader.GetInt64(1));
        }

        return totals;
    }

    internal async Task<bool> TransitionAsync(long id, DeadLetterStatus expected, DeadLetterStatus next, string actor, CancellationToken cancellationToken)
    {
        await using var connection = await this.OpenAsync(cancellationToken);
        await using var transaction = connection.BeginTransaction();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE dead_letters
            SET status = $next,
                payload = CASE WHEN $erase THEN NULL ELSE payload END,
                headers = CASE WHEN $erase THEN '{}' ELSE headers END,
                correlation_id = CASE WHEN $erase THEN NULL ELSE correlation_id END
            WHERE id = $id AND status = $expected;
            """;
        Add(command, "$id", id);
        Add(command, "$next", (int)next);
        Add(command, "$expected", (int)expected);
        Add(command, "$erase", next == DeadLetterStatus.Discarded);
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
        {
            return false;
        }

        await using var audit = connection.CreateCommand();
        audit.Transaction = transaction;
        audit.CommandText = "INSERT INTO dead_letter_audit (dead_letter_id, timestamp, actor, status) VALUES ($id, $timestamp, $actor, $status);";
        Add(audit, "$id", id);
        Add(audit, "$timestamp", DateTimeOffset.UtcNow.ToString("O"));
        Add(audit, "$actor", actor);
        Add(audit, "$status", (int)next);
        await audit.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    private async Task<SqliteConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(this.connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    private static void Add(SqliteCommand command, string name, object? value) => command.Parameters.AddWithValue(name, value ?? DBNull.Value);

    private static string? ReadString(SqliteDataReader reader, int index) => reader.IsDBNull(index) ? null : reader.GetString(index);

    private static DeadLetterSummary ReadSummary(SqliteDataReader reader)
    {
        return new DeadLetterSummary(reader.GetInt64(0), DateTimeOffset.Parse(reader.GetString(1)), reader.GetString(2), ReadString(reader, 3), ReadString(reader, 4), ReadString(reader, 5), ReadString(reader, 6), reader.GetInt32(7), (DeadLetterStatus)reader.GetInt32(8));
    }
}
