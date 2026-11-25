using System.Text.Json;
using CourierLane.Abstractions.Producers;
using CourierLane.RabbitMQ.Configuration;
using CourierLane.RabbitMQ.Producers;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CourierLane.Outbox.PostgreSql;

public class PostgreSqlOutbox(MessageTypeRegistry contracts, IOptions<PostgreSqlOutboxOptions> options)
{
    private readonly JsonSerializerOptions jsonOptions = new(JsonSerializerDefaults.Web);

    public Task<string> EnqueueCommandAsync<TCommand>(NpgsqlTransaction transaction, string queue, TCommand command, SendOptions? metadata = null, CancellationToken cancellationToken = default) =>
        this.EnqueueAsync(transaction, queue, command, metadata, false, cancellationToken);

    public Task<string> EnqueueEventAsync<TEvent>(NpgsqlTransaction transaction, string exchange, TEvent message, SendOptions? metadata = null, CancellationToken cancellationToken = default) =>
        this.EnqueueAsync(transaction, exchange, message, metadata, true, cancellationToken);

    private async Task<string> EnqueueAsync<TMessage>(NpgsqlTransaction transaction, string destination, TMessage value, SendOptions? requested, bool isEvent, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        ArgumentNullException.ThrowIfNull(value);
        var connection = transaction.Connection ?? throw new InvalidOperationException("The outbox requires an active business transaction.");
        var type = contracts.GetMessageTypeId<TMessage>();
        var payload = JsonSerializer.SerializeToUtf8Bytes(value, this.jsonOptions);
        var metadata = new SendOptions
        {
            MessageId = requested?.MessageId ?? Guid.NewGuid().ToString("N"),
            CorrelationId = requested?.CorrelationId,
            CausationId = requested?.CausationId,
            Source = requested?.Source,
            Timestamp = requested?.Timestamp ?? DateTimeOffset.UtcNow,
            Headers = requested?.Headers
        };
        RabbitMessageMetadata.Create(payload, type, metadata);

        await using var command = new NpgsqlCommand("""
            INSERT INTO courierlane_outbox_v1 (id, scope, destination, type, is_event, payload, metadata)
            VALUES (@id, @scope, @destination, @type, @event, @payload, @metadata);
            """, connection, transaction);
        command.Parameters.AddWithValue("id", Guid.NewGuid());
        command.Parameters.AddWithValue("scope", options.Value.Scope);
        command.Parameters.AddWithValue("destination", destination);
        command.Parameters.AddWithValue("type", type);
        command.Parameters.AddWithValue("event", isEvent);
        command.Parameters.AddWithValue("payload", payload);
        command.Parameters.AddWithValue("metadata", JsonSerializer.Serialize(metadata));
        await command.ExecuteNonQueryAsync(cancellationToken);
        return metadata.MessageId;
    }
}
