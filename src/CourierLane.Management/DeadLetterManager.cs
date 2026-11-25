using CourierLane.RabbitMQ.Connections;
using CourierLane.RabbitMQ.Producers;
using CourierLane.Abstractions.Producers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.AMQP.Client;

namespace CourierLane.Management;

internal class DeadLetterManager(DeadLetterArchive archive, RabbitConnectionManager connections, IOptions<DeadLetterOptions> options, ILogger<DeadLetterManager> logger) : IDeadLetterManager
{
    public Task<IReadOnlyList<DeadLetterSummary>> ListAsync(long after = 0, int limit = 50, DeadLetterStatus? status = null, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(after);
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, 100);
        return archive.ListAsync(after, limit, status, cancellationToken);
    }

    public Task<DeadLetterDetails?> InspectAsync(long id, CancellationToken cancellationToken = default) => archive.InspectAsync(id, cancellationToken);

    public Task<IReadOnlyDictionary<DeadLetterStatus, long>> CountAsync(CancellationToken cancellationToken = default) => archive.CountAsync(cancellationToken);

    public async Task<DeadLetterResult> ApplyAsync(long id, DeadLetterAction action, string actor, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(actor);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(actor.Length, 256);
        if (!Enum.IsDefined(action))
        {
            throw new ArgumentOutOfRangeException(nameof(action));
        }

        var entry = await archive.InspectAsync(id, cancellationToken);
        if (entry is null)
        {
            return DeadLetterResult.Missing;
        }

        if (action == DeadLetterAction.Replay)
        {
            return await this.ReplayAsync(entry, actor, cancellationToken);
        }

        var status = entry.Summary.Status;
        var next = action == DeadLetterAction.Archive ? DeadLetterStatus.Archived : DeadLetterStatus.Discarded;
        if (status != DeadLetterStatus.Pending && !(next == DeadLetterStatus.Discarded && status == DeadLetterStatus.Archived))
        {
            return DeadLetterResult.Conflict;
        }

        return await archive.TransitionAsync(id, status, next, actor, cancellationToken) ? DeadLetterResult.Applied : DeadLetterResult.Conflict;
    }

    private async Task<DeadLetterResult> ReplayAsync(DeadLetterDetails entry, string actor, CancellationToken cancellationToken)
    {
        var row = entry.Summary;
        if (row.Status != DeadLetterStatus.Pending)
        {
            return DeadLetterResult.Conflict;
        }

        if (row.OriginalQueue is not { } target || row.MessageId is not { Length: > 0 } messageId || row.MessageType is not { Length: > 0 } type || entry.Payload is not { } payload ||
            row.ReplayCount >= options.Value.MaxReplayAttempts || !options.Value.Sources.TryGetValue(row.DeadLetterQueue, out var targets) ||
            !targets.Contains(target, StringComparer.Ordinal))
        {
            return DeadLetterResult.Rejected;
        }

        if (!await archive.TransitionAsync(row.Id, DeadLetterStatus.Pending, DeadLetterStatus.Replaying, actor, cancellationToken))
        {
            return DeadLetterResult.Conflict;
        }

        var submitted = false;
        try
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            deadline.CancelAfter(TimeSpan.FromSeconds(15));
            var connection = await connections.GetConnectionAsync(deadline.Token);
            using var publisher = await connection.PublisherBuilder().Queue(target).BuildAsync(deadline.Token);
            var message = RabbitMessageMetadata.Create(payload, type, new SendOptions
            {
                MessageId = messageId,
                CorrelationId = ReadHeader(entry, "courierlane-correlation-id") ?? entry.CorrelationId,
                CausationId = ReadHeader(entry, "courierlane-causation-id"),
                Source = ReadHeader(entry, "courierlane-source"),
                Timestamp = DateTimeOffset.TryParse(ReadHeader(entry, "courierlane-timestamp"), out var timestamp) ? timestamp : null,
                Headers = entry.Headers.Where(header => !RabbitMessageMetadata.IsReservedHeader(header.Key)).ToDictionary()
            })
                .Property(DeadLetterIngestionService.ReplayCountProperty, row.ReplayCount + 1)
                .Durable(true);
            if (entry.ContentType is { } contentType)
            {
                message.ContentType(contentType);
            }

            submitted = true;
            var result = await publisher.PublishAsync(message, deadline.Token);
            if (result.Outcome.State == OutcomeState.Accepted)
            {
                await this.FinishAsync(row.Id, DeadLetterStatus.Replayed, actor);
                return DeadLetterResult.Applied;
            }

            await this.FinishAsync(row.Id, DeadLetterStatus.Pending, actor);
            return DeadLetterResult.Rejected;
        }
        catch (Exception ex)
        {
            logger.LogWarning("Replay of {Id} failed with {ErrorType}", row.Id, ex.GetType().Name);
            if (!submitted)
            {
                try
                {
                    await this.FinishAsync(row.Id, DeadLetterStatus.Pending, actor);
                    return DeadLetterResult.Rejected;
                }
                catch (Exception resetError)
                {
                    logger.LogWarning("Replay state reset for {Id} failed with {ErrorType}", row.Id, resetError.GetType().Name);
                }
            }

            return DeadLetterResult.Uncertain;
        }
    }

    private async Task FinishAsync(long id, DeadLetterStatus status, string actor)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        if (!await archive.TransitionAsync(id, DeadLetterStatus.Replaying, status, actor, deadline.Token))
        {
            throw new InvalidOperationException("The replay claim changed before settlement.");
        }
    }

    private static string? ReadHeader(DeadLetterDetails entry, string key) =>
        entry.Headers.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value : null;
}
