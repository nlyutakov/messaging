namespace Messaging.Management;

public enum DeadLetterStatus
{
    Pending,
    Replaying,
    Replayed,
    Archived,
    Discarded
}

public enum DeadLetterAction
{
    Replay,
    Archive,
    Discard
}

public enum DeadLetterResult
{
    Applied,
    Missing,
    Conflict,
    Rejected,
    Uncertain
}

public record DeadLetterSummary(long Id, DateTimeOffset ReceivedAt, string DeadLetterQueue, string? OriginalQueue, string? MessageId, string? MessageType, string? Failure, int ReplayCount, DeadLetterStatus Status);

public record DeadLetterDetails(DeadLetterSummary Summary, byte[]? Payload, string? ContentType, string? CorrelationId, IReadOnlyDictionary<string, string> Headers);

public interface IDeadLetterManager
{
    Task<IReadOnlyList<DeadLetterSummary>> ListAsync(long after = 0, int limit = 50, DeadLetterStatus? status = null, CancellationToken cancellationToken = default);
    Task<DeadLetterDetails?> InspectAsync(long id, CancellationToken cancellationToken = default);
    Task<IReadOnlyDictionary<DeadLetterStatus, long>> CountAsync(CancellationToken cancellationToken = default);
    Task<DeadLetterResult> ApplyAsync(long id, DeadLetterAction action, string actor, CancellationToken cancellationToken = default);
}

public class DeadLetterOptions
{
    public string DatabasePath { get; set; } = "";
    public Dictionary<string, string[]> Sources { get; set; } = new(StringComparer.Ordinal);
    public int MaxReplayAttempts { get; set; } = 1;
}
