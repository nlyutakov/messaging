namespace Messaging.Abstractions.Producers;

public class SendOptions
{
    public string? MessageId { get; init; }
    public string? CorrelationId { get; init; }
    public string? CausationId { get; init; }
    public string? Source { get; init; }
    public DateTimeOffset? Timestamp { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}
