using System.Text.Json;
using CourierLane.Abstractions.Producers;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

namespace CourierLane.RabbitMQ.Producers;

internal static class RabbitMessageMetadata
{
    private static readonly HashSet<string> Reserved = new(StringComparer.OrdinalIgnoreCase)
    {
        "message-type", "message-id", "correlation-id", "content-type", "reply-to", "routing-key", "traceparent", "tracestate"
    };

    internal static IMessage Create(byte[] payload, string typeId, SendOptions? options, bool useNativeCorrelation = true)
    {
        var message = new AmqpMessage(payload)
            .Property("message-type", typeId)
            .ContentType("application/json")
            .MessageId(Validate(options?.MessageId, nameof(SendOptions.MessageId)) ?? Guid.NewGuid().ToString("N"))
            .Property("courierlane-timestamp", (options?.Timestamp ?? DateTimeOffset.UtcNow).ToUniversalTime().ToString("O"));
        if (Validate(options?.CorrelationId, nameof(SendOptions.CorrelationId)) is { } correlation)
        {
            message.Property("courierlane-correlation-id", correlation);
            if (useNativeCorrelation)
            {
                message.CorrelationId(correlation);
            }
        }

        if (Validate(options?.CausationId, nameof(SendOptions.CausationId)) is { } causation)
        {
            message.Property("courierlane-causation-id", causation);
        }

        if (Validate(options?.Source, nameof(SendOptions.Source)) is { } source)
        {
            message.Property("courierlane-source", source);
        }

        if (options?.Headers is { Count: > 0 } headers)
        {
            var headerNames = new List<string>();
            foreach (var (name, value) in headers)
            {
                if (string.IsNullOrWhiteSpace(name) || IsReservedHeader(name) || value is null)
                {
                    throw new ArgumentException("Header names must be non-empty, non-reserved and have string values.", nameof(options));
                }

                message.Property(name, value);
                headerNames.Add(name);
            }

            message.Property("courierlane-header-names", JsonSerializer.Serialize(headerNames));
        }

        return message;
    }

    internal static bool IsReservedHeader(string name) =>
        Reserved.Contains(name) || name.StartsWith("courierlane-", StringComparison.OrdinalIgnoreCase) ||
        name.StartsWith("x-", StringComparison.OrdinalIgnoreCase);

    private static string? Validate(string? value, string name)
    {
        if (value is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(value, name);
        }

        return value;
    }
}
