using CourierLane.Abstractions.Producers;
using CourierLane.RabbitMQ.Producers;
using Xunit;

namespace CourierLane.RabbitMQ.Tests;

public class MessageMetadataTests
{
    [Theory]
    [InlineData("MESSAGE-TYPE")]
    [InlineData("courierlane-replay-count")]
    [InlineData("x-first-death-queue")]
    [InlineData("reply-to")]
    public void ReservedHeadersCannotOverrideTransportMetadata(string header)
    {
        var options = new SendOptions { Headers = new Dictionary<string, string> { [header] = "value" } };
        Assert.Throws<ArgumentException>(() => RabbitMessageMetadata.Create([1], "order.v1", options));
    }

    [Fact]
    public void DefaultIdentityAndTimestampAreGenerated()
    {
        var first = RabbitMessageMetadata.Create([1], "order.v1", null);
        var second = RabbitMessageMetadata.Create([1], "order.v1", null);
        Assert.True(Guid.TryParseExact(first.MessageId()?.ToString(), "N", out _));
        Assert.NotEqual(first.MessageId(), second.MessageId());
        Assert.True(DateTimeOffset.TryParse(first.Property("courierlane-timestamp") as string, out var timestamp));
        Assert.Equal(TimeSpan.Zero, timestamp.Offset);
    }

    [Fact]
    public void LogicalRpcCorrelationLeavesNativeMatchingToRequester()
    {
        var options = new SendOptions { MessageId = "stable-id", CorrelationId = "logical-correlation" };
        var rpc = RabbitMessageMetadata.Create([1], "order.v1", options, useNativeCorrelation: false);
        var command = RabbitMessageMetadata.Create([1], "order.v1", options);
        Assert.Equal("stable-id", rpc.MessageId());
        Assert.Equal("logical-correlation", rpc.Property("courierlane-correlation-id"));
        Assert.Null(rpc.CorrelationId());
        Assert.Equal("logical-correlation", command.CorrelationId());
    }
}
