using System.Diagnostics;
using Messaging.RabbitMQ.Diagnostics;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Messaging.RabbitMQ.Tests;

public class MessagingDiagnosticsTests
{
    [Fact]
    public void TraceContextSurvivesTheMessageBoundary()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == MessagingDiagnostics.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        using var publisher = MessagingDiagnostics.Activities.StartActivity("test publish", ActivityKind.Producer);
        Assert.NotNull(publisher);
        publisher.TraceStateString = "messaging=test";
        var message = new AmqpMessage(new byte[] { 1, 2, 3 });
        MessagingDiagnostics.InjectTrace(message);

        publisher.Stop();
        using var consumer = MessagingDiagnostics.StartProcessing(message, "test receive", "orders");
        Assert.NotNull(consumer);
        Assert.Equal(publisher.TraceId, consumer.TraceId);
        Assert.Equal(publisher.SpanId, consumer.ParentSpanId);
        Assert.Equal("messaging=test", consumer.TraceStateString);
    }
}
