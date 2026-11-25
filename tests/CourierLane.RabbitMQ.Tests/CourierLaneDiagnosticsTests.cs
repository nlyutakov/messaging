using System.Diagnostics;
using CourierLane.RabbitMQ.Diagnostics;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace CourierLane.RabbitMQ.Tests;

public class CourierLaneDiagnosticsTests
{
    [Fact]
    public void TraceContextSurvivesTheMessageBoundary()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == CourierLaneDiagnostics.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        using var publisher = CourierLaneDiagnostics.Activities.StartActivity("test publish", ActivityKind.Producer);
        Assert.NotNull(publisher);
        publisher.TraceStateString = "courierlane=test";
        var message = new AmqpMessage(new byte[] { 1, 2, 3 });
        CourierLaneDiagnostics.InjectTrace(message);

        publisher.Stop();
        using var consumer = CourierLaneDiagnostics.StartProcessing(message, "test receive", "orders");
        Assert.NotNull(consumer);
        Assert.Equal(publisher.TraceId, consumer.TraceId);
        Assert.Equal(publisher.SpanId, consumer.ParentSpanId);
        Assert.Equal("courierlane=test", consumer.TraceStateString);
    }
}
