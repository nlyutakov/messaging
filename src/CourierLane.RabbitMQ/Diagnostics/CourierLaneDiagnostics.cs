using System.Diagnostics;
using System.Diagnostics.Metrics;
using RabbitMQ.AMQP.Client;

namespace CourierLane.RabbitMQ.Diagnostics;

public static class CourierLaneDiagnostics
{
    public const string SourceName = "CourierLane.RabbitMQ";

    internal static ActivitySource Activities { get; } = new(SourceName);
    private static Meter Metrics { get; } = new(SourceName);

    internal static Counter<long> Published { get; } = Metrics.CreateCounter<long>("courierlane.published");
    internal static Counter<long> Processed { get; } = Metrics.CreateCounter<long>("courierlane.processed");
    internal static Counter<long> Retries { get; } = Metrics.CreateCounter<long>("courierlane.retries");
    internal static Counter<long> Duplicates { get; } = Metrics.CreateCounter<long>("courierlane.duplicates");
    internal static Counter<long> LeaseFailures { get; } = Metrics.CreateCounter<long>("courierlane.lease.failures");
    internal static Histogram<double> PublishDuration { get; } = Metrics.CreateHistogram<double>("courierlane.publish.duration", "ms");
    internal static Histogram<double> ProcessDuration { get; } = Metrics.CreateHistogram<double>("courierlane.process.duration", "ms");

    internal static void InjectTrace(IMessage message)
    {
        if (Activity.Current is { IdFormat: ActivityIdFormat.W3C, Id: { } traceParent } activity)
        {
            message.Property("traceparent", traceParent);
            if (!string.IsNullOrWhiteSpace(activity.TraceStateString))
            {
                message.Property("tracestate", activity.TraceStateString);
            }
        }
    }

    internal static Activity? StartProcessing(IMessage message, string operation, string destination)
    {
        var parent = message.Property("traceparent") as string;
        var state = message.Property("tracestate") as string;
        Activity? activity = ActivityContext.TryParse(parent, state, isRemote: true, out var trace)
            ? Activities.StartActivity(operation, ActivityKind.Consumer, trace)
            : Activities.StartActivity(operation, ActivityKind.Consumer);
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.destination.name", destination);
        return activity;
    }
}
