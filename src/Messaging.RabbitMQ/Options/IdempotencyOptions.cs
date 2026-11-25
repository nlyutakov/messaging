namespace Messaging.RabbitMQ.Options;

public class IdempotencyOptions
{
    public string Provider { get; set; } = "InMemory";

    public TimeSpan ProcessingTimeout { get; set; } = TimeSpan.FromMinutes(5);

    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(7);
}