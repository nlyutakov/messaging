namespace Messaging.RabbitMQ.Consumers;

internal sealed class RabbitMqConsumerRegistry
{
    
    public RabbitMqConsumerRegistry(
        IEnumerable<RabbitMqConsumerDefinition> definitions)
    {
        Definitions = definitions.ToArray();
    }

    public IReadOnlyCollection<RabbitMqConsumerDefinition> Definitions
    {
        get;
    }
}