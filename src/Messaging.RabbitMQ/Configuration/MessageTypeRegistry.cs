namespace Messaging.RabbitMQ.Configuration;

public class MessageTypeRegistry
{
    private readonly Dictionary<Type, string> types = new();
    public void Register<TMessage>(string messageTypeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageTypeId);

        if (this.types.ContainsKey(typeof(TMessage)) || this.types.ContainsValue(messageTypeId))
        {
            throw new InvalidOperationException($"Duplicate message type/id '{messageTypeId}'.");
        }

        this.types.Add(typeof(TMessage), messageTypeId);
    }
    public string GetMessageTypeId<TMessage>() => this.types.TryGetValue(typeof(TMessage), out var id) ? id : throw new InvalidOperationException($"Message type '{typeof(TMessage).Name}' is not registered.");
}
