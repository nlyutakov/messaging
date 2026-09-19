namespace Messaging.RabbitMQ.Consumers;

internal record RabbitMqConsumerDefinition(Type MessageType, Type ConsumerType, string QueueName);