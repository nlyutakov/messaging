using RabbitMQ.Client;

namespace Messaging.RabbitMQ.Connection;

internal interface IRabbitMqConnection : IAsyncDisposable
{
    Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);
}