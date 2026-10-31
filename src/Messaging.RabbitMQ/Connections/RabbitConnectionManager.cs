using Messaging.RabbitMQ.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

namespace Messaging.RabbitMQ.Connections;

public class RabbitConnectionManager : IAsyncDisposable
{
    private readonly RabbitMessagingOptions options;
    private readonly ILogger<RabbitConnectionManager> logger;
    private readonly SemaphoreSlim gate = new(1, 1);
    private IEnvironment? environment; private IConnection? connection;
    private bool disposed;

    public RabbitConnectionManager(IOptions<RabbitMessagingOptions> options, ILogger<RabbitConnectionManager> logger)
    {
        this.options = options.Value;
        this.logger = logger;
    }

    public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        await this.gate.WaitAsync(cancellationToken);

        try
        {
            ObjectDisposedException.ThrowIf(this.disposed, this);

            if (this.connection is not null)
            {
                return connection;
            }

            if (this.environment is null)
            {
                var settings = ConnectionSettingsBuilder.Create()
                    .Host(this.options.Host).Port(this.options.Port)
                    .User(this.options.Username)
                    .Password(this.options.Password)
                    .VirtualHost(this.options.VirtualHost)
                    .Build();

                this.environment = AmqpEnvironment.Create(settings);
            }

            this.connection = await this.environment.CreateConnectionAsync(cancellationToken);
            return connection;
        }
        finally
        {
            this.gate.Release();
        }
    }
    public async ValueTask DisposeAsync()
    {
        await this.gate.WaitAsync();

        try
        {
            if (this.disposed)
            {
                return;
            }
            
            this.disposed = true;
            
            if (this.environment is not null)
            {
                await this.environment.CloseAsync(); this.environment = null; this.connection = null;
            }
        }
        finally 
        { 
            this.gate.Release(); 
        }
    }
}
