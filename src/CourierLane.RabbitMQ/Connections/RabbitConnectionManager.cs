using CourierLane.RabbitMQ.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
namespace CourierLane.RabbitMQ.Connections;

public class RabbitConnectionManager : IAsyncDisposable
{
    private readonly CourierLaneOptions options;

    private readonly ILogger<RabbitConnectionManager> logger;

    private readonly SemaphoreSlim gate = new(1, 1);

    private IEnvironment? environment;

    private IConnection? connection;

    private bool disposed;

    public bool IsConnected => Volatile.Read(ref this.connection)?.State == State.Open;
    public RabbitConnectionManager(IOptions<CourierLaneOptions> options, ILogger<RabbitConnectionManager> logger)
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
                return this.connection;
            }

            if (this.environment is null)
            {
                var builder = ConnectionSettingsBuilder.Create();
                if (this.options.UseTls)
                {
                    builder.Uri(new UriBuilder("amqps", this.options.Host, this.options.Port).Uri);
                }

                var settings = builder
                    .Host(this.options.Host)
                    .Port(this.options.Port)
                    .User(this.options.Username)
                    .Password(this.options.Password)
                    .VirtualHost(this.options.VirtualHost)
                    .RecoveryConfiguration(new RecoveryConfiguration().Activated(true).Topology(this.options.Consumer.AutoDeclare))
                    .Build();

                this.environment = AmqpEnvironment.Create(settings);
            }

            this.connection = await this.environment.CreateConnectionAsync(cancellationToken);

            return this.connection;
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
                await this.environment.CloseAsync();

                this.environment = null;

                this.connection = null;
            }
        }
        finally
        {
            this.gate.Release();
        }
    }
}
