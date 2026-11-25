using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using CourierLane.Abstractions.Producers;
using CourierLane.RabbitMQ.Configuration;
using CourierLane.RabbitMQ.Connections;
using CourierLane.RabbitMQ.Diagnostics;
using CourierLane.RabbitMQ.Rpc;
using Microsoft.Extensions.Logging;
using RabbitMQ.AMQP.Client;
namespace CourierLane.RabbitMQ.Producers;

public class RabbitMessageProducer : IMessageProducer, IEventPublisher, IAsyncDisposable
{
    private readonly RabbitConnectionManager connections;

    private readonly MessageTypeRegistry messageTypes;

    private readonly RabbitRpcClient rpcClient;

    private readonly ILogger<RabbitMessageProducer> logger;

    private readonly ConcurrentDictionary<(string Destination, string? RoutingKey), Lazy<Task<IPublisher>>> publishers = new();

    private readonly JsonSerializerOptions jsonOptions = new(JsonSerializerDefaults.Web);

    private bool disposed;
    public RabbitMessageProducer(RabbitConnectionManager connections, MessageTypeRegistry messageTypes, RabbitRpcClient rpcClient, ILogger<RabbitMessageProducer> logger)
    {
        this.connections = connections;
        this.messageTypes = messageTypes;
        this.rpcClient = rpcClient;
        this.logger = logger;
    }
    public Task SendAsync<TCommand>(string destination, TCommand command, CancellationToken cancellationToken = default)
    {
        return this.PublishMessageAsync(destination, command, null, false, cancellationToken);
    }

    public Task SendAsync<TCommand>(string destination, TCommand command, SendOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        return this.PublishMessageAsync(destination, command, options, false, cancellationToken);
    }

    public Task PublishAsync<TEvent>(string exchange, TEvent message, CancellationToken cancellationToken = default)
    {
        return this.PublishMessageAsync(exchange, message, null, true, cancellationToken);
    }

    public Task PublishAsync<TEvent>(string exchange, TEvent message, SendOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        return this.PublishMessageAsync(exchange, message, options, true, cancellationToken);
    }

    private async Task PublishMessageAsync<TMessage>(string destination, TMessage messageBody, SendOptions? options, bool isEvent, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        ArgumentException.ThrowIfNullOrWhiteSpace(destination);

        ArgumentNullException.ThrowIfNull(messageBody);

        var typeId = this.messageTypes.GetMessageTypeId<TMessage>();

        var payload = JsonSerializer.SerializeToUtf8Bytes(messageBody, this.jsonOptions);

        await this.PublishSerializedAsync(destination, typeId, payload, options, isEvent, cancellationToken);
    }

    internal async Task PublishSerializedAsync(string destination, string typeId, byte[] payload, SendOptions? options, bool isEvent, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        var message = RabbitMessageMetadata.Create(payload, typeId, options).Durable(true);
        using var activity = CourierLaneDiagnostics.Activities.StartActivity("publish", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.destination.name", destination);
        CourierLaneDiagnostics.InjectTrace(message);
        var started = Stopwatch.GetTimestamp();
        var outcome = "failed";
        try
        {
            var publisher = await this.GetPublisherAsync((destination, isEvent ? typeId : null)).WaitAsync(cancellationToken);
            var result = await publisher.PublishAsync(message, cancellationToken);
            if (result.Outcome.State != OutcomeState.Accepted)
            {
                throw new InvalidOperationException($"RabbitMQ did not accept '{typeId}' for '{destination}': {result.Outcome.State}; {result.Outcome.Error}");
            }

            outcome = "accepted";
            this.logger.LogInformation("Published {MessageType} to {Destination}", typeId, destination);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            outcome = "cancelled";
            throw;
        }
        finally
        {
            var tag = new KeyValuePair<string, object?>("outcome", outcome);
            CourierLaneDiagnostics.Published.Add(1, tag);
            CourierLaneDiagnostics.PublishDuration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds, tag);
            if (outcome == "failed")
            {
                activity?.SetStatus(ActivityStatusCode.Error);
            }
        }
    }
    public Task<TResponse> RequestAsync<TRequest, TResponse>(string destination, TRequest request, RequestOptions? options = null, CancellationToken cancellationToken = default)
    {
        return this.rpcClient.RequestAsync<TRequest, TResponse>(destination, request, options, cancellationToken);
    }
    private async Task<IPublisher> GetPublisherAsync((string Destination, string? RoutingKey) address)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        var lazy = this.publishers.GetOrAdd(address, key => new Lazy<Task<IPublisher>>(() => this.CreatePublisherAsync(key.Destination, key.RoutingKey)));

        try
        {
            return await lazy.Value;
        }
        catch
        {
            this.publishers.TryRemove(KeyValuePair.Create(address, lazy));

            throw;
        }
    }
    private async Task<IPublisher> CreatePublisherAsync(string destination, string? routingKey)
    {
        var connection = await this.connections.GetConnectionAsync();

        var builder = connection.PublisherBuilder();
        return await (routingKey is null ? builder.Queue(destination) : builder.Exchange(destination).Key(routingKey)).BuildAsync();
    }
    public async ValueTask DisposeAsync()
    {
        if (this.disposed)
        {
            return;
        }

        this.disposed = true;

        foreach (var lazy in this.publishers.Values)
        {
            if (!lazy.IsValueCreated)
            {
                continue;
            }

            try
            {
                var publisher = await lazy.Value;

                await publisher.CloseAsync();

                publisher.Dispose();
            }
            catch (Exception ex)
            {
                this.logger.LogWarning(ex, "Error closing publisher");
            }
        }

        this.publishers.Clear();
    }
}
