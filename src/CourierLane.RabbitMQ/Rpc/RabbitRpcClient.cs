using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using CourierLane.Abstractions.Exceptions;
using CourierLane.Abstractions.Producers;
using CourierLane.Abstractions.Rpc;
using CourierLane.RabbitMQ.Configuration;
using CourierLane.RabbitMQ.Connections;
using CourierLane.RabbitMQ.Diagnostics;
using CourierLane.RabbitMQ.Producers;
using RabbitMQ.AMQP.Client;
namespace CourierLane.RabbitMQ.Rpc;

public class RabbitRpcClient : IAsyncDisposable
{
    private readonly RabbitConnectionManager connections;

    private readonly MessageTypeRegistry messageTypes;

    private readonly RpcTimeoutManager timeouts;

    private readonly ConcurrentDictionary<string, Lazy<Task<IRequester>>> requesters = new();

    private readonly JsonSerializerOptions jsonOptions = new(JsonSerializerDefaults.Web);
    public RabbitRpcClient(RabbitConnectionManager connections, MessageTypeRegistry messageTypes, RpcTimeoutManager timeouts)
    {
        this.connections = connections;
        this.messageTypes = messageTypes;
        this.timeouts = timeouts;
    }
    public async Task<TResponse> RequestAsync<TRequest, TResponse>(string destination, TRequest request, RequestOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);

        ArgumentNullException.ThrowIfNull(request);

        var timeout = this.timeouts.ResolveTimeout(options);

        var messageTypeId = this.messageTypes.GetMessageTypeId<TRequest>();

        var payload = JsonSerializer.SerializeToUtf8Bytes(request, this.jsonOptions);

        var message = RabbitMessageMetadata.Create(payload, messageTypeId, options?.Metadata, useNativeCorrelation: false);
        using var activity = CourierLaneDiagnostics.Activities.StartActivity("request", ActivityKind.Client);
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.destination.name", destination);
        CourierLaneDiagnostics.InjectTrace(message);

        var requester = await this.GetRequesterAsync(destination, cancellationToken);

        var reply = await RpcTimeoutExecutor.ExecuteAsync(ct => requester.PublishAsync(message, ct), destination, messageTypeId, timeout, cancellationToken);

        var body = reply.Body() ?? throw new JsonException("RPC response has no body.");

        var envelope = JsonSerializer.Deserialize<RpcResponseEnvelope>(body, this.jsonOptions) ?? throw new JsonException("RPC response envelope is null.");

        if (!envelope.Success)
        { 
            throw new RpcRemoteException(envelope.ErrorCode ?? "UNKNOWN_ERROR", envelope.ErrorMessage ?? "Remote RPC request failed."); 
        }

        if (envelope.Payload is null) 
        { 
            throw new JsonException("Successful RPC response has no payload."); 
        }

        return JsonSerializer.Deserialize<TResponse>(envelope.Payload, this.jsonOptions) ?? throw new JsonException("RPC response payload deserialized to null.");
    }
    private async Task<IRequester> GetRequesterAsync(string destination, CancellationToken ct)
    {
        var lazy = this.requesters.GetOrAdd(destination, q => new Lazy<Task<IRequester>>(() => CreateRequesterAsync(q)));
        try
        {
            return await lazy.Value.WaitAsync(ct);
        }
        catch
        {
            if (lazy.IsValueCreated && lazy.Value.IsFaulted) 
            { 
                this.requesters.TryRemove(new KeyValuePair<string, Lazy<Task<IRequester>>>(destination, lazy));
            }

            throw;
        }
    }
    private async Task<IRequester> CreateRequesterAsync(string destination)
    {
        var connection = await this.connections.GetConnectionAsync();

        return await connection.RequesterBuilder()
            .RequestAddress()
            .Queue(destination)
            .Requester()
            .Timeout(TimeSpan.FromMinutes(10))
            .BuildAsync();
    }
    public async ValueTask DisposeAsync()
    {
        foreach (var lazy in this.requesters.Values)
        {
            if (!lazy.IsValueCreated)
            { 
                continue; 
            }
            try
            {
                var requester = await lazy.Value;
                await requester.CloseAsync();
                requester.Dispose();
            }
            catch { }
        }
        this.requesters.Clear();
    }
}
