using System.Collections.Concurrent;
using System.Text.Json;
using Messaging.Abstractions.Exceptions;
using Messaging.Abstractions.Producers;
using Messaging.Abstractions.Rpc;
using Messaging.RabbitMQ.Configuration;
using Messaging.RabbitMQ.Connections;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
namespace Messaging.RabbitMQ.Rpc;

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

        var requester = await GetRequesterAsync(destination, cancellationToken);

        var payload = JsonSerializer.SerializeToUtf8Bytes(request, this.jsonOptions);

        var message = new AmqpMessage(payload)
            .Property("message-type", messageTypeId)
            .ContentType("application/json")
            .MessageId(Guid.NewGuid().ToString("N"));

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
