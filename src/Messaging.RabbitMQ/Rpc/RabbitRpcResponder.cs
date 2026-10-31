using System.Text.Json;
using Messaging.Abstractions.Rpc;
using Messaging.RabbitMQ.Connections;
using Messaging.RabbitMQ.Dispatching;
using Microsoft.Extensions.Logging;
using RabbitMQ.AMQP.Client;
namespace Messaging.RabbitMQ.Rpc;

public class RabbitRpcResponder
{
    private readonly RabbitConnectionManager connections;

    private readonly IMessageDispatcher dispatcher;

    private readonly ILogger<RabbitRpcResponder> logger;

    private readonly JsonSerializerOptions jsonOptions = new(JsonSerializerDefaults.Web);
    public RabbitRpcResponder(RabbitConnectionManager connections, IMessageDispatcher dispatcher, ILogger<RabbitRpcResponder> logger)
    {
        this.connections = connections;
        this.dispatcher = dispatcher;
        this.logger = logger;
    }
    public async Task<IResponder> StartAsync(string queueName, CancellationToken cancellationToken = default)
    {
        var connection = await this.connections.GetConnectionAsync(cancellationToken);

        var responder = await connection.ResponderBuilder()
            .RequestQueue(queueName)
            .Handler(async (context, message) =>
            {
                RpcResponseEnvelope envelope;

                try
                {
                    var typeId = message.Property("message-type") as string;

                    if (string.IsNullOrWhiteSpace(typeId))
                    {
                        throw new InvalidOperationException("RPC request has no message-type.");
                    }

                    var body = message.Body() ?? throw new JsonException("RPC request has no body.");

                    var result = await this.dispatcher.DispatchAsync(queueName, typeId, body, cancellationToken);

                    if (result is null)
                    {
                        throw new InvalidOperationException("RPC handler returned null.");
                    }

                    envelope = new RpcResponseEnvelope
                    {
                        Success = true,
                        Payload = JsonSerializer.SerializeToUtf8Bytes(result, result.GetType(), this.jsonOptions)
                    };
                }
                catch (Exception ex)
                {
                    this.logger.LogError(ex, "RPC request processing failed on {Queue}", queueName);

                    envelope = new RpcResponseEnvelope
                    {
                        Success = false,
                        ErrorCode = "HANDLER_ERROR",
                        ErrorMessage = "RPC request processing failed."
                    };
                }

                return context.Message(JsonSerializer.SerializeToUtf8Bytes(envelope, this.jsonOptions));
            })
            .BuildAsync();

        this.logger.LogInformation("RPC responder started for {Queue}", queueName);

        return responder;
    }
}
