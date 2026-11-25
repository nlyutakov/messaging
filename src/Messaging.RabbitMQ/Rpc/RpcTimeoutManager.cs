using Messaging.Abstractions.Producers;
using Messaging.RabbitMQ.Configuration;
using Microsoft.Extensions.Options;
namespace Messaging.RabbitMQ.Rpc;

public class RpcTimeoutManager
{
    private readonly RpcOptions options;
    public RpcTimeoutManager(IOptions<RabbitMessagingOptions> options)
    {
        this.options = options.Value.Rpc;
    }
    public TimeSpan ResolveTimeout(RequestOptions? options)
    {
        var timeout = options?.Timeout ?? this.options.DefaultTimeout;

        if (timeout <= TimeSpan.Zero || timeout == Timeout.InfiniteTimeSpan)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "RPC timeout must be finite and positive.");
        }

        return timeout;
    }
}
