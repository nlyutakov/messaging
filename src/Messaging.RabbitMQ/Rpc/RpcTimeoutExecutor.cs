using Messaging.Abstractions.Exceptions;
namespace Messaging.RabbitMQ.Rpc;

public static class RpcTimeoutExecutor
{
    public static async Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> operation, string destination, string requestType, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        using var timeoutCts = new CancellationTokenSource(timeout);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        try
        {
            return await operation(linkedCts.Token)
                .WaitAsync(linkedCts.Token);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
        {
            throw new RpcTimeoutException(destination, requestType, timeout);
        }
    }
}
