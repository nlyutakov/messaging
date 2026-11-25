using Messaging.Abstractions.Idempotency;
using Microsoft.Extensions.Logging;
namespace Messaging.RabbitMQ.Idempotency;

internal class MessageLeaseManager
{
    private readonly IMessageDeduplicationStore store;

    private readonly ILogger logger;

    private readonly TimeSpan renewalInterval;

    internal MessageLeaseManager(IMessageDeduplicationStore store, ILogger logger, TimeSpan processingTimeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(processingTimeout, TimeSpan.FromMilliseconds(3));
        this.store = store;
        this.logger = logger;
        this.renewalInterval = TimeSpan.FromTicks(Math.Min(processingTimeout.Ticks / 3, TimeSpan.FromMinutes(1).Ticks));
    }

    internal Task<MessageAcquireResponse> AcquireAsync(string key, CancellationToken cancellationToken)
    {
        return InvokeStoreAsync(ct => this.store.TryAcquireAsync(key, ct), "acquire", cancellationToken);
    }

    internal async Task ProcessAsync(MessageLease lease, Func<CancellationToken, Task> dispatch, CancellationToken cancellationToken)
    {
        using var processing = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var stopRenewal = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var renewal = this.RenewUntilStoppedAsync(lease, processing, stopRenewal.Token, cancellationToken);

        try
        {
            try
            {
                await dispatch(processing.Token);
            }
            finally
            {
                await stopRenewal.CancelAsync();
                await renewal;
                cancellationToken.ThrowIfCancellationRequested();
            }

            if (!await InvokeStoreAsync(ct => this.store.MarkCompletedAsync(lease, ct), "complete", cancellationToken))
            {
                throw new MessageLeaseException("Ownership was lost before lease completion.");
            }
        }
        catch
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            try
            {
                await this.store.ReleaseAsync(lease, cleanup.Token);
            }
            catch (Exception ex)
            {
                this.logger.LogWarning(ex, "Could not release message lease");
            }

            throw;
        }
    }

    private async Task RenewUntilStoppedAsync(MessageLease lease, CancellationTokenSource processing, CancellationToken stopRenewal, CancellationToken shutdown)
    {
        try
        {
            while (true)
            {
                try
                {
                    await Task.Delay(this.renewalInterval, stopRenewal);
                }
                catch (OperationCanceledException) when (stopRenewal.IsCancellationRequested)
                {
                    return;
                }

                if (stopRenewal.IsCancellationRequested)
                {
                    return;
                }

                // Finish any renewal already in flight before attempting completion.
                using var deadline = CancellationTokenSource.CreateLinkedTokenSource(shutdown);
                deadline.CancelAfter(this.renewalInterval);

                if (!await InvokeStoreAsync(ct => this.store.RenewAsync(lease, ct), "renew", deadline.Token))
                {
                    throw new MessageLeaseException("Ownership was lost during lease renewal.");
                }

                deadline.Token.ThrowIfCancellationRequested();
            }
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            try
            {
                await processing.CancelAsync();
            }
            catch (Exception cancellationError)
            {
                this.logger.LogWarning(cancellationError, "Message handler cancellation callback failed");
            }

            throw ex as MessageLeaseException ?? new MessageLeaseException("Lease renewal failed.", ex);
        }
    }

    private static async Task<T> InvokeStoreAsync<T>(Func<CancellationToken, Task<T>> operation, string name, CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await operation(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new MessageLeaseException($"Could not {name} message lease.", ex);
        }
    }
}

internal class MessageLeaseException(string message, Exception? innerException = null) : Exception(message, innerException);
