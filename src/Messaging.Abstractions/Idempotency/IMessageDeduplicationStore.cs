namespace Messaging.Abstractions.Idempotency;

public interface IMessageDeduplicationStore
{
    Task<MessageAcquireResponse> TryAcquireAsync(string key, CancellationToken cancellationToken = default);

    Task<bool> RenewAsync(MessageLease lease, CancellationToken cancellationToken = default);

    Task<bool> MarkCompletedAsync(MessageLease lease, CancellationToken cancellationToken = default);

    Task ReleaseAsync(MessageLease lease, CancellationToken cancellationToken = default);
}