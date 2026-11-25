using System.Collections.Concurrent;
using CourierLane.Abstractions.Idempotency;
using CourierLane.RabbitMQ.Configuration;
using CourierLane.RabbitMQ.Options;
using Microsoft.Extensions.Options;

namespace CourierLane.RabbitMQ.Idempotency;

public class InMemoryMessageDeduplicationStore : IMessageDeduplicationStore
{
    private readonly ConcurrentDictionary<string, MessageEntry> messages = new();
    private readonly IdempotencyOptions options;
    private readonly object cleanupLock = new();
    private IEnumerator<KeyValuePair<string, MessageEntry>>? cleanupCursor;
    private int acquisitions;

    public InMemoryMessageDeduplicationStore(IOptions<CourierLaneOptions> options)
{
    this.options = options.Value.Idempotency;
}

    public Task<MessageAcquireResponse> TryAcquireAsync(string key, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (Interlocked.Increment(ref this.acquisitions) % 64 == 0)
        {
            this.RemoveExpiredEntries();
        }

        while (true)
        {
            var now = DateTimeOffset.UtcNow;
            var token = Guid.NewGuid().ToString("N");

            var newEntry = new MessageEntry(
                token,
                MessageProcessingState.Processing,
                now.Add(this.options.ProcessingTimeout));

            if (this.messages.TryAdd(key, newEntry))
            {
                return Task.FromResult(new MessageAcquireResponse(
                    MessageAcquireResult.Acquired,
                    new MessageLease(key, token)));
            }

            if (!this.messages.TryGetValue(key, out var existing))
            {
                continue;
            }

            if (existing.ExpiresAt <= now)
            {
                if (this.messages.TryUpdate(key, newEntry, existing))
                {
                    return Task.FromResult(new MessageAcquireResponse(
                        MessageAcquireResult.Acquired,
                        new MessageLease(key, token)));
                }

                continue;
            }

            if (existing.State == MessageProcessingState.Completed)
            {
                return Task.FromResult(new MessageAcquireResponse(MessageAcquireResult.AlreadyCompleted));
            }

            return Task.FromResult(new MessageAcquireResponse(MessageAcquireResult.InProgress));
        }
    }

    public Task<bool> RenewAsync(MessageLease lease, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        while (this.messages.TryGetValue(lease.Key, out var existing))
        {
            var now = DateTimeOffset.UtcNow;

            if (existing.Token != lease.Token ||
                existing.State != MessageProcessingState.Processing ||
                existing.ExpiresAt <= now)
            {
                return Task.FromResult(false);
            }

            var updated = existing with
            {
                ExpiresAt = now.Add(this.options.ProcessingTimeout)
            };

            if (this.messages.TryUpdate(lease.Key, updated, existing))
            {
                return Task.FromResult(true);
            }
        }

        return Task.FromResult(false);
    }

    public Task<bool> MarkCompletedAsync(MessageLease lease, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        while (this.messages.TryGetValue(lease.Key, out var existing))
        {
            var now = DateTimeOffset.UtcNow;

            if (existing.Token != lease.Token ||
                existing.State != MessageProcessingState.Processing ||
                existing.ExpiresAt <= now)
            {
                return Task.FromResult(false);
            }

            var updated = existing with
            {
                State = MessageProcessingState.Completed,
                ExpiresAt = now.Add(this.options.RetentionPeriod)
            };

            if (this.messages.TryUpdate(lease.Key, updated, existing))
            {
                return Task.FromResult(true);
            }
        }

        return Task.FromResult(false);
    }

    public Task ReleaseAsync(MessageLease lease, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        while (this.messages.TryGetValue(lease.Key, out var existing))
        {
            if (existing.Token != lease.Token ||
                existing.State != MessageProcessingState.Processing)
            {
                break;
            }

            if (this.messages.TryRemove(new KeyValuePair<string, MessageEntry>(lease.Key, existing)))
            {
                break;
            }
        }

        return Task.CompletedTask;
    }

    private enum MessageProcessingState
    {
        Processing,
        Completed
    }

    private void RemoveExpiredEntries()
    {
        lock (this.cleanupLock)
        {
            var now = DateTimeOffset.UtcNow;
            this.cleanupCursor ??= this.messages.GetEnumerator();

            for (var scanned = 0; scanned < 64; scanned++)
            {
                if (!this.cleanupCursor.MoveNext())
                {
                    this.cleanupCursor.Dispose();
                    this.cleanupCursor = null;
                    break;
                }

                var entry = this.cleanupCursor.Current;
                if (entry.Value.ExpiresAt <= now)
                {
                    this.messages.TryRemove(entry);
                }
            }
        }
    }

    private record MessageEntry(string Token, MessageProcessingState State, DateTimeOffset ExpiresAt);
}
