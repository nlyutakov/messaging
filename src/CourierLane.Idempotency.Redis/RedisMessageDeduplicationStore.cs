using Messaging.Abstractions.Idempotency;
using StackExchange.Redis;

namespace Messaging.Idempotency.Redis;

internal class RedisMessageDeduplicationStore(IDatabase database, string keyPrefix, string virtualHost, TimeSpan processingTimeout, TimeSpan retentionPeriod) : IMessageDeduplicationStore
{
    private const string AcquireScript = """
        local value = redis.call('GET', KEYS[1])
        if not value then
            redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
            return 1
        end
        if value == 'completed' then
            return 2
        end
        return 3
        """;

    private const string RenewScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('PEXPIRE', KEYS[1], ARGV[2])
        end
        return 0
        """;

    private const string CompleteScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            redis.call('SET', KEYS[1], 'completed', 'PX', ARGV[2])
            return 1
        end
        return 0
        """;

    private const string ReleaseScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
        """;

    private readonly IDatabase database = database;
    private readonly string scopedPrefix = $"{keyPrefix}{virtualHost.Length}:{virtualHost}:";
    private readonly long processingMilliseconds = (long)Math.Ceiling(processingTimeout.TotalMilliseconds);
    private readonly long retentionMilliseconds = (long)Math.Ceiling(retentionPeriod.TotalMilliseconds);

    public async Task<MessageAcquireResponse> TryAcquireAsync(string key, CancellationToken cancellationToken = default)
    {
        var token = Guid.NewGuid().ToString("N");
        var result = await this.EvaluateAsync(AcquireScript, key, [token, this.processingMilliseconds], cancellationToken);
        return result switch
        {
            1 => new(MessageAcquireResult.Acquired, new MessageLease(key, token)),
            2 => new(MessageAcquireResult.AlreadyCompleted),
            3 => new(MessageAcquireResult.InProgress),
            _ => throw new InvalidOperationException("Unexpected Redis lease acquire result.")
        };
    }

    public async Task<bool> RenewAsync(MessageLease lease, CancellationToken cancellationToken = default)
    {
        return await this.EvaluateAsync(RenewScript, lease.Key, [lease.Token, this.processingMilliseconds], cancellationToken) == 1;
    }

    public async Task<bool> MarkCompletedAsync(MessageLease lease, CancellationToken cancellationToken = default)
    {
        return await this.EvaluateAsync(CompleteScript, lease.Key, [lease.Token, this.retentionMilliseconds], cancellationToken) == 1;
    }

    public async Task ReleaseAsync(MessageLease lease, CancellationToken cancellationToken = default)
    {
        await this.EvaluateAsync(ReleaseScript, lease.Key, [lease.Token], cancellationToken);
    }

    private async Task<int> EvaluateAsync(string script, string key, RedisValue[] arguments, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var operation = this.database.ScriptEvaluateAsync(script, [this.scopedPrefix + key], arguments);
        return (int)await operation.WaitAsync(cancellationToken);
    }
}
