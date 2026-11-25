using System.Reflection;
using CourierLane.Abstractions.Idempotency;
using CourierLane.RabbitMQ.Configuration;
using CourierLane.RabbitMQ.Consumers;
using CourierLane.RabbitMQ.Dispatching;
using CourierLane.RabbitMQ.Idempotency;
using CourierLane.RabbitMQ.Retry;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RabbitMQ.AMQP.Client;
using Xunit;

namespace CourierLane.RabbitMQ.Tests;

public class MessageLeaseTests
{
    [Fact]
    public async Task ConcurrentDuplicateWaitsWhileHandlerRunsBeyondOriginalLease()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var options = CreateOptions();
        var store = new Store(options);
        var finish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var consumer = CreateConsumer(options, store, async ct =>
        {
            Interlocked.Increment(ref calls);
            entered.TrySetResult();
            await finish.Task.WaitAsync(ct);
        });

        var first = Start(consumer, deadline.Token);
        await entered.Task.WaitAsync(deadline.Token);
        var duplicate = Start(consumer, deadline.Token);
        await store.DuplicateObserved.Task.WaitAsync(deadline.Token);
        await store.FourRenewals.Task.WaitAsync(deadline.Token);
        Assert.False(duplicate.Processing.IsCompleted);
        finish.SetResult();

        await Task.WhenAll(first.Processing, duplicate.Processing);
        Assert.Equal(1, calls);
        Assert.Equal("Accept", first.Context.Settlement);
        Assert.Equal("Accept", duplicate.Context.Settlement);
    }

    [Fact]
    public async Task RenewalContinuesDuringRetryBackoff()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var options = CreateOptions();
        options.Value.Consumer.Retry.MaxAttempts = 2;
        options.Value.Consumer.Retry.InitialDelay = TimeSpan.FromMilliseconds(1600);
        var store = new Store(options);
        var calls = 0;
        var consumer = CreateConsumer(options, store, _ =>
        {
            if (++calls == 1)
            {
                throw new InvalidOperationException("Transient handler failure");
            }

            Assert.True(store.FourRenewals.Task.IsCompletedSuccessfully);
            return Task.CompletedTask;
        });

        var delivery = Start(consumer, deadline.Token);
        await delivery.Processing;
        Assert.Equal(2, calls);
        Assert.Equal("Accept", delivery.Context.Settlement);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RenewalFailureCancelsHandlerAndRequeues(bool storeThrows)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var options = CreateOptions();
        var store = new Store(options)
        {
            Renew = (_, _) => storeThrows ? throw new TimeoutException() : Task.FromResult(false)
        };
        var cancelled = false;
        var consumer = CreateConsumer(options, store, async ct =>
        {
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            finally
            {
                cancelled = ct.IsCancellationRequested;
            }
        });

        var delivery = Start(consumer, deadline.Token);
        await delivery.Processing;
        Assert.True(cancelled);
        Assert.Equal(0, store.Completions);
        Assert.Equal(1, store.Releases);
        Assert.Equal("Requeue", delivery.Context.Settlement);
        Assert.False(deadline.IsCancellationRequested);
    }

    [Fact]
    public async Task ShutdownRequeuesAndReleases()
    {
        using var shutdown = new CancellationTokenSource();
        var options = CreateOptions();
        var store = new Store(options);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = CreateConsumer(options, store, async ct =>
        {
            entered.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        });

        var delivery = Start(consumer, shutdown.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await shutdown.CancelAsync();
        await delivery.Processing.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal("Requeue", delivery.Context.Settlement);
        Assert.Equal(0, store.Completions);
        Assert.Equal(1, store.Releases);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompletionWaitsForInFlightRenewal(bool loseOwnership)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var options = CreateOptions();
        var inRenewal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finishRenewal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finishHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new Store(options)
        {
            Renew = async (_, ct) =>
            {
                inRenewal.TrySetResult();
                await finishRenewal.Task.WaitAsync(ct);
                return !loseOwnership;
            }
        };
        var consumer = CreateConsumer(options, store, _ => finishHandler.Task);
        var delivery = Start(consumer, deadline.Token);

        await inRenewal.Task.WaitAsync(deadline.Token);
        finishHandler.SetResult();
        Assert.Equal(0, store.Completions);
        Assert.Null(delivery.Context.Settlement);
        finishRenewal.SetResult();
        await delivery.Processing;

        Assert.Equal(loseOwnership ? "Requeue" : "Accept", delivery.Context.Settlement);
        Assert.Equal(loseOwnership ? 0 : 1, store.Completions);
    }

    [Fact]
    public async Task LostLeaseDuringCompletionRequeuesWithoutAcknowledging()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var options = CreateOptions();
        var store = new Store(options) { CanComplete = false };
        var consumer = CreateConsumer(options, store, _ => Task.CompletedTask);

        var delivery = Start(consumer, deadline.Token);
        await delivery.Processing;
        Assert.Equal(1, store.Completions);
        Assert.Equal(1, store.Releases);
        Assert.Equal("Requeue", delivery.Context.Settlement);
        Assert.False(deadline.IsCancellationRequested);
    }

    [Fact]
    public async Task ReleasedStaleOwnerCannotMutateTheReplacementLease()
    {
        var store = new InMemoryMessageDeduplicationStore(CreateOptions());
        var old = Assert.IsType<MessageLease>((await store.TryAcquireAsync("key")).Lease);
        await store.ReleaseAsync(old);
        var current = Assert.IsType<MessageLease>((await store.TryAcquireAsync("key")).Lease);

        Assert.False(await store.RenewAsync(old));
        Assert.False(await store.MarkCompletedAsync(old));
        await store.ReleaseAsync(old);
        Assert.True(await store.MarkCompletedAsync(current));
    }

    private static IOptions<CourierLaneOptions> CreateOptions()
    {
        var options = new CourierLaneOptions();
        options.Consumer.EnableIdempotency = true;
        options.Idempotency.ProcessingTimeout = TimeSpan.FromMilliseconds(900);
        return Microsoft.Extensions.Options.Options.Create(options);
    }

    private static RabbitQueueConsumer CreateConsumer(IOptions<CourierLaneOptions> options, Store store, Func<CancellationToken, Task> dispatch)
    {
        return new RabbitQueueConsumer(null!, new Dispatcher(dispatch), options, NullLogger<RabbitQueueConsumer>.Instance, NullLoggerFactory.Instance, new DefaultRetryExceptionClassifier(), store);
    }

    private static (Task Processing, ContextProxy Context) Start(RabbitQueueConsumer consumer, CancellationToken cancellationToken)
    {
        var context = DispatchProxy.Create<IContext, ContextProxy>();
        var message = DispatchProxy.Create<IMessage, MessageProxy>();
        return (consumer.ProcessMessageAsync("commands", context, message, cancellationToken), (ContextProxy)(object)context);
    }

    public class ContextProxy : DispatchProxy
    {
        public string? Settlement { get; private set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            Assert.Null(this.Settlement);
            this.Settlement = targetMethod!.Name;
            return null;
        }
    }

    public class MessageProxy : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            return targetMethod!.Name switch
            {
                "Property" => "command.v1",
                "MessageId" => "id",
                "Body" => Array.Empty<byte>(),
                _ => throw new InvalidOperationException(targetMethod.Name)
            };
        }
    }

    private class Dispatcher(Func<CancellationToken, Task> dispatch) : IMessageDispatcher
    {
        public async Task<object?> DispatchAsync(string queueName, string messageTypeId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
        {
            await dispatch(cancellationToken);
            return null;
        }
    }

    private class Store(IOptions<CourierLaneOptions> options) : IMessageDeduplicationStore
    {
        private readonly InMemoryMessageDeduplicationStore inner = new(options);
        private int renewals;

        public Func<MessageLease, CancellationToken, Task<bool>>? Renew { get; init; }
        public bool CanComplete { get; init; } = true;
        public TaskCompletionSource DuplicateObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource FourRenewals { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Completions { get; private set; }
        public int Releases { get; private set; }

        public async Task<MessageAcquireResponse> TryAcquireAsync(string key, CancellationToken cancellationToken = default)
        {
            var response = await this.inner.TryAcquireAsync(key, cancellationToken);
            if (response.Result == MessageAcquireResult.InProgress)
            {
                this.DuplicateObserved.TrySetResult();
            }

            return response;
        }

        public async Task<bool> RenewAsync(MessageLease lease, CancellationToken cancellationToken = default)
        {
            var renewed = this.Renew is null
                ? await this.inner.RenewAsync(lease, cancellationToken)
                : await this.Renew(lease, cancellationToken);
            if (renewed && Interlocked.Increment(ref this.renewals) == 4)
            {
                this.FourRenewals.TrySetResult();
            }

            return renewed;
        }

        public Task<bool> MarkCompletedAsync(MessageLease lease, CancellationToken cancellationToken = default)
        {
            this.Completions++;
            return this.CanComplete ? this.inner.MarkCompletedAsync(lease, cancellationToken) : Task.FromResult(false);
        }

        public Task ReleaseAsync(MessageLease lease, CancellationToken cancellationToken = default)
        {
            this.Releases++;
            return this.inner.ReleaseAsync(lease, cancellationToken);
        }
    }
}
