using System.Diagnostics;
using Messaging.Abstractions.Idempotency;
using Messaging.RabbitMQ.Configuration;
using Messaging.RabbitMQ.Connections;
using Messaging.RabbitMQ.Dispatching;
using Messaging.RabbitMQ.Diagnostics;
using Messaging.RabbitMQ.Idempotency;
using Messaging.RabbitMQ.Retry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.AMQP.Client;
namespace Messaging.RabbitMQ.Consumers;

public class RabbitQueueConsumer
{
    private readonly RabbitConnectionManager connections;

    private readonly IMessageDispatcher dispatcher;

    private readonly RabbitMessagingOptions options;

    private readonly ILogger<RabbitQueueConsumer> logger;

    private readonly ILoggerFactory loggerFactory;

    private readonly IRetryExceptionClassifier retryExceptionClassifier;

    private readonly MessageLeaseManager? leaseManager;

    public RabbitQueueConsumer(RabbitConnectionManager connections, IMessageDispatcher dispatcher, IOptions<RabbitMessagingOptions> options, ILogger<RabbitQueueConsumer> logger, ILoggerFactory loggerFactory, IRetryExceptionClassifier retryExceptionClassifier, IMessageDeduplicationStore deduplicationStore)
    {
        this.connections = connections;
        this.dispatcher = dispatcher;
        this.options = options.Value;
        this.logger = logger;
        this.loggerFactory = loggerFactory;
        this.retryExceptionClassifier = retryExceptionClassifier;
        if (this.options.Consumer.EnableIdempotency)
        {
            this.leaseManager = new MessageLeaseManager(deduplicationStore, logger, this.options.Idempotency.ProcessingTimeout);
        }
    }
    public async Task<IConsumer> StartAsync(QueueRegistration queue, CancellationToken cancellationToken = default)
    {
        var connection = await this.connections.GetConnectionAsync(cancellationToken);

        var credit = queue.Options.Credit ?? this.options.Consumer.Credit;
        var concurrency = new SemaphoreSlim(queue.Options.MaxConcurrency ?? this.options.Consumer.MaxConcurrency);
        var queueType = queue.EventBinding?.Broadcast == true ? RabbitQueueType.Classic : queue.Options.QueueType ?? this.options.Consumer.QueueType;

        var consumer = await connection.ConsumerBuilder()
            .Queue(queue.Name)
            .InitialCredits(credit)
            .MessageHandler(async (context, message) =>
            {
                try
                {
                    await concurrency.WaitAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    context.Requeue();
                    return;
                }

                try
                {
                    await this.ProcessMessageAsync(queue.Name, context, message, cancellationToken, queueType == RabbitQueueType.Quorum && this.options.Consumer.DeadLetter.Enabled);
                }
                finally
                {
                    concurrency.Release();
                }
            })
            .BuildAndStartAsync(cancellationToken);

        this.logger.LogInformation("Consumer started for {Queue}", queue.Name);

        return consumer;
    }
    internal async Task ProcessMessageAsync(string queueName, IContext context, IMessage message, CancellationToken cancellationToken, bool includeFailureReason = false)
    {
        using var activity = MessagingDiagnostics.StartProcessing(message, "process", queueName);
        var started = Stopwatch.GetTimestamp();
        var outcome = "discarded";
        try
        {
            var messageTypeId = message.Property("message-type") as string;

            if (string.IsNullOrWhiteSpace(messageTypeId))
            {
                this.logger.LogWarning("Message without message-type on {Queue}", queueName);

                Discard(context, includeFailureReason, "missing-message-type");

                return;
            }

            var body = message.Body();

            if (body is null)
            {
                this.logger.LogWarning("Message without body on {Queue}", queueName);

                Discard(context, includeFailureReason, "missing-body");

                return;
            }

            var messageId = message.MessageId()?.ToString();

            await this.ProcessWithIdempotencyAsync(queueName, messageTypeId, body, messageId, cancellationToken);

            cancellationToken.ThrowIfCancellationRequested();
            context.Accept();
            outcome = "accepted";
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            context.Requeue();
            outcome = "requeued";
        }
        catch (MessageLeaseException ex)
        {
            MessagingDiagnostics.LeaseFailures.Add(1);
            this.logger.LogWarning(ex, "Message lease failed on {Queue}; requeuing", queueName);

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }

            context.Requeue();
            outcome = "requeued";
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Message processing failed on {Queue}; discarding to DLQ if configured", queueName);

            Discard(context, includeFailureReason, ex switch
            {
                UnknownMessageTypeException => "unknown-message-type",
                System.Text.Json.JsonException => "invalid-json",
                _ => "handler-failed"
            });
        }
        finally
        {
            var tag = new KeyValuePair<string, object?>("outcome", outcome);
            MessagingDiagnostics.Processed.Add(1, tag);
            MessagingDiagnostics.ProcessDuration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds, tag);
            if (outcome == "discarded")
            {
                activity?.SetStatus(ActivityStatusCode.Error);
            }
        }
    }

    private static void Discard(IContext context, bool includeFailureReason, string reason)
    {
        if (includeFailureReason)
        {
            context.Discard(new Dictionary<string, object> { ["x-opt-messaging-failure"] = reason });
        }
        else
        {
            context.Discard();
        }
    }

    private async Task ProcessWithIdempotencyAsync(string queueName, string messageTypeId, ReadOnlyMemory<byte> body, string? messageId, CancellationToken cancellationToken)
    {
        if (this.leaseManager is null || string.IsNullOrWhiteSpace(messageId))
        {
            await this.DispatchWithRetryAsync(queueName, messageTypeId, body, cancellationToken);
            return;
        }

        var key = $"{queueName}:{messageId}";

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var response = await this.leaseManager.AcquireAsync(key, cancellationToken);

            if (response.Result == MessageAcquireResult.AlreadyCompleted)
            {
                MessagingDiagnostics.Duplicates.Add(1);
                this.logger.LogInformation("Skipping duplicate message {MessageId} on {Queue}", messageId, queueName);
                return;
            }

            if (response.Result == MessageAcquireResult.InProgress)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
                continue;
            }

            if (response.Result != MessageAcquireResult.Acquired || response.Lease is null)
            {
                throw new MessageLeaseException("Deduplication store returned an invalid acquire response.");
            }

            await this.leaseManager.ProcessAsync(response.Lease, ct => this.DispatchWithRetryAsync(queueName, messageTypeId, body, ct), cancellationToken);
            return;
        }
    }
    private async Task DispatchWithRetryAsync(string queueName, string messageTypeId, ReadOnlyMemory<byte> body, CancellationToken cancellationToken)
    {
        var retryExecutor = new RetryExecutor(this.options.Consumer.Retry, this.retryExceptionClassifier, this.loggerFactory.CreateLogger<RetryExecutor>());

        await retryExecutor.ExecuteAsync(async ct =>
        {
            await this.dispatcher.DispatchAsync(queueName, messageTypeId, body, ct);
        }, cancellationToken);
    }
}
