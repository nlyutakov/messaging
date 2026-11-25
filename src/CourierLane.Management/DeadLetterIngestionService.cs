using System.Security.Cryptography;
using System.Text.Json;
using Messaging.RabbitMQ.Connections;
using Messaging.RabbitMQ.Producers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.AMQP.Client;

namespace Messaging.Management;

internal class DeadLetterIngestionService(DeadLetterArchive archive, RabbitConnectionManager connections, IOptions<DeadLetterOptions> options, ILogger<DeadLetterIngestionService> logger) : BackgroundService
{
    internal const string ReplayCountProperty = "messaging-replay-count";

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await archive.InitializeAsync(cancellationToken);
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (options.Value.Sources.Count == 0)
        {
            return;
        }

        var connection = await connections.GetConnectionAsync(stoppingToken);
        var consumers = new List<IConsumer>();
        try
        {
            foreach (var source in options.Value.Sources.Keys)
            {
                var consumer = await connection.ConsumerBuilder().Queue(source).InitialCredits(1)
                    .MessageHandler((context, message) => this.IngestAsync(source, context, message, stoppingToken))
                    .BuildAndStartAsync(stoppingToken);
                consumers.Add(consumer);
            }

            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            foreach (var consumer in consumers)
            {
                using (consumer)
                {
                    await consumer.CloseAsync();
                }
            }
        }
    }

    private async Task IngestAsync(string deadLetterQueue, IContext context, IMessage message, CancellationToken stoppingToken)
    {
        try
        {
            var messageId = message.MessageId() as string;
            var type = message.Property("message-type") as string;
            var target = ReadAnnotation(message, "x-last-death-queue") ?? ReadAnnotation(message, "x-first-death-queue");
            var failure = ReadAnnotation(message, "x-opt-messaging-failure") ?? ReadAnnotation(message, "x-last-death-reason") ?? ReadAnnotation(message, "x-first-death-reason");
            var count = message.Property(ReplayCountProperty) is int replayCount ? Math.Max(0, replayCount) : 0;
            var payload = message.Body();
            var headers = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var name in new[] { "x-first-death-exchange", "x-first-death-reason", "x-last-death-queue", "x-last-death-reason" })
            {
                if (ReadAnnotation(message, name) is { } value)
                {
                    headers.Add(name, value);
                }
            }

            foreach (var name in new[] { "messaging-correlation-id", "messaging-causation-id", "messaging-source", "messaging-timestamp" })
            {
                if (message.Property(name) is string value)
                {
                    headers.Add(name, value);
                }
            }

            if (message.Property("messaging-header-names") is string catalog)
            {
                try
                {
                    foreach (var name in JsonSerializer.Deserialize<string[]>(catalog) ?? [])
                    {
                        if (!string.IsNullOrWhiteSpace(name) && !RabbitMessageMetadata.IsReservedHeader(name) && message.Property(name) is string value)
                        {
                            headers.TryAdd(name, value);
                        }
                    }
                }
                catch (JsonException)
                {
                }
            }

            var fingerprint = messageId is null ? null : JsonSerializer.Serialize(new object?[]
            {
                deadLetterQueue, target, messageId, type, count, payload is null ? null : Convert.ToHexString(SHA256.HashData(payload))
            });
            var summary = new DeadLetterSummary(0, DateTimeOffset.UtcNow, deadLetterQueue, target, messageId, type, failure, count, DeadLetterStatus.Pending);
            await archive.AddAsync(new DeadLetterDetails(summary, payload, message.ContentType(), message.CorrelationId() as string, headers), fingerprint, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            context.Requeue();
            return;
        }
        catch (Exception ex)
        {
            logger.LogError("DLQ archive write failed on {Queue} with {ErrorType}", deadLetterQueue, ex.GetType().Name);
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }

            context.Requeue();
            return;
        }

        context.Accept();
    }

    private static string? ReadAnnotation(IMessage message, string name) => message.Annotation(name)?.ToString();
}
