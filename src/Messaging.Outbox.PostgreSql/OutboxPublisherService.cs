using System.Diagnostics.Metrics;
using System.Text.Json;
using Messaging.Abstractions.Producers;
using Messaging.RabbitMQ.Producers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Messaging.Outbox.PostgreSql;

internal class OutboxPublisherService(OutboxStorage storage, RabbitMessageProducer producer, IOptions<PostgreSqlOutboxOptions> options, ILogger<OutboxPublisherService> logger) : BackgroundService, IHostedLifecycleService
{
    private readonly Meter meter = new("Messaging.Outbox.PostgreSql");
    private long backlog = -1;

    public Task StartingAsync(CancellationToken cancellationToken) => storage.InitializeAsync(cancellationToken);
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        this.meter.CreateObservableGauge("messaging.outbox.backlog", () =>
        {
            var value = Interlocked.Read(ref this.backlog);
            return value < 0 ? Array.Empty<Measurement<long>>() : new[] { new Measurement<long>(value) };
        });
        var nextCleanup = DateTimeOffset.UtcNow;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (DateTimeOffset.UtcNow >= nextCleanup)
                {
                    await storage.CleanupAsync(stoppingToken);
                    Interlocked.Exchange(ref this.backlog, await storage.BacklogAsync(stoppingToken));
                    nextCleanup = DateTimeOffset.UtcNow.AddMinutes(1);
                }

                var claim = await storage.ClaimAsync(stoppingToken);
                if (claim is null)
                {
                    await Task.Delay(250, stoppingToken);
                    continue;
                }

                await this.ProcessAsync(claim, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogWarning("Outbox storage operation failed with {ErrorType}", ex.GetType().Name);
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
            }
        }
    }

    public override void Dispose()
    {
        this.meter.Dispose();
        base.Dispose();
    }

    private async Task ProcessAsync(OutboxClaim claim, CancellationToken stoppingToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        deadline.CancelAfter(options.Value.PublishTimeout);
        try
        {
            var metadata = JsonSerializer.Deserialize<SendOptions>(claim.Metadata);
            if (string.IsNullOrWhiteSpace(metadata?.MessageId))
            {
                throw new InvalidOperationException("Outbox row has no stable MessageId.");
            }

            await producer.PublishSerializedAsync(claim.Destination, claim.Type, claim.Payload, metadata, claim.IsEvent, deadline.Token);
        }
        catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
        {
            logger.LogWarning("Outbox record {OutboxId} attempt {Attempt} failed with {ErrorType}", claim.Id, claim.Attempts, ex.GetType().Name);
            await storage.RetryAsync(claim, stoppingToken);
            return;
        }

        if (!await storage.CompleteAsync(claim, stoppingToken))
        {
            logger.LogWarning("Broker accepted outbox record {OutboxId} after the claim was replaced", claim.Id);
        }
    }
}
