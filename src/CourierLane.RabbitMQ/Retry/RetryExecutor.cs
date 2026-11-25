using CourierLane.RabbitMQ.Configuration;
using CourierLane.RabbitMQ.Diagnostics;
using Microsoft.Extensions.Logging;

namespace CourierLane.RabbitMQ.Retry;

public class RetryExecutor
{
    private readonly RetryOptions options;
    private readonly IRetryExceptionClassifier classifier;
    private readonly ILogger<RetryExecutor> logger;

    public RetryExecutor(RetryOptions options, IRetryExceptionClassifier classifier, ILogger<RetryExecutor> logger)
    {
        this.options = options;
        this.classifier = classifier;
        this.logger = logger;
    }

    public async Task ExecuteAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        if (this.options.MaxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(this.options.MaxAttempts));
        }

        for (var attempt = 1; attempt <= this.options.MaxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await operation(cancellationToken);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (!this.classifier.ShouldRetry(ex))
                {
                    this.logger.LogWarning(ex, "Non-retryable message processing error on attempt {Attempt}", attempt);
                    throw;
                }

                if (attempt == this.options.MaxAttempts)
                {
                    this.logger.LogError(ex, "Message processing failed after {Attempts} attempts", attempt);
                    throw;
                }

                var delay = this.CalculateDelay(attempt);
                CourierLaneDiagnostics.Retries.Add(1);

                this.logger.LogWarning(ex, "Message processing failed on attempt {Attempt}/{MaxAttempts}; retrying in {Delay}",
                    attempt, this.options.MaxAttempts, delay);

                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    private TimeSpan CalculateDelay(int failedAttempt)
    {
        var multiplier = Math.Pow(this.options.BackoffMultiplier, failedAttempt - 1);
        var milliseconds = Math.Min(this.options.InitialDelay.TotalMilliseconds * multiplier, this.options.MaxDelay.TotalMilliseconds);

        return TimeSpan.FromMilliseconds(milliseconds);
    }
}
