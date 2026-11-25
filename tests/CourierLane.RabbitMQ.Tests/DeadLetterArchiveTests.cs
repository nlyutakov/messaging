using Messaging.Management;
using Microsoft.Extensions.Options;
using Xunit;

namespace Messaging.RabbitMQ.Tests;

public class DeadLetterArchiveTests
{
    [Fact]
    public async Task ReopenIsIdempotentAndOnlyOneReplayClaimCanWin()
    {
        var folder = Directory.CreateTempSubdirectory("messaging-dlq-");
        try
        {
            var options = Microsoft.Extensions.Options.Options.Create(new DeadLetterOptions { DatabasePath = Path.Combine(folder.FullName, "archive.db") });
            var first = new DeadLetterArchive(options);
            await first.InitializeAsync(default);
            var details = new DeadLetterDetails(new DeadLetterSummary(0, DateTimeOffset.UtcNow, "orders.dlq", "orders", "id", "orders.v1", "handler-failed", 0, DeadLetterStatus.Pending),
                [1, 2, 3], "application/json", "correlation", new Dictionary<string, string> { ["reason"] = "rejected" });
            await first.AddAsync(details, "delivery-1", default);

            var reopened = new DeadLetterArchive(options);
            await reopened.InitializeAsync(default);
            await reopened.AddAsync(details, "delivery-1", default);
            var row = Assert.Single(await reopened.ListAsync(0, 20, DeadLetterStatus.Pending, default));
            Assert.Equal(new byte[] { 1, 2, 3 }, (await reopened.InspectAsync(row.Id, default))?.Payload);

            var claims = await Task.WhenAll(
                Task.Run(() => first.TransitionAsync(row.Id, DeadLetterStatus.Pending, DeadLetterStatus.Replaying, "first", default)),
                Task.Run(() => reopened.TransitionAsync(row.Id, DeadLetterStatus.Pending, DeadLetterStatus.Replaying, "second", default)));
            Assert.Single(claims, claimed => claimed);
            Assert.Equal(1, (await reopened.CountAsync(default))[DeadLetterStatus.Replaying]);
            Assert.True(await reopened.TransitionAsync(row.Id, DeadLetterStatus.Replaying, DeadLetterStatus.Replayed, "operator", default));
            Assert.Empty(await reopened.ListAsync(row.Id, 20, null, default));

            await reopened.AddAsync(details, "delivery-2", default);
            var pending = Assert.Single(await reopened.ListAsync(row.Id, 20, DeadLetterStatus.Pending, default));
            Assert.True(await reopened.TransitionAsync(pending.Id, DeadLetterStatus.Pending, DeadLetterStatus.Archived, "operator", default));
            Assert.True(await reopened.TransitionAsync(pending.Id, DeadLetterStatus.Archived, DeadLetterStatus.Discarded, "operator", default));
            var discarded = Assert.IsType<DeadLetterDetails>(await reopened.InspectAsync(pending.Id, default));
            Assert.Null(discarded.Payload);
            Assert.Null(discarded.CorrelationId);
            Assert.Empty(discarded.Headers);
        }
        finally
        {
            folder.Delete(recursive: true);
        }
    }
}
