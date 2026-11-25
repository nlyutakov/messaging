using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Encodings.Web;
using CourierLane.Management;
using CourierLane.RabbitMQ.DependencyInjection;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;

namespace CourierLane.RabbitMQ.Tests;

public class DeadLetterApiTests
{
    [Fact]
    public async Task PayloadAndMutationsRequireTheirOwnPermission()
    {
        var folder = Directory.CreateTempSubdirectory("courierlane-dlq-api-");
        try
        {
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            builder.Configuration["Management:DatabasePath"] = Path.Combine(folder.FullName, "archive.db");
            builder.Services.AddAuthentication("test").AddScheme<AuthenticationSchemeOptions, TestAuthHandler>("test", _ => { });
            builder.Services.AddCourierLane(builder.Configuration.GetSection("CourierLane"));
            builder.Services.AddDeadLetterManagement(builder.Configuration.GetSection("Management"), "test");

            await using var app = builder.Build();
            app.MapDeadLetterManagement();
            await app.StartAsync();
            using var client = new HttpClient { BaseAddress = new Uri(Assert.Single(app.Urls)) };
            var archive = app.Services.GetRequiredService<DeadLetterArchive>();
            var summary = new DeadLetterSummary(0, DateTimeOffset.UtcNow, "orders.dlq", "orders", "id", "orders.v1", "rejected", 0, DeadLetterStatus.Pending);
            await archive.AddAsync(new DeadLetterDetails(summary, [1, 2, 3], "binary", null, new Dictionary<string, string>()), "delivery", default);

            using var anonymous = await client.GetAsync("/api/dead-letters");
            Assert.Equal(HttpStatusCode.Unauthorized, anonymous.StatusCode);
            client.DefaultRequestHeaders.Add("X-Test-Permissions", "courierlane.dlq.read");
            using var listing = await client.GetAsync("/api/dead-letters");
            listing.EnsureSuccessStatusCode();
            var item = Assert.Single((await listing.Content.ReadFromJsonAsync<List<DeadLetterSummary>>())!);
            Assert.True(listing.Headers.CacheControl?.NoStore);
            var counts = await client.GetFromJsonAsync<Dictionary<string, long>>("/api/dead-letters/counts");
            Assert.Equal(1L, counts?["Pending"]);
            using var privateDetails = await client.GetAsync($"/api/dead-letters/{item.Id}");
            using var protectedMutation = await client.PostAsync($"/api/dead-letters/{item.Id}/discard", null);
            Assert.Equal(HttpStatusCode.Forbidden, privateDetails.StatusCode);
            Assert.Equal(HttpStatusCode.Forbidden, protectedMutation.StatusCode);

            client.DefaultRequestHeaders.Remove("X-Test-Permissions");
            client.DefaultRequestHeaders.Add("X-Test-Permissions", "courierlane.dlq.read,courierlane.dlq.payload,courierlane.dlq.discard");
            var details = await client.GetFromJsonAsync<DeadLetterDetails>($"/api/dead-letters/{item.Id}");
            Assert.Equal(new byte[] { 1, 2, 3 }, details?.Payload);
            using var badBatch = await client.PostAsJsonAsync("/api/dead-letters/batch/discard", new[] { item.Id, item.Id });
            Assert.Equal(HttpStatusCode.BadRequest, badBatch.StatusCode);
            using var discarded = await client.PostAsync($"/api/dead-letters/{item.Id}/discard", null);
            discarded.EnsureSuccessStatusCode();
            Assert.Null((await client.GetFromJsonAsync<DeadLetterDetails>($"/api/dead-letters/{item.Id}"))?.Payload);
            await app.StopAsync();
        }
        finally
        {
            folder.Delete(recursive: true);
        }
    }

    private class TestAuthHandler(IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory loggerFactory, UrlEncoder encoder)
        : AuthenticationHandler<AuthenticationSchemeOptions>(options, loggerFactory, encoder)
    {
        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            if (!this.Request.Headers.TryGetValue("X-Test-Permissions", out var permissions))
            {
                return Task.FromResult(AuthenticateResult.NoResult());
            }

            var claims = permissions.ToString().Split(',').Select(permission => new Claim("courierlane.dlq", permission)).ToList();
            claims.Add(new Claim(ClaimTypes.NameIdentifier, "test-operator"));
            var identity = new ClaimsIdentity(claims, this.Scheme.Name);
            return Task.FromResult(AuthenticateResult.Success(new AuthenticationTicket(new ClaimsPrincipal(identity), this.Scheme.Name)));
        }
    }
}
