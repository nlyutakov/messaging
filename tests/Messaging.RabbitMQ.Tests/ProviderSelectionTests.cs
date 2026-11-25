using Messaging.Abstractions.Idempotency;
using Messaging.RabbitMQ.Configuration;
using Messaging.RabbitMQ.DependencyInjection;
using Messaging.RabbitMQ.Idempotency;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace Messaging.RabbitMQ.Tests;

public class ProviderSelectionTests
{
    [Fact]
    public void InMemoryIsTheDefaultProvider()
    {
        using var services = CreateServices().BuildServiceProvider();

        var store = services.GetRequiredService<IMessageDeduplicationStore>();
        Assert.IsType<InMemoryMessageDeduplicationStore>(store);
        Assert.Same(services.GetRequiredKeyedService<IMessageDeduplicationStore>("InMemory"), store);
    }

    [Fact]
    public void ProviderIsResolvedUsingItsExplicitRegistration()
    {
        var registrations = CreateServices("Other");
        registrations.AddKeyedSingleton<IMessageDeduplicationStore, InMemoryMessageDeduplicationStore>("Other");
        using var services = registrations.BuildServiceProvider();

        var store = services.GetRequiredService<IMessageDeduplicationStore>();
        Assert.Same(services.GetRequiredKeyedService<IMessageDeduplicationStore>("Other"), store);
        Assert.NotSame(services.GetRequiredKeyedService<IMessageDeduplicationStore>("InMemory"), store);
    }

    [Theory]
    [InlineData("")]
    [InlineData("Redis")]
    public void MissingOrMismatchedProviderFailsAtStartup(string name)
    {
        var registrations = CreateServices(name);
        registrations.AddKeyedSingleton<IMessageDeduplicationStore, InMemoryMessageDeduplicationStore>("Other");
        using var services = registrations.BuildServiceProvider();

        var error = Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IStartupValidator>().Validate());
        Assert.Contains("Idempotency.Provider", error.Message);
    }

    [Theory]
    [InlineData("Idempotency:ProcessingTimeout", "00:00:00.001")]
    [InlineData("Idempotency:RetentionPeriod", "00:00:00")]
    [InlineData("Consumer:Retry:InitialDelay", "-00:00:01")]
    [InlineData("Consumer:Retry:BackoffMultiplier", "NaN")]
    [InlineData("Consumer:Credit", "0")]
    public void InvalidOptionsFailAtStartup(string key, string value)
    {
        var registrations = new ServiceCollection();
        registrations.AddRabbitMessaging(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { [key] = value }).Build());
        using var services = registrations.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IStartupValidator>().Validate());
    }

    [Fact]
    public void InvalidQueueOverrideFailsDuringRegistration()
    {
        var error = Assert.Throws<InvalidOperationException>(() =>
            new ServiceCollection().AddRabbitListener(builder => builder.Listen("orders", options => options.Credit = 0)));
        Assert.Contains("must be positive", error.Message);
    }

    private static IServiceCollection CreateServices(string? name = null)
    {
        var settings = new Dictionary<string, string?>();
        if (name is not null)
        {
            settings["Idempotency:Provider"] = name;
        }

        return new ServiceCollection().AddRabbitMessaging(new ConfigurationBuilder().AddInMemoryCollection(settings).Build());
    }
}
