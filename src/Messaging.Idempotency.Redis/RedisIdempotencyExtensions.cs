using Messaging.Abstractions.Idempotency;
using Messaging.RabbitMQ.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Messaging.Idempotency.Redis;

public static class RedisIdempotencyExtensions
{
    public static IServiceCollection AddRedisIdempotency(this IServiceCollection services, IConfiguration redisConfiguration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(redisConfiguration);

        services.AddOptions<RedisIdempotencyOptions>()
            .Bind(redisConfiguration)
            .Validate(options => HasEndpoint(options.ConnectionString), "Idempotency.Redis.ConnectionString must contain a valid Redis endpoint.")
            .Validate(options => !string.IsNullOrWhiteSpace(options.KeyPrefix), "Idempotency.Redis.KeyPrefix is required.")
            .ValidateOnStart();

        services.TryAddKeyedSingleton<IConnectionMultiplexer>("Redis", (provider, _) =>
        {
            var options = provider.GetRequiredService<IOptions<RedisIdempotencyOptions>>().Value;
            var configuration = ConfigurationOptions.Parse(options.ConnectionString);
            configuration.BacklogPolicy = BacklogPolicy.FailFast;
            configuration.AbortOnConnectFail = false;
            return ConnectionMultiplexer.Connect(configuration);
        });

        services.TryAddKeyedSingleton<IMessageDeduplicationStore>("Redis", (provider, _) =>
        {
            var rabbit = provider.GetRequiredService<IOptions<RabbitMessagingOptions>>().Value;
            var redis = provider.GetRequiredService<IOptions<RedisIdempotencyOptions>>().Value;
            var connection = provider.GetRequiredKeyedService<IConnectionMultiplexer>("Redis");
            return new RedisMessageDeduplicationStore(connection.GetDatabase(), redis.KeyPrefix, rabbit.VirtualHost, rabbit.Idempotency.ProcessingTimeout, rabbit.Idempotency.RetentionPeriod);
        });

        return services;
    }

    private static bool HasEndpoint(string connectionString)
    {
        try
        {
            return !string.IsNullOrWhiteSpace(connectionString) && ConfigurationOptions.Parse(connectionString).EndPoints.Count > 0;
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException or OverflowException)
        {
            return false;
        }
    }
}

internal class RedisIdempotencyOptions
{
    public string ConnectionString { get; set; } = "";

    public string KeyPrefix { get; set; } = "messaging:idempotency:";
}
