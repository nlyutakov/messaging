using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Messaging.Outbox.PostgreSql;

public static class PostgreSqlOutboxExtensions
{
    internal const string ConnectionKey = "Messaging.Outbox.PostgreSql";

    public static IServiceCollection AddPostgreSqlOutbox(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<PostgreSqlOutboxOptions>().Bind(configuration)
            .Validate(options => !string.IsNullOrWhiteSpace(options.ConnectionString), "Outbox.ConnectionString is required.")
            .Validate(options => !string.IsNullOrWhiteSpace(options.Scope), "Outbox.Scope is required.")
            .Validate(options => options.PublishTimeout >= TimeSpan.FromMilliseconds(1) && options.PublishTimeout <= TimeSpan.FromMinutes(5) &&
                options.ClaimTimeout >= options.PublishTimeout + TimeSpan.FromSeconds(5) && options.ClaimTimeout <= TimeSpan.FromMinutes(10),
                "Outbox claim timeout must exceed the publish timeout by at least 5 seconds.")
            .Validate(options => options.RetentionPeriod > TimeSpan.Zero && options.RetentionPeriod <= TimeSpan.FromDays(36500), "Outbox retention period is out of range.")
            .ValidateOnStart();
        services.TryAddKeyedSingleton<NpgsqlDataSource>(ConnectionKey, (provider, _) =>
            NpgsqlDataSource.Create(provider.GetRequiredService<IOptions<PostgreSqlOutboxOptions>>().Value.ConnectionString));
        services.TryAddSingleton<PostgreSqlOutbox>();
        services.TryAddSingleton<OutboxStorage>();
        services.AddHostedService<OutboxPublisherService>();
        return services;
    }
}

public class PostgreSqlOutboxOptions
{
    public string ConnectionString { get; set; } = "";
    public string Scope { get; set; } = "";
    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(15);
    public TimeSpan ClaimTimeout { get; set; } = TimeSpan.FromMinutes(1);
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(7);
}
