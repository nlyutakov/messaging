using Microsoft.Extensions.Configuration;

namespace Messaging.RabbitMQ.Tests;

internal static class RabbitIntegrationSettings
{
    internal const string HostVariable = "MESSAGING_TEST_RABBITMQ";

    internal static void AddRabbitSettings(this IConfiguration configuration, string sectionName)
    {
        var section = configuration.GetSection(sectionName);
        section["Host"] = Environment.GetEnvironmentVariable(HostVariable) ?? "localhost";
        section["Port"] = Environment.GetEnvironmentVariable("MESSAGING_TEST_RABBITMQ_PORT") ?? "5672";
        section["Username"] = Environment.GetEnvironmentVariable("MESSAGING_TEST_RABBITMQ_USER") ?? "guest";
        section["Password"] = Environment.GetEnvironmentVariable("MESSAGING_TEST_RABBITMQ_PASSWORD") ?? "guest";
    }
}
