using Microsoft.Extensions.Configuration;
using System.Globalization;
using CourierLane.RabbitMQ.Tests.Infrastructure;

namespace CourierLane.RabbitMQ.Tests;

internal static class RabbitIntegrationSettings
{
    internal const string HostVariable = "COURIERLANE_TEST_RABBITMQ";

    internal static void AddRabbitSettings(this IConfiguration configuration, string sectionName)
    {
        var section = configuration.GetSection(sectionName);
        section["Host"] = Environment.GetEnvironmentVariable(HostVariable) ?? "localhost";
        section["Port"] = Environment.GetEnvironmentVariable("COURIERLANE_TEST_RABBITMQ_PORT") ?? "5672";
        section["Username"] = Environment.GetEnvironmentVariable("COURIERLANE_TEST_RABBITMQ_USER") ?? "guest";
        section["Password"] = Environment.GetEnvironmentVariable("COURIERLANE_TEST_RABBITMQ_PASSWORD") ?? "guest";
        section["VirtualHost"] = Environment.GetEnvironmentVariable("COURIERLANE_TEST_RABBITMQ_VHOST") ?? "/";
    }

    internal static void AddRabbitSettings(this IConfiguration configuration, string sectionName, IntegrationTestFixture fixture)
    {
        var section = configuration.GetSection(sectionName);

        section["Host"] = fixture.RabbitMqHost;
        section["Port"] = fixture.RabbitMqPort.ToString(CultureInfo.InvariantCulture);
        section["Username"] = fixture.RabbitMqUsername;
        section["Password"] = fixture.RabbitMqPassword;
        section["VirtualHost"] = fixture.RabbitMqVirtualHost;
    }
}