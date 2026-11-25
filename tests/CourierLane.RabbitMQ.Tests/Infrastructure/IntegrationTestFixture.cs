using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;
using Testcontainers.Redis;
using Xunit;

namespace CourierLane.RabbitMQ.Tests.Infrastructure;

public class IntegrationTestFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer rabbitMq;
    private readonly RedisContainer redis;
    private readonly PostgreSqlContainer postgreSql;

    public string RabbitMqUsername { get; } = "courierlane_test";

    public string RabbitMqPassword { get; } = Guid.NewGuid().ToString("N");

    public string RabbitMqVirtualHost { get; } = "/";

    public string RabbitMqHost => this.rabbitMq.Hostname;

    public int RabbitMqPort => this.rabbitMq.GetMappedPublicPort(5672);

    public string RedisConnectionString => this.redis.GetConnectionString();

    public string PostgreSqlConnectionString => this.postgreSql.GetConnectionString();

    public IntegrationTestFixture()
    {
        this.rabbitMq = new RabbitMqBuilder("rabbitmq:4-management")
            .WithUsername(this.RabbitMqUsername)
            .WithPassword(this.RabbitMqPassword)
            .WithLabel("courierlane.purpose", "integration-tests")
            .WithCleanUp(true)
            .Build();

        this.redis = new RedisBuilder("redis:8.8.0")
            .WithLabel("courierlane.purpose", "integration-tests")
            .WithCleanUp(true)
            .Build();

        this.postgreSql = new PostgreSqlBuilder("postgres:15")
            .WithDatabase("courierlane_test")
            .WithUsername("courierlane_test")
            .WithPassword(Guid.NewGuid().ToString("N"))
            .WithLabel("courierlane.purpose", "integration-tests")
            .WithCleanUp(true)
            .Build();
    }

    public async Task InitializeAsync()
    {
        await Task.WhenAll(
            this.rabbitMq.StartAsync(),
            this.redis.StartAsync(),
            this.postgreSql.StartAsync());
    }

    public async Task DisposeAsync()
    {
        await Task.WhenAll(
            this.rabbitMq.DisposeAsync().AsTask(),
            this.redis.DisposeAsync().AsTask(),
            this.postgreSql.DisposeAsync().AsTask());
    }
}