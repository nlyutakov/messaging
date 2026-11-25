using System.Net.Sockets;
using Xunit;
using StackExchange.Redis;
using Npgsql;

namespace CourierLane.RabbitMQ.Tests.Infrastructure;

[Collection(IntegrationTestCollection.Name)]
[Trait("Category", "Integration")]
public class InfrastructureSmokeTests
{
    private readonly IntegrationTestFixture fixture;

    public InfrastructureSmokeTests(IntegrationTestFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task RabbitMqIsReachable()
    {
        using var client = new TcpClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await client.ConnectAsync(this.fixture.RabbitMqHost, this.fixture.RabbitMqPort, timeout.Token);

        Assert.True(client.Connected);
    }

    [Fact]
    public async Task RedisCanStoreAndRetrieveValue()
    {
        using var connection = await ConnectionMultiplexer.ConnectAsync(this.fixture.RedisConnectionString);

        var database = connection.GetDatabase();
        var key = $"courierlane:tests:smoke:{Guid.NewGuid():N}";
        const string value = "CourierLane";

        try
        {
            Assert.True(await database.StringSetAsync(key, value));

            var result = await database.StringGetAsync(key);

            Assert.Equal(value, result.ToString());
        }
        finally
        {
            await database.KeyDeleteAsync(key);
        }
    }

    [Fact]
    public async Task PostgreSqlCanStoreAndRetrieveValue()
    {
        await using var connection = new NpgsqlConnection(this.fixture.PostgreSqlConnectionString);

        await connection.OpenAsync();

        await using var createCommand = new NpgsqlCommand(
            """
        CREATE TEMP TABLE courierlane_smoke_test
        (
            id integer PRIMARY KEY,
            value text NOT NULL
        );
        """,
            connection);

        await createCommand.ExecuteNonQueryAsync();

        await using var insertCommand = new NpgsqlCommand(
            """
        INSERT INTO courierlane_smoke_test (id, value)
        VALUES (1, @value);
        """,
            connection);

        insertCommand.Parameters.AddWithValue("value", "CourierLane");

        Assert.Equal(1, await insertCommand.ExecuteNonQueryAsync());

        await using var selectCommand = new NpgsqlCommand(
            """
        SELECT value
        FROM courierlane_smoke_test
        WHERE id = 1;
        """,
            connection);

        var result = await selectCommand.ExecuteScalarAsync();

        Assert.Equal("CourierLane", Assert.IsType<string>(result));
    }
}