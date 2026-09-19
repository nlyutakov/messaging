using Messaging.Abstractions;
using Messaging.Core.Naming;
using Messaging.RabbitMQ.Configuration;
using Messaging.RabbitMQ.Connection;
using Microsoft.Extensions.DependencyInjection;
using Messaging.RabbitMQ.Consumers;

namespace Messaging.RabbitMQ.DependencyInjection;

public static class RabbitMqServiceCollectionExtensions
{
    public static IServiceCollection AddRabbitMqMessaging(this IServiceCollection services, Action<RabbitMqOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);

        services.AddSingleton<IMessageNameFormatter, DefaultMessageNameFormatter>();
        services.AddSingleton<IRabbitMqConnection, RabbitMqConnection>();
        services.AddSingleton<IMessageSender, RabbitMqMessageSender>();

        services.AddSingleton<RabbitMqConsumerRegistry>();
        services.AddHostedService<RabbitMqConsumerHostedService>();

        return services;
    }

    public static IServiceCollection AddRabbitMqConsumer<TMessage, TConsumer>(
    this IServiceCollection services,
    string queueName)
    where TMessage : class
    where TConsumer : class, IConsumer<TMessage>
{
    ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

    services.AddScoped<TConsumer>();

    services.AddSingleton(
        new RabbitMqConsumerDefinition(
            typeof(TMessage),
            typeof(TConsumer),
            queueName));

    return services;
}
}