using Messaging.Abstractions;
using Messaging.Core.Naming;
using Messaging.RabbitMQ.Configuration;
using Messaging.RabbitMQ.Connection;
using Microsoft.Extensions.DependencyInjection;

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

        return services;
    }
}