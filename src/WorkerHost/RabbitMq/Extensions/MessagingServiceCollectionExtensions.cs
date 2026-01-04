using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using WorkerHost.Messaging;
using WorkerHost.RabbitMq.Configuration;
using WorkerHost.RabbitMq.Messaging;

namespace WorkerHost.RabbitMq.Extensions;

internal static class MessagingServiceCollectionExtensions
{
    /// <summary>
    /// Registers broker configuration and publisher dependencies.
    /// </summary>
    public static IServiceCollection AddBrokerMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        services
            .AddOptions<BrokerOptions>()
            .Bind(configuration.GetSection(nameof(BrokerOptions)))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddSingleton<IBrokerChannelAccess, BrokerChannelAccess>();
        services.AddSingleton<IMessagePublisher, MessagePublisher>();
        services.AddSingleton<IMessageListener, MessageListener>();

        return services;
    }
}
