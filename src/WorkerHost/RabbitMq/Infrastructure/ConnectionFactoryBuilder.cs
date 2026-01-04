using RabbitMQ.Client;
using WorkerHost.RabbitMq.Configuration;

namespace WorkerHost.RabbitMq.Infrastructure;

internal static class ConnectionFactoryBuilder
{
#region Factory creation
    public static ConnectionFactory Create(BrokerOptions config, string? overrideClientProvidedName = null)
    {
        ArgumentNullException.ThrowIfNull(config);

        var connection = config.Connection;

        var factory = new ConnectionFactory
        {
            AutomaticRecoveryEnabled = connection.AutomaticRecoveryEnabled,
            TopologyRecoveryEnabled = connection.BindingsRecoveryEnabled,
        };

        if (!string.IsNullOrWhiteSpace(connection.ConnectionString))
        {
            factory.Uri = new Uri(connection.ConnectionString);
        }
        else
        {
            factory.HostName = ValueOrDefault(connection.Host, BrokerDefaults.Host);
            factory.Port = connection.Port == 0 ? BrokerDefaults.Port : connection.Port;
            factory.UserName = ValueOrDefault(connection.UserName, BrokerDefaults.UserName);
            factory.Password = ValueOrDefault(connection.Password, BrokerDefaults.Password);
            factory.VirtualHost = "/";
        }

        factory.ClientProvidedName = ValueOrDefault(
            string.IsNullOrWhiteSpace(overrideClientProvidedName) ? connection.ClientProvidedName : overrideClientProvidedName,
            BrokerDefaults.WorkerClientProvidedName);

        return factory;
    }
#endregion

    private static string ValueOrDefault(string? value, string fallback)
        => string.IsNullOrWhiteSpace(value) ? fallback : value;
}
