using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace WorkerHost.RabbitMq.Messaging;

/// <summary>
/// Centralized helper to close and dispose broker resources with consistent logging.
/// Use this wherever channels/connections are torn down to avoid duplicated try/catch code.
/// </summary>
internal static class BrokerResourceCleaner
{
    /// <summary>
    /// Attempts to close and dispose a channel; logs a warning if closure fails.
    /// </summary>
    public static async ValueTask CloseAndDisposeChannelAsync(IChannel channel, ILogger logger, string context)
    {
        try
        {
            if (channel.IsOpen)
            {
                await channel.CloseAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "{Context}: Closing channel failed", context);
        }

        await channel.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Attempts to close and dispose a connection; logs a warning if closure fails.
    /// </summary>
    public static async ValueTask CloseAndDisposeConnectionAsync(IConnection connection, ILogger logger, string context)
    {
        try
        {
            if (connection.IsOpen)
            {
                await connection.CloseAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "{Context}: Closing connection failed", context);
        }

        await connection.DisposeAsync().ConfigureAwait(false);
    }
}
