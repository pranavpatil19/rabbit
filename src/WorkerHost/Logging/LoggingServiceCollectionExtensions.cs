using Microsoft.Extensions.DependencyInjection;
namespace WorkerHost.Logging;

internal static class LoggingServiceCollectionExtensions
{
    /// <summary>
    /// Registers the ordered log queue pipeline plus its draining background service.
    /// </summary>
    public static IServiceCollection AddLoggingPipeline(this IServiceCollection services)
    {
        services.AddSingleton<ILogQueue, ChannelLogQueue>();
        services.AddHostedService<LogDrainService>();
        return services;
    }
}
