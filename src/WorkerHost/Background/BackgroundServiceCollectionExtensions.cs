using Microsoft.Extensions.DependencyInjection;
namespace WorkerHost.Background;

internal static class BackgroundServiceCollectionExtensions
{
    /// <summary>
    /// Registers background services responsible for queue processing.
    /// </summary>
    public static IServiceCollection AddQueueProcessingWorker(this IServiceCollection services)
    {
        services.AddHostedService<QueueWorker>();
        return services;
    }
}
