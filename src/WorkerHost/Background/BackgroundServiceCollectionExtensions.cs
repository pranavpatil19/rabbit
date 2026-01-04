using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using WorkerHost.RabbitMq.Configuration;

namespace WorkerHost.Background;

internal static class BackgroundServiceCollectionExtensions
{
    /// <summary>
    /// Registers background services responsible for queue processing.
    /// </summary>
    public static IServiceCollection AddQueueProcessingWorker(this IServiceCollection services)
    {
        services.AddSingleton<DeliveryBuffer>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<BrokerOptions>>().Value;
            var logger = sp.GetRequiredService<ILogger<DeliveryBuffer>>();
            var workerCount = WorkerBufferPlanner.CalculateWorkerCount(options);
            var capacity = WorkerBufferPlanner.CalculateChannelCapacity(options, workerCount, logger);
            return new DeliveryBuffer(capacity);
        });

        services.AddSingleton<DeliveryProcessor>();
        services.AddHostedService<ListenerPump>();
        services.AddHostedService<QueueWorker>();
        return services;
    }
}
