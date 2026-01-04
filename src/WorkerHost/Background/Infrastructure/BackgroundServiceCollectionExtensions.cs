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
        // Register the shared delivery queue so both ingest and worker services can exchange messages.
        services.AddSingleton<DeliveryQueue>(sp =>
        {
            // Resolve queue-related settings from configuration.
            var options = sp.GetRequiredService<IOptions<BrokerOptions>>().Value;
            // Fetch a logger to emit warnings when the capacity is clamped.
            var logger = sp.GetRequiredService<ILogger<DeliveryQueue>>();
            // Determine how many worker tasks we intend to run.
            var workerCount = WorkerBufferPlanner.CalculateWorkerCount(options);
            // Clamp the channel capacity to a safe range based on worker count and configuration.
            var capacity = WorkerBufferPlanner.CalculateChannelCapacity(options, workerCount, logger);
            // Create the actual channel-backed queue with the calculated capacity.
            return new DeliveryQueue(capacity);
        });

        // Register the per-message processor as a singleton; it is stateless between deliveries.
        services.AddSingleton<DeliveryProcessor>();
        // Hosted service #1: keeps the broker subscription alive and writes deliveries into the queue.
        services.AddHostedService<DeliveryIngestService>();
        // Hosted service #2: drains the queue, invokes the processor, and handles ack/nack work.
        // Both services communicate exclusively via the DeliveryQueue singleton above; no other shared state exists.
        services.AddHostedService<QueueWorker>();
        // Return the service collection for fluent registration chains.
        return services;
    }
}
