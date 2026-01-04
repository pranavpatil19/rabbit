using Microsoft.Extensions.Logging;

using WorkerHost.RabbitMq.Configuration;

namespace WorkerHost.Background;

internal static class WorkerBufferPlanner
{
    public static int CalculateWorkerCount(BrokerOptions config) =>
        Math.Clamp(config.DefaultConcurrency, 1, config.MaxConcurrency);

    public static int CalculateChannelCapacity(BrokerOptions config, int workerCount, ILogger logger)
    {
        var maxCapacity = Math.Max(workerCount, config.MaxConcurrency * 4);
        var capacity = Math.Clamp(config.WorkerChannelCapacity, workerCount, maxCapacity);
        if (capacity != config.WorkerChannelCapacity)
        {
            logger.LogWarning(
                "WorkerChannelCapacity {ConfiguredCapacity} is outside [{Min}, {Max}]; clamped to {Clamped}.",
                config.WorkerChannelCapacity,
                workerCount,
                maxCapacity,
                capacity);
        }

        return capacity;
    }
}
