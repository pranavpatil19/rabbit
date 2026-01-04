using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WorkerHost.Common.Background;
using WorkerHost.RabbitMq.Configuration;

namespace WorkerHost.RabbitMq.Monitoring;

/// <summary>
/// Periodically logs publisher metrics for troubleshooting.
/// </summary>
internal sealed class PublisherMetricsReporter : PollingBackgroundService
{
    private readonly PublisherMetrics _metrics;
    private readonly ILogger<PublisherMetricsReporter> _logger;

    public PublisherMetricsReporter(
        PublisherMetrics metrics,
        IOptions<BrokerOptions> config,
        ILogger<PublisherMetricsReporter> logger)
        : base(logger, TimeSpan.FromSeconds(Math.Clamp(config.Value.ListenerMetricsLogIntervalSeconds, 5, 3600)))
    {
        _metrics = metrics;
        _logger = logger;
    }

    protected override Task<bool> ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        var snapshot = _metrics.CreateSnapshot();
        _logger.LogInformation(
            "Publisher metrics: published={Published}, avgPayloadBytes={AvgPayload}, lastPayloadBytes={LastPayload}, avgLatencyMs={AvgLatency}, lastLatencyMs={LastLatency}",
            snapshot.PublishedCount,
            snapshot.AveragePayloadBytes,
            snapshot.LastPayloadBytes,
            snapshot.AverageLatencyMs,
            snapshot.LastLatencyMs);

        return Task.FromResult(false); // request the base class to wait for the configured idle interval.
    }
}
