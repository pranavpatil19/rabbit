using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WorkerHost.Common.Background;
using WorkerHost.RabbitMq.Configuration;

namespace WorkerHost.RabbitMq.Monitoring;

/// <summary>
/// Periodically logs listener metrics for troubleshooting.
/// </summary>
internal sealed class MessageListenerMetricsReporter : PollingBackgroundService
{
    private readonly MessageListenerMetrics _metrics;
    private readonly ILogger<MessageListenerMetricsReporter> _logger;

    public MessageListenerMetricsReporter(
        MessageListenerMetrics metrics,
        IOptions<BrokerOptions> config,
        ILogger<MessageListenerMetricsReporter> logger)
        : base(logger, TimeSpan.FromSeconds(Math.Clamp(config.Value.ListenerMetricsLogIntervalSeconds, 5, 3600)))
    {
        _metrics = metrics;
        _logger = logger;
    }

    protected override Task<bool> ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        var snapshot = _metrics.CreateSnapshot();
        _logger.LogInformation(
            "Message listener metrics: dequeued={Dequeued}, idle={Idle}, failures={Failures}, reconnects={Reconnects}, connectionOpen={ConnectionOpen}, buffered={Buffered}, prefetch={Prefetch}, avgDeliveryLatencyMs={AvgLatency}, lastDeliveryLatencyMs={LastLatency}, lastMessageUtc={LastMessage}, lastFailureUtc={LastFailure}",
            snapshot.MessagesDequeued,
            snapshot.IdlePolls,
            snapshot.Failures,
            snapshot.Reconnects,
            snapshot.ConnectionOpen,
            snapshot.BufferedDeliveries,
            snapshot.Prefetch,
            snapshot.AverageDeliveryLatencyMs,
            snapshot.LastDeliveryLatencyMs,
            snapshot.LastMessageUtc?.ToString("O") ?? "n/a",
            snapshot.LastFailureUtc?.ToString("O") ?? "n/a");

        return Task.FromResult(false); // request the base class to wait for the configured idle interval.
    }
}
