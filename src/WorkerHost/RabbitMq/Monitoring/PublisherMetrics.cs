using System;
using System.Threading;

namespace WorkerHost.RabbitMq.Monitoring;

/// <summary>
/// Tracks basic publish metrics: counts, payload sizes, and latency.
/// </summary>
public sealed class PublisherMetrics
{
    private long _publishedCount;
    private long _totalPayloadBytes;
    private long _lastPayloadBytes;
    private long _totalLatencyTicks;
    private long _lastLatencyTicks;

    /// <summary>Record a publish with its latency and payload size.</summary>
    public void RecordPublish(TimeSpan latency, int payloadBytes)
    {
        Interlocked.Increment(ref _publishedCount);
        Interlocked.Add(ref _totalPayloadBytes, payloadBytes);
        Interlocked.Exchange(ref _lastPayloadBytes, payloadBytes);
        Interlocked.Add(ref _totalLatencyTicks, latency.Ticks);
        Interlocked.Exchange(ref _lastLatencyTicks, latency.Ticks);
    }

    /// <summary>Creates a snapshot of current publish metrics.</summary>
    public PublisherMetricsSnapshot CreateSnapshot()
    {
        var count = Interlocked.Read(ref _publishedCount);
        var avgPayload = count == 0 ? 0 : Interlocked.Read(ref _totalPayloadBytes) / count;
        var avgLatencyTicks = count == 0 ? 0 : Interlocked.Read(ref _totalLatencyTicks) / count;

        return new PublisherMetricsSnapshot(
            PublishedCount: count,
            AveragePayloadBytes: avgPayload,
            LastPayloadBytes: Interlocked.Read(ref _lastPayloadBytes),
            AverageLatencyMs: TimeSpan.FromTicks(avgLatencyTicks).TotalMilliseconds,
            LastLatencyMs: TimeSpan.FromTicks(Interlocked.Read(ref _lastLatencyTicks)).TotalMilliseconds);
    }
}

public sealed record PublisherMetricsSnapshot(
    long PublishedCount,
    long AveragePayloadBytes,
    long LastPayloadBytes,
    double AverageLatencyMs,
    double LastLatencyMs);
