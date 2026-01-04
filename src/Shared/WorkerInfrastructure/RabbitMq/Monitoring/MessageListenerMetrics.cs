using System;
using System.Threading;

namespace WorkerHost.RabbitMq.Monitoring;

/// <summary>
/// Lightweight metrics collector the message listener updates so health endpoints can report broker state.
/// </summary>
public sealed class MessageListenerMetrics
{
    private long _messagesDequeued;
    private long _idlePolls;
    private long _failures;
    private long _reconnects;
    private long _lastMessageUnixMs;
    private long _lastFailureUnixMs;
    private int _connectionOpen;
    private long _bufferedDeliveries;
    private int _prefetchCount;
    private long _totalDeliveryLatencyTicks;
    private long _deliveryCount;
    private long _lastDeliveryLatencyTicks;

    public void RecordMessageDequeued()
    {
        Interlocked.Increment(ref _messagesDequeued);
        Interlocked.Exchange(ref _lastMessageUnixMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }

    public void RecordIdlePoll() => Interlocked.Increment(ref _idlePolls);

    public void RecordFailure()
    {
        Interlocked.Increment(ref _failures);
        Interlocked.Exchange(ref _lastFailureUnixMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }

    public void RecordReconnectAttempt() => Interlocked.Increment(ref _reconnects);

    public void RecordConnectionOpened() => Interlocked.Exchange(ref _connectionOpen, 1);

    public void RecordConnectionClosed() => Interlocked.Exchange(ref _connectionOpen, 0);

    public void RecordDeliveryBuffered() => Interlocked.Increment(ref _bufferedDeliveries);

    public void RecordBufferedDeliveryReleased()
    {
        var value = Interlocked.Decrement(ref _bufferedDeliveries);
        if (value < 0)
        {
            Interlocked.Exchange(ref _bufferedDeliveries, 0);
        }
    }

    public void SetPrefetch(int prefetch) => Interlocked.Exchange(ref _prefetchCount, prefetch);

    public void RecordDeliveryLatency(TimeSpan latency)
    {
        Interlocked.Add(ref _totalDeliveryLatencyTicks, latency.Ticks);
        Interlocked.Increment(ref _deliveryCount);
        Interlocked.Exchange(ref _lastDeliveryLatencyTicks, latency.Ticks);
    }

    public MessageListenerMetricsSnapshot CreateSnapshot()
    {
        var lastMessage = ReadTimestamp(ref _lastMessageUnixMs);
        var lastFailure = ReadTimestamp(ref _lastFailureUnixMs);
        var deliveries = Interlocked.Read(ref _deliveryCount);
        var avgLatencyTicks = deliveries == 0 ? 0 : Interlocked.Read(ref _totalDeliveryLatencyTicks) / deliveries;

        return new MessageListenerMetricsSnapshot(
            MessagesDequeued: Interlocked.Read(ref _messagesDequeued),
            IdlePolls: Interlocked.Read(ref _idlePolls),
            Failures: Interlocked.Read(ref _failures),
            Reconnects: Interlocked.Read(ref _reconnects),
            LastMessageUtc: lastMessage,
            LastFailureUtc: lastFailure,
            ConnectionOpen: Volatile.Read(ref _connectionOpen) == 1,
            BufferedDeliveries: Math.Max(0, Interlocked.Read(ref _bufferedDeliveries)),
            Prefetch: Volatile.Read(ref _prefetchCount),
            AverageDeliveryLatencyMs: TimeSpan.FromTicks(avgLatencyTicks).TotalMilliseconds,
            LastDeliveryLatencyMs: TimeSpan.FromTicks(Interlocked.Read(ref _lastDeliveryLatencyTicks)).TotalMilliseconds);
    }

    private static DateTimeOffset? ReadTimestamp(ref long storage)
    {
        var value = Interlocked.Read(ref storage);
        return value == 0 ? null : DateTimeOffset.FromUnixTimeMilliseconds(value);
    }
}

public sealed record MessageListenerMetricsSnapshot(
    long MessagesDequeued,
    long IdlePolls,
    long Failures,
    long Reconnects,
    DateTimeOffset? LastMessageUtc,
    DateTimeOffset? LastFailureUtc,
    bool ConnectionOpen,
    long BufferedDeliveries,
    int Prefetch,
    double AverageDeliveryLatencyMs,
    double LastDeliveryLatencyMs);
