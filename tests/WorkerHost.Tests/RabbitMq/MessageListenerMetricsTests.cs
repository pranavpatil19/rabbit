using WorkerHost.RabbitMq.Monitoring;

namespace WorkerHost.Tests.RabbitMq;

public sealed class MessageListenerMetricsTests
{
    [Fact]
    public void SnapshotStartsWithZeros()
    {
        var metrics = new MessageListenerMetrics();

        var snapshot = metrics.CreateSnapshot();

        Assert.Equal(0, snapshot.MessagesDequeued);
        Assert.Equal(0, snapshot.IdlePolls);
        Assert.Equal(0, snapshot.Failures);
        Assert.Equal(0, snapshot.Reconnects);
        Assert.Null(snapshot.LastMessageUtc);
        Assert.Null(snapshot.LastFailureUtc);
        Assert.False(snapshot.ConnectionOpen);
    }

    [Fact]
    public void SnapshotReflectsRecordedEvents()
    {
        var metrics = new MessageListenerMetrics();

        metrics.RecordConnectionOpened();
        metrics.RecordMessageDequeued();
        metrics.RecordIdlePoll();
        metrics.RecordFailure();
        metrics.RecordReconnectAttempt();
        metrics.RecordMessageDequeued();
        metrics.RecordConnectionClosed();

        var snapshot = metrics.CreateSnapshot();

        Assert.Equal(2, snapshot.MessagesDequeued);
        Assert.Equal(1, snapshot.IdlePolls);
        Assert.Equal(1, snapshot.Failures);
        Assert.Equal(1, snapshot.Reconnects);
        Assert.NotNull(snapshot.LastMessageUtc);
        Assert.NotNull(snapshot.LastFailureUtc);
        Assert.False(snapshot.ConnectionOpen);
    }
}
