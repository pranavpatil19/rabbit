using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using WorkerHost.RabbitMq.Configuration;
using WorkerHost.RabbitMq.Monitoring;

namespace WorkerHost.Tests.RabbitMq;

public sealed class MessageListenerHealthCheckTests
{
    [Fact]
    public async Task HealthyWhenConnectionOpenAndRecentMessage()
    {
        var metrics = new MessageListenerMetrics();
        metrics.RecordConnectionOpened();
        metrics.RecordMessageDequeued();

        var healthCheck = new MessageListenerHealthCheck(metrics, Options.Create(new BrokerOptions
        {
            ListenerInactivityThresholdSeconds = 5,
        }));

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task UnhealthyWhenIdleBeyondThreshold()
    {
        var metrics = new MessageListenerMetrics();
        metrics.RecordConnectionOpened();
        metrics.RecordMessageDequeued();

        var healthCheck = new MessageListenerHealthCheck(metrics, Options.Create(new BrokerOptions
        {
            ListenerInactivityThresholdSeconds = 1,
        }));

        await Task.Delay(TimeSpan.FromSeconds(1.2));

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("idle", result.Description, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task UnhealthyWhenConnectionClosed()
    {
        var metrics = new MessageListenerMetrics();
        metrics.RecordFailure();

        var healthCheck = new MessageListenerHealthCheck(metrics, Options.Create(new BrokerOptions()));

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("connection", result.Description, StringComparison.OrdinalIgnoreCase);
    }
}
