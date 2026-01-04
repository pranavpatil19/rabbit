using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using WorkerHost.RabbitMq.Configuration;

namespace WorkerHost.RabbitMq.Monitoring;

/// <summary>
/// Health check that evaluates the message listener's connectivity and activity.
/// </summary>
public sealed class MessageListenerHealthCheck : IHealthCheck
{
    private readonly MessageListenerMetrics _metrics;
    private readonly TimeSpan _inactivityThreshold;

    public MessageListenerHealthCheck(
        MessageListenerMetrics metrics,
        IOptions<BrokerOptions> config)
    {
        _metrics = metrics;
        var seconds = Math.Max(1, config.Value.ListenerInactivityThresholdSeconds);
        _inactivityThreshold = TimeSpan.FromSeconds(seconds);
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var snapshot = _metrics.CreateSnapshot();
        var now = DateTimeOffset.UtcNow;
        var lastMessage = snapshot.LastMessageUtc ?? DateTimeOffset.MinValue;

        var isActive = snapshot.MessagesDequeued == 0 || now - lastMessage <= _inactivityThreshold;
        var connectionHealthy = snapshot.ConnectionOpen;

        var data = new Dictionary<string, object>
        {
            ["messagesDequeued"] = snapshot.MessagesDequeued,
            ["idlePolls"] = snapshot.IdlePolls,
            ["failures"] = snapshot.Failures,
            ["reconnects"] = snapshot.Reconnects,
            ["lastMessageUtc"] = snapshot.LastMessageUtc?.ToString("O") ?? "n/a",
            ["lastFailureUtc"] = snapshot.LastFailureUtc?.ToString("O") ?? "n/a",
            ["connectionOpen"] = snapshot.ConnectionOpen,
        };

        if (connectionHealthy && isActive)
        {
            return Task.FromResult(HealthCheckResult.Healthy("Message listener active", data));
        }

        var description = connectionHealthy
            ? "Message listener has been idle beyond the configured threshold"
            : "Message listener connection is closed";

        return Task.FromResult(HealthCheckResult.Unhealthy(description, data: data));
    }
}
