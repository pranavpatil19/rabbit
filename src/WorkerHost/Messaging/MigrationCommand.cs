using System.Text.Json.Serialization;

namespace WorkerHost.Messaging;

public enum MigrationScope
{
    Computer,
    Agent,
    TaskBatch,
}

public enum MigrationPriority
{
    Low,
    Normal,
    High,
}

public sealed record MigrationCommand
{
    public Guid MigrationId { get; init; }
    public MigrationScope MigrationScope { get; init; }
    public string RequestedBy { get; init; } = string.Empty;
    public DateTimeOffset RequestedAtUtc { get; init; }
    public MigrationPriority Priority { get; init; } = MigrationPriority.Normal;
    public int RetryCount { get; init; }
    public MigrationMetadata? Metadata { get; init; }
    public ExpectedCounts? ExpectedCounts { get; init; }
    public string? ValidationChecksum { get; init; }
    public CallbackInfo? Callback { get; init; }
    public required TransferPayload Transfer { get; init; }
}

public sealed record MigrationMetadata
{
    public string? ReasonCode { get; init; }
    public string? Notes { get; init; }
}

public sealed record ExpectedCounts
{
    public int? Agents { get; init; }
    public int? Tasks { get; init; }
}

public sealed record CallbackInfo
{
    public Uri? WebhookUrl { get; init; }
    public string? Channel { get; init; }
}

public sealed record ComputerMigrationPayload
{
    public required string SourceComputerId { get; init; }
    public required string DestinationComputerId { get; init; }
    public IReadOnlyCollection<string>? IncludeAgentIds { get; init; }
}

public sealed record AgentMigrationPayload
{
    public required string SourceComputerId { get; init; }
    public required string DestinationComputerId { get; init; }
    public required string SourceAgentId { get; init; }
    public required string DestinationAgentId { get; init; }
    public TaskFilter? TaskFilter { get; init; }
}

public sealed record TaskBatchMigrationPayload
{
    public required string ComputerId { get; init; }
    public required string AgentId { get; init; }
    public IReadOnlyCollection<string> TaskIds { get; init; } = Array.Empty<string>();
    public string? TargetComputerId { get; init; }
    public string? TargetAgentId { get; init; }
}

public sealed record TransferPayload
{
    public required TransferEndpoint Source { get; init; }
    public required TransferEndpoint Destination { get; init; }
}

public sealed record TransferEndpoint
{
    public string? ComputerId { get; init; }
    public string? AgentId { get; init; }
    public IReadOnlyCollection<string>? AgentIds { get; init; }
    public IReadOnlyCollection<string>? TaskIds { get; init; }
}

public sealed record TaskFilter
{
    public string? Status { get; init; }
    public string? TaskType { get; init; }
    public DateTimeOffset? ScheduledAfterUtc { get; init; }
    public DateTimeOffset? ScheduledBeforeUtc { get; init; }
}
