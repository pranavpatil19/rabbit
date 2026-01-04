using System.Text.Json.Serialization;
using WorkerHost.Messaging;

namespace WorkerHost.Api.Migrations;

public sealed record MigrationRequestDto
{
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public MigrationScope Scope { get; init; }

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public MigrationPriority Priority { get; init; } = MigrationPriority.Normal;

    public string RequestedBy { get; init; } = string.Empty;
    public required TransferRequestDto Transfer { get; init; }
    public string? Notes { get; init; }
}

public sealed record TransferRequestDto
{
    public required TransferEndpointRequestDto Source { get; init; }
    public required TransferEndpointRequestDto Destination { get; init; }
}

public sealed record TransferEndpointRequestDto
{
    public string? ComputerId { get; init; }
    public string? AgentId { get; init; }
    public IReadOnlyCollection<string>? AgentIds { get; init; }
    public IReadOnlyCollection<string>? TaskIds { get; init; }
}

public sealed record MigrationAcceptedResponse(Guid MigrationId, string Status);

public sealed record MigrationStatusResponse(
    Guid MigrationId,
    string Scope,
    string Status,
    string? Message,
    int AgentsProcessed,
    int TasksProcessed,
    DateTimeOffset LastUpdatedUtc);
