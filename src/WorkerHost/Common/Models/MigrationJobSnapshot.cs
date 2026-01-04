using WorkerHost.Messaging;

namespace WorkerHost.Common.Models;

public sealed record MigrationJobSnapshot(
    Guid MigrationId,
    MigrationScope Scope,
    MigrationJobStatus Status,
    string? Message,
    int AgentsProcessed,
    int TasksProcessed,
    DateTimeOffset LastUpdatedUtc);
