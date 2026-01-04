using System.Collections.Concurrent;
using WorkerHost.Common.Models;
using WorkerHost.Messaging;

namespace WorkerHost.Dal;

public sealed class InMemoryMigrationJobStore : IMigrationJobStore
{
    private readonly ConcurrentDictionary<Guid, MigrationJobSnapshot> _jobs = new();

    public Task InitializeAsync(Guid migrationId, MigrationScope scope, CancellationToken cancellationToken)
    {
        _jobs.TryAdd(
            migrationId,
            new MigrationJobSnapshot(
                MigrationId: migrationId,
                Scope: scope,
                Status: MigrationJobStatus.Queued,
                Message: null,
                AgentsProcessed: 0,
                TasksProcessed: 0,
                LastUpdatedUtc: DateTimeOffset.UtcNow));
        return Task.CompletedTask;
    }

    public Task UpdateStatusAsync(Guid migrationId, MigrationJobStatus status, string? message, CancellationToken cancellationToken)
    {
        _jobs.AddOrUpdate(
            migrationId,
            id => new MigrationJobSnapshot(id, MigrationScope.Agent, status, message, 0, 0, DateTimeOffset.UtcNow),
            (_, existing) => existing with
            {
                Status = status,
                Message = message,
                LastUpdatedUtc = DateTimeOffset.UtcNow,
            });
        return Task.CompletedTask;
    }

    public Task IncrementProgressAsync(Guid migrationId, int agentsDelta, int tasksDelta, CancellationToken cancellationToken)
    {
        _jobs.AddOrUpdate(
            migrationId,
            id => new MigrationJobSnapshot(id, MigrationScope.Agent, MigrationJobStatus.InProgress, null, agentsDelta, tasksDelta, DateTimeOffset.UtcNow),
            (_, existing) => existing with
            {
                AgentsProcessed = existing.AgentsProcessed + agentsDelta,
                TasksProcessed = existing.TasksProcessed + tasksDelta,
                LastUpdatedUtc = DateTimeOffset.UtcNow,
            });
        return Task.CompletedTask;
    }

    public Task<MigrationJobSnapshot?> GetAsync(Guid migrationId, CancellationToken cancellationToken)
    {
        _jobs.TryGetValue(migrationId, out var snapshot);
        return Task.FromResult(snapshot);
    }

    public Task<IReadOnlyCollection<MigrationJobSnapshot>> ListAsync(CancellationToken cancellationToken)
    {
        IReadOnlyCollection<MigrationJobSnapshot> copy = _jobs.Values.ToArray();
        return Task.FromResult(copy);
    }
}
