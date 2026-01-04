using WorkerHost.Common.Models;
using WorkerHost.Messaging;

namespace WorkerHost.Dal;

public interface IMigrationJobStore
{
    Task InitializeAsync(Guid migrationId, MigrationScope scope, CancellationToken cancellationToken);

    Task UpdateStatusAsync(Guid migrationId, MigrationJobStatus status, string? message, CancellationToken cancellationToken);

    Task IncrementProgressAsync(Guid migrationId, int agentsDelta, int tasksDelta, CancellationToken cancellationToken);

    Task<MigrationJobSnapshot?> GetAsync(Guid migrationId, CancellationToken cancellationToken);

    Task<IReadOnlyCollection<MigrationJobSnapshot>> ListAsync(CancellationToken cancellationToken);
}
