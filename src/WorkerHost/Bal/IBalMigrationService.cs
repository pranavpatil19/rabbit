using WorkerHost.Messaging;

namespace WorkerHost.Bal;

/// <summary>
/// Business logic surface that orchestrates migrations across DAL boundaries.
/// </summary>
public interface IBalMigrationService
{
    Task TransferComputerAsync(MigrationCommand command, ComputerMigrationPayload payload, CancellationToken cancellationToken);

    Task TransferAgentAsync(MigrationCommand command, AgentMigrationPayload payload, CancellationToken cancellationToken);

    Task TransferTaskBatchAsync(MigrationCommand command, TaskBatchMigrationPayload payload, CancellationToken cancellationToken);
}
