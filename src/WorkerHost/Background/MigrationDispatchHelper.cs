using WorkerHost.Bal;
using WorkerHost.Messaging;

namespace WorkerHost.Background;

/// <summary>
/// Routes migration commands to the correct BAL handler.
/// </summary>
internal static class MigrationDispatchHelper
{
    public static async Task DispatchAsync(IBalMigrationService balService, MigrationCommand command, CancellationToken cancellationToken)
    {
        var payload = MigrationPayloadFactory.CreatePayload(command);
        switch (payload)
        {
            case ComputerMigrationPayload computer:
                await balService.TransferComputerAsync(command, computer, cancellationToken).ConfigureAwait(false);
                break;
            case AgentMigrationPayload agent:
                await balService.TransferAgentAsync(command, agent, cancellationToken).ConfigureAwait(false);
                break;
            case TaskBatchMigrationPayload taskBatch:
                await balService.TransferTaskBatchAsync(command, taskBatch, cancellationToken).ConfigureAwait(false);
                break;
            default:
                throw new InvalidOperationException("Unsupported payload type.");
        }
    }
}
