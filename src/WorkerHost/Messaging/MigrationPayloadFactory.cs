namespace WorkerHost.Messaging;

/// <summary>
/// Builds strongly-typed migration payloads from a command, validating required fields per scope.
/// </summary>
public static class MigrationPayloadFactory
{
    /// <summary>
    /// Create the correct payload object for the command's scope or throw if missing/invalid.
    /// </summary>
    public static object CreatePayload(MigrationCommand command) =>
        command.MigrationScope switch
        {
            MigrationScope.Computer => CreateComputerPayload(command),
            MigrationScope.Agent => CreateAgentPayload(command),
            MigrationScope.TaskBatch => CreateTaskBatchPayload(command),
            _ => throw new InvalidOperationException($"Migration scope {command.MigrationScope} missing payload"),
        };

    /// <summary>Builds a computer migration payload and validates required IDs.</summary>
    private static object CreateComputerPayload(MigrationCommand command)
    {
        var transfer = command.Transfer ?? throw new InvalidOperationException("Transfer payload missing.");
        var sourceComputer = Require(transfer.Source.ComputerId, "source computerId");
        var destinationComputer = Require(transfer.Destination.ComputerId, "destination computerId");
        return new ComputerMigrationPayload
        {
            SourceComputerId = sourceComputer,
            DestinationComputerId = destinationComputer,
            IncludeAgentIds = transfer.Source.AgentIds,
        };
    }

    /// <summary>Builds an agent migration payload and validates required IDs.</summary>
    private static object CreateAgentPayload(MigrationCommand command)
    {
        var transfer = command.Transfer ?? throw new InvalidOperationException("Transfer payload missing.");
        return new AgentMigrationPayload
        {
            SourceComputerId = Require(transfer.Source.ComputerId, "source computerId"),
            SourceAgentId = Require(transfer.Source.AgentId, "source agentId"),
            DestinationComputerId = transfer.Destination.ComputerId ?? transfer.Source.ComputerId ?? throw new InvalidOperationException("destination computerId missing"),
            DestinationAgentId = Require(transfer.Destination.AgentId, "destination agentId"),
            TaskFilter = null,
        };
    }

    /// <summary>Builds a task-batch migration payload and validates required IDs/task list.</summary>
    private static object CreateTaskBatchPayload(MigrationCommand command)
    {
        var transfer = command.Transfer ?? throw new InvalidOperationException("Transfer payload missing.");
        var taskIds = transfer.Source.TaskIds ?? Array.Empty<string>();
        if (!taskIds.Any())
        {
            throw new InvalidOperationException("At least one taskId must be supplied for TaskBatch migrations.");
        }

        return new TaskBatchMigrationPayload
        {
            ComputerId = Require(transfer.Source.ComputerId, "source computerId"),
            AgentId = Require(transfer.Source.AgentId, "source agentId"),
            TaskIds = taskIds,
            TargetComputerId = transfer.Destination.ComputerId,
            TargetAgentId = transfer.Destination.AgentId,
        };
    }

    private static string Require(string? value, string fieldName)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"Missing {fieldName}.");
}
