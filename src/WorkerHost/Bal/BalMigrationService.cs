using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WorkerHost.Common.Extensions;
using WorkerHost.Common.Models;
using WorkerHost.Dal;
using WorkerHost.Logging;
using WorkerHost.Messaging;
using WorkerHost.RabbitMq.Configuration;

namespace WorkerHost.Bal;

public sealed class BalMigrationService : IBalMigrationService
{
    private readonly IAgentDataStore _agentStore;
    private readonly ITaskDataStore _taskStore;
    private readonly IMigrationJobStore _jobStore;
    private readonly BrokerOptions _config;
    private readonly ILogger<BalMigrationService> _logger;

    public BalMigrationService(
        IAgentDataStore agentStore,
        ITaskDataStore taskStore,
        IMigrationJobStore jobStore,
        IOptions<BrokerOptions> config,
        ILogger<BalMigrationService> logger)
    {
        _agentStore = agentStore;
        _taskStore = taskStore;
        _jobStore = jobStore;
        _config = config.Value;
        _logger = logger;
    }

    public Task TransferComputerAsync(MigrationCommand command, ComputerMigrationPayload payload, CancellationToken cancellationToken)
    {
        return ExecuteWithJobTrackingAsync(
            command,
            async ct =>
            {
                var agents = await _agentStore.GetAgentsByComputerAsync(payload.SourceComputerId, payload.IncludeAgentIds, ct)
                    .ConfigureAwait(false);

                var totalAgents = 0;
                var totalTasks = 0;

                foreach (var agent in agents)
                {
                    var agentPayload = new AgentMigrationPayload
                    {
                        SourceComputerId = agent.ComputerId,
                        DestinationComputerId = payload.DestinationComputerId,
                        SourceAgentId = agent.AgentId,
                        DestinationAgentId = agent.AgentId,
                        TaskFilter = null,
                    };

                var result = await TransferAgentCoreAsync(agentPayload, ct).ConfigureAwait(false);
                    totalAgents += result.AgentsMigrated;
                    totalTasks += result.TasksMigrated;
                    await _jobStore.IncrementProgressAsync(command.MigrationId, result.AgentsMigrated, result.TasksMigrated, ct)
                        .ConfigureAwait(false);
                }

                return new TransferExecutionResult(totalAgents, totalTasks);
            },
            cancellationToken);
    }

    public Task TransferAgentAsync(MigrationCommand command, AgentMigrationPayload payload, CancellationToken cancellationToken)
    {
        return ExecuteWithJobTrackingAsync(
            command,
            ct => TransferAgentCoreAsync(payload, ct),
            cancellationToken);
    }

    public Task TransferTaskBatchAsync(MigrationCommand command, TaskBatchMigrationPayload payload, CancellationToken cancellationToken)
    {
        return ExecuteWithJobTrackingAsync(
            command,
            async ct =>
            {
                var destinationComputerId = payload.TargetComputerId ?? payload.ComputerId;
                var destinationAgentId = payload.TargetAgentId ?? payload.AgentId;
                await _agentStore.EnsureDestinationAgentAsync(destinationComputerId, destinationAgentId, ct).ConfigureAwait(false);

                var tasksMigrated = await TransferTasksAsync(
                    () => _taskStore.StreamTaskBatchAsync(payload.ComputerId, payload.AgentId, payload.TaskIds, ct),
                    payload.ComputerId,
                    payload.AgentId,
                    destinationComputerId,
                    destinationAgentId,
                    ct).ConfigureAwait(false);

                return new TransferExecutionResult(
                    AgentsMigrated: destinationAgentId == payload.AgentId ? 0 : 1,
                    TasksMigrated: tasksMigrated);
            },
            cancellationToken);
    }

    private Task ExecuteWithJobTrackingAsync(
        MigrationCommand command,
        Func<CancellationToken, Task<TransferExecutionResult>> operation,
        CancellationToken cancellationToken)
    {
        return ExecuteInternalAsync(command, operation, cancellationToken);
    }

    private async Task ExecuteInternalAsync(
        MigrationCommand command,
        Func<CancellationToken, Task<TransferExecutionResult>> operation,
        CancellationToken cancellationToken)
    {
        using var logContext = MigrationLogContext.Push(command);

        await _jobStore.UpdateStatusAsync(command.MigrationId, MigrationJobStatus.InProgress, null, cancellationToken)
            .ConfigureAwait(false);
        LogMigration(command, LogLevel.Information, "Migration job moved to InProgress", null, null);

        try
        {
            var result = await operation(cancellationToken).ConfigureAwait(false);
            await _jobStore.UpdateStatusAsync(command.MigrationId, MigrationJobStatus.Completed, null, cancellationToken)
                .ConfigureAwait(false);
            _logger.LogInformation(
                "Migration {MigrationId} completed (agents: {Agents}, tasks: {Tasks})",
                command.MigrationId,
                result.AgentsMigrated,
                result.TasksMigrated);
            LogMigration(
                command,
                LogLevel.Information,
                "Migration completed (agents: {Agents}, tasks: {Tasks})",
                null,
                new Dictionary<string, object?>
                {
                    ["AgentsMigrated"] = result.AgentsMigrated,
                    ["TasksMigrated"] = result.TasksMigrated,
                },
                result.AgentsMigrated,
                result.TasksMigrated);
        }
        catch (Exception ex)
        {
            await _jobStore.UpdateStatusAsync(command.MigrationId, MigrationJobStatus.Failed, ex.Message, cancellationToken)
                .ConfigureAwait(false);
            _logger.LogError(ex, "Migration {MigrationId} failed", command.MigrationId);
            LogMigration(
                command,
                LogLevel.Error,
                "Migration failed: {Error}",
                ex,
                null,
                ex.Message);
            throw new InvalidOperationException($"Migration {command.MigrationId} failed.", ex);
        }
    }

    private async Task<TransferExecutionResult> TransferAgentCoreAsync(
        AgentMigrationPayload payload,
        CancellationToken cancellationToken)
    {
        await _agentStore.LockAgentAsync(payload.SourceComputerId, payload.SourceAgentId, cancellationToken).ConfigureAwait(false);
        try
        {
            var sourceAgent = await _agentStore.GetAgentAsync(payload.SourceComputerId, payload.SourceAgentId, cancellationToken)
                .ConfigureAwait(false)
                ?? throw new InvalidOperationException($"Source agent {payload.SourceAgentId} not found.");

            var destinationAgent = await _agentStore.EnsureDestinationAgentAsync(payload.DestinationComputerId, payload.DestinationAgentId, cancellationToken)
                .ConfigureAwait(false);

            await _agentStore.ValidateAgentTransferAsync(sourceAgent, destinationAgent, cancellationToken).ConfigureAwait(false);

            var tasksMigrated = await TransferTasksAsync(
                () => _taskStore.StreamAgentTasksAsync(payload.SourceComputerId, payload.SourceAgentId, payload.TaskFilter, cancellationToken),
                payload.SourceComputerId,
                payload.SourceAgentId,
                payload.DestinationComputerId,
                payload.DestinationAgentId,
                cancellationToken).ConfigureAwait(false);

            return new TransferExecutionResult(AgentsMigrated: 1, TasksMigrated: tasksMigrated);
        }
        finally
        {
            await _agentStore.UnlockAgentAsync(payload.SourceComputerId, payload.SourceAgentId, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private async Task<int> TransferTasksAsync(
        Func<IAsyncEnumerable<TaskRecord>> streamFactory,
        string sourceComputerId,
        string sourceAgentId,
        string destinationComputerId,
        string destinationAgentId,
        CancellationToken cancellationToken)
    {
        var chunkSize = Math.Clamp(_config.DefaultBatchLimit, 1, _config.MaxBatchLimit);
        var total = 0;

        await foreach (var chunk in streamFactory().ChunkAsync(chunkSize, cancellationToken).ConfigureAwait(false))
        {
            if (chunk.Count == 0)
            {
                continue;
            }

            var rewrittenTasks = chunk
                .Select(task => task with { ComputerId = destinationComputerId, AgentId = destinationAgentId })
                .ToArray();

            await _taskStore.InsertTasksAsync(destinationComputerId, destinationAgentId, rewrittenTasks, cancellationToken)
                .ConfigureAwait(false);

            await _taskStore.MarkTasksMigratedAsync(sourceComputerId, sourceAgentId, chunk.Select(task => task.TaskId), cancellationToken)
                .ConfigureAwait(false);

            total += chunk.Count;
        }

        _logger.LogInformation(
            "Transferred {TaskCount} tasks from {SourceComputer}/{SourceAgent} to {DestinationComputer}/{DestinationAgent}",
            total,
            sourceComputerId,
            sourceAgentId,
            destinationComputerId,
            destinationAgentId);

        return total;
    }

    private sealed record TransferExecutionResult(int AgentsMigrated, int TasksMigrated);

    private void LogMigration(
        MigrationCommand command,
        LogLevel level,
        string template,
        Exception? exception,
        IReadOnlyDictionary<string, object?>? properties,
        params object?[] args)
    {
        ArgumentNullException.ThrowIfNull(command);
        using var scope = MigrationLogContext.PushProperties(properties);
        _logger.Log(level, exception, template, args);
    }
}
