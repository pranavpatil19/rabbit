using WorkerHost.Common.Models;
using WorkerHost.Messaging;

namespace WorkerHost.Dal;

public interface ITaskDataStore
{
    IAsyncEnumerable<TaskRecord> StreamAgentTasksAsync(
        string computerId,
        string agentId,
        TaskFilter? filter,
        CancellationToken cancellationToken);

    IAsyncEnumerable<TaskRecord> StreamTaskBatchAsync(
        string computerId,
        string agentId,
        IReadOnlyCollection<string> taskIds,
        CancellationToken cancellationToken);

    Task InsertTasksAsync(
        string destinationComputerId,
        string destinationAgentId,
        IReadOnlyCollection<TaskRecord> tasks,
        CancellationToken cancellationToken);

    Task MarkTasksMigratedAsync(
        string computerId,
        string agentId,
        IEnumerable<string> taskIds,
        CancellationToken cancellationToken);
}
