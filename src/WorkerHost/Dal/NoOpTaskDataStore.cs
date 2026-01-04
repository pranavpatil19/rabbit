using System.Linq;
using WorkerHost.Common.Models;
using WorkerHost.Messaging;

namespace WorkerHost.Dal;

public sealed class NoOpTaskDataStore : ITaskDataStore
{
    public IAsyncEnumerable<TaskRecord> StreamAgentTasksAsync(string computerId, string agentId, TaskFilter? filter, CancellationToken cancellationToken)
        => EmptyAsync();

    public IAsyncEnumerable<TaskRecord> StreamTaskBatchAsync(string computerId, string agentId, IReadOnlyCollection<string> taskIds, CancellationToken cancellationToken)
        => EmptyAsync();

    public Task InsertTasksAsync(string destinationComputerId, string destinationAgentId, IReadOnlyCollection<TaskRecord> tasks, CancellationToken cancellationToken)
        => Task.CompletedTask;

    public Task MarkTasksMigratedAsync(string computerId, string agentId, IEnumerable<string> taskIds, CancellationToken cancellationToken)
        => Task.CompletedTask;

    private static async IAsyncEnumerable<TaskRecord> EmptyAsync()
    {
        await Task.CompletedTask;
        yield break;
    }
}
