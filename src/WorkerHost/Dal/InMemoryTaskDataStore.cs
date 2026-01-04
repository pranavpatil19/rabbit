using WorkerHost.Common.Models;
using WorkerHost.Messaging;

namespace WorkerHost.Dal;

public sealed class InMemoryTaskDataStore : ITaskDataStore
{
    private readonly TestDataContext _context;

    public InMemoryTaskDataStore(TestDataContext context)
    {
        _context = context;
    }

    public async IAsyncEnumerable<TaskRecord> StreamAgentTasksAsync(string computerId, string agentId, TaskFilter? filter, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var record in _context.GetAgentTasks(computerId, agentId, filter))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return record;
            await Task.Yield();
        }
    }

    public async IAsyncEnumerable<TaskRecord> StreamTaskBatchAsync(string computerId, string agentId, IReadOnlyCollection<string> taskIds, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var record in _context.GetTaskBatch(computerId, agentId, taskIds))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return record;
            await Task.Yield();
        }
    }

    public Task InsertTasksAsync(string destinationComputerId, string destinationAgentId, IReadOnlyCollection<TaskRecord> tasks, CancellationToken cancellationToken)
    {
        _context.InsertTasks(destinationComputerId, destinationAgentId, tasks);
        return Task.CompletedTask;
    }

    public Task MarkTasksMigratedAsync(string computerId, string agentId, IEnumerable<string> taskIds, CancellationToken cancellationToken)
    {
        _context.RemoveTasks(computerId, agentId, taskIds);
        return Task.CompletedTask;
    }
}
