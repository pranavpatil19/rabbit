using System.Collections.Generic;
using System.Linq;
using Moq;
using WorkerHost.Bal;
using WorkerHost.Common.Models;
using WorkerHost.Dal;
using WorkerHost.Logging;
using WorkerHost.Messaging;
using WorkerHost.RabbitMq.Configuration;
using Microsoft.Extensions.Options;
using Xunit;

namespace WorkerHost.Tests.Bal;

public class BalMigrationServiceTests
{
    [Fact]
    public async Task TransferAgentAsync_MovesTasks_AndUpdatesJobStore()
    {
        var agentStore = new Mock<IAgentDataStore>();
        var taskStore = new Mock<ITaskDataStore>();
        var jobStore = new Mock<IMigrationJobStore>();
        var logQueue = new Mock<ILogQueue>();

        var agentPayload = new AgentMigrationPayload
        {
            SourceComputerId = "comp-1",
            DestinationComputerId = "comp-2",
            SourceAgentId = "agent-a",
            DestinationAgentId = "agent-b",
            TaskFilter = null,
        };

        var command = new MigrationCommand
        {
            MigrationId = Guid.NewGuid(),
            MigrationScope = MigrationScope.Agent,
            RequestedBy = "tester",
            RequestedAtUtc = DateTimeOffset.UtcNow,
            Transfer = new TransferPayload
            {
                Source = new TransferEndpoint
                {
                    ComputerId = agentPayload.SourceComputerId,
                    AgentId = agentPayload.SourceAgentId,
                },
                Destination = new TransferEndpoint
                {
                    ComputerId = agentPayload.DestinationComputerId,
                    AgentId = agentPayload.DestinationAgentId,
                },
            },
        };

        var sourceAgent = new AgentInfo("comp-1", "agent-a", false, 2);
        var destAgent = new AgentInfo("comp-2", "agent-b", false, 0);
        var tasks = new[]
        {
            new TaskRecord("task-1", "comp-1", "agent-a", "Pending", "{}"),
            new TaskRecord("task-2", "comp-1", "agent-a", "Pending", "{}"),
        };

        agentStore.Setup(s => s.LockAgentAsync("comp-1", "agent-a", It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        agentStore.Setup(s => s.UnlockAgentAsync("comp-1", "agent-a", It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        agentStore.Setup(s => s.GetAgentAsync("comp-1", "agent-a", It.IsAny<CancellationToken>()))
            .ReturnsAsync(sourceAgent);
        agentStore.Setup(s => s.EnsureDestinationAgentAsync("comp-2", "agent-b", It.IsAny<CancellationToken>()))
            .ReturnsAsync(destAgent);
        agentStore.Setup(s => s.ValidateAgentTransferAsync(sourceAgent, destAgent, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        taskStore.Setup(s => s.StreamAgentTasksAsync("comp-1", "agent-a", null, It.IsAny<CancellationToken>()))
            .Returns(ToAsyncEnumerable(tasks));
        taskStore.Setup(s => s.InsertTasksAsync("comp-2", "agent-b", It.IsAny<IReadOnlyCollection<TaskRecord>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        taskStore.Setup(s => s.MarkTasksMigratedAsync("comp-1", "agent-a", It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        jobStore.Setup(s => s.UpdateStatusAsync(command.MigrationId, MigrationJobStatus.InProgress, null, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        jobStore.Setup(s => s.UpdateStatusAsync(command.MigrationId, MigrationJobStatus.Completed, null, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        jobStore.Setup(s => s.IncrementProgressAsync(command.MigrationId, It.IsAny<int>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        logQueue.Setup(q => q.EnqueueAsync(It.IsAny<MigrationLogEvent>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        var service = new BalMigrationService(
            agentStore.Object,
            taskStore.Object,
            jobStore.Object,
            Options.Create(CreateConfig()),
            Mock.Of<Microsoft.Extensions.Logging.ILogger<BalMigrationService>>(),
            logQueue.Object);

        await service.TransferAgentAsync(command, agentPayload, CancellationToken.None);

        taskStore.Verify(s =>
            s.InsertTasksAsync(
                "comp-2",
                "agent-b",
                It.Is<IReadOnlyCollection<TaskRecord>>(collection =>
                    collection.All(t => t.ComputerId == "comp-2" && t.AgentId == "agent-b")),
                It.IsAny<CancellationToken>()),
            Times.Once);

        jobStore.Verify(s => s.UpdateStatusAsync(command.MigrationId, MigrationJobStatus.InProgress, null, It.IsAny<CancellationToken>()), Times.Once);
        jobStore.Verify(s => s.UpdateStatusAsync(command.MigrationId, MigrationJobStatus.Completed, null, It.IsAny<CancellationToken>()), Times.Once);
        agentStore.VerifyAll();
    }

    [Fact]
    public async Task TransferComputerAsync_MigratesEachAgentAndAccumulatesProgress()
    {
        var agentStore = new Mock<IAgentDataStore>(MockBehavior.Strict);
        var taskStore = new Mock<ITaskDataStore>(MockBehavior.Strict);
        var jobStore = new Mock<IMigrationJobStore>(MockBehavior.Strict);
        var logQueue = new Mock<ILogQueue>();
        var includeAgents = new[] { "agent-a", "agent-b" };

        var command = new MigrationCommand
        {
            MigrationId = Guid.NewGuid(),
            MigrationScope = MigrationScope.Computer,
            RequestedBy = "tester",
            RequestedAtUtc = DateTimeOffset.UtcNow,
            Transfer = new TransferPayload
            {
                Source = new TransferEndpoint { ComputerId = "comp-source" },
                Destination = new TransferEndpoint { ComputerId = "comp-dest" },
            },
        };

        var payload = new ComputerMigrationPayload
        {
            SourceComputerId = "comp-source",
            DestinationComputerId = "comp-dest",
            IncludeAgentIds = includeAgents,
        };

        var agents = new[]
        {
            new AgentInfo("comp-source", "agent-a", false, 2),
            new AgentInfo("comp-source", "agent-b", false, 1),
        };

        agentStore.Setup(s => s.GetAgentsByComputerAsync("comp-source", includeAgents, It.IsAny<CancellationToken>()))
            .ReturnsAsync(agents);

        foreach (var agent in agents)
        {
            agentStore.Setup(s => s.LockAgentAsync(agent.ComputerId, agent.AgentId, It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            agentStore.Setup(s => s.UnlockAgentAsync(agent.ComputerId, agent.AgentId, It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            agentStore.Setup(s => s.GetAgentAsync(agent.ComputerId, agent.AgentId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(agent);
            agentStore.Setup(s => s.EnsureDestinationAgentAsync("comp-dest", agent.AgentId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new AgentInfo("comp-dest", agent.AgentId, false, 0));
            agentStore.Setup(s => s.ValidateAgentTransferAsync(agent, It.IsAny<AgentInfo>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
        }

        taskStore.Setup(s => s.StreamAgentTasksAsync("comp-source", "agent-a", null, It.IsAny<CancellationToken>()))
            .Returns(ToAsyncEnumerable(new[]
            {
                new TaskRecord("task-1", "comp-source", "agent-a", "Queued", "{}"),
                new TaskRecord("task-2", "comp-source", "agent-a", "Queued", "{}"),
            }));
        taskStore.Setup(s => s.StreamAgentTasksAsync("comp-source", "agent-b", null, It.IsAny<CancellationToken>()))
            .Returns(ToAsyncEnumerable(new[]
            {
                new TaskRecord("task-3", "comp-source", "agent-b", "Queued", "{}"),
            }));

        taskStore.Setup(s => s.InsertTasksAsync("comp-dest", "agent-a", It.IsAny<IReadOnlyCollection<TaskRecord>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        taskStore.Setup(s => s.InsertTasksAsync("comp-dest", "agent-b", It.IsAny<IReadOnlyCollection<TaskRecord>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        taskStore.Setup(s => s.MarkTasksMigratedAsync("comp-source", "agent-a", It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        taskStore.Setup(s => s.MarkTasksMigratedAsync("comp-source", "agent-b", It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        jobStore.Setup(s => s.UpdateStatusAsync(command.MigrationId, MigrationJobStatus.InProgress, null, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        jobStore.Setup(s => s.UpdateStatusAsync(command.MigrationId, MigrationJobStatus.Completed, null, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        jobStore.Setup(s => s.IncrementProgressAsync(command.MigrationId, 1, 2, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        jobStore.Setup(s => s.IncrementProgressAsync(command.MigrationId, 1, 1, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        logQueue.Setup(q => q.EnqueueAsync(It.IsAny<MigrationLogEvent>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        var service = new BalMigrationService(
            agentStore.Object,
            taskStore.Object,
            jobStore.Object,
            Options.Create(CreateConfig()),
            Mock.Of<Microsoft.Extensions.Logging.ILogger<BalMigrationService>>(),
            logQueue.Object);

        await service.TransferComputerAsync(command, payload, CancellationToken.None);

        agentStore.Verify(s => s.GetAgentsByComputerAsync("comp-source", includeAgents, It.IsAny<CancellationToken>()), Times.Once);
        taskStore.Verify(s => s.InsertTasksAsync("comp-dest", "agent-a", It.IsAny<IReadOnlyCollection<TaskRecord>>(), It.IsAny<CancellationToken>()), Times.Once);
        taskStore.Verify(s => s.InsertTasksAsync("comp-dest", "agent-b", It.IsAny<IReadOnlyCollection<TaskRecord>>(), It.IsAny<CancellationToken>()), Times.Once);
        jobStore.Verify(s => s.IncrementProgressAsync(command.MigrationId, 1, 2, It.IsAny<CancellationToken>()), Times.Once);
        jobStore.Verify(s => s.IncrementProgressAsync(command.MigrationId, 1, 1, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TransferTaskBatchAsync_StreamsSpecifiedTasksAndRespectsTargets()
    {
        var agentStore = new Mock<IAgentDataStore>();
        var taskStore = new Mock<ITaskDataStore>();
        var jobStore = new Mock<IMigrationJobStore>();
        var logQueue = new Mock<ILogQueue>();

        var payload = new TaskBatchMigrationPayload
        {
            ComputerId = "comp-1",
            AgentId = "agent-a",
            TaskIds = new[] { "task-10", "task-11" },
            TargetComputerId = "comp-2",
            TargetAgentId = "agent-b",
        };

        var command = new MigrationCommand
        {
            MigrationId = Guid.NewGuid(),
            MigrationScope = MigrationScope.TaskBatch,
            RequestedBy = "tester",
            RequestedAtUtc = DateTimeOffset.UtcNow,
            Transfer = new TransferPayload
            {
                Source = new TransferEndpoint { ComputerId = payload.ComputerId, AgentId = payload.AgentId, TaskIds = payload.TaskIds },
                Destination = new TransferEndpoint { ComputerId = payload.TargetComputerId, AgentId = payload.TargetAgentId },
            },
        };

        var tasks = new[]
        {
            new TaskRecord("task-10", "comp-1", "agent-a", "Queued", "{}"),
            new TaskRecord("task-11", "comp-1", "agent-a", "Queued", "{}"),
        };

        agentStore.Setup(s => s.EnsureDestinationAgentAsync("comp-2", "agent-b", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AgentInfo("comp-2", "agent-b", false, 0));

        taskStore.Setup(s => s.StreamTaskBatchAsync("comp-1", "agent-a", payload.TaskIds, It.IsAny<CancellationToken>()))
            .Returns(ToAsyncEnumerable(tasks));

        taskStore.Setup(s => s.InsertTasksAsync("comp-2", "agent-b", It.Is<IReadOnlyCollection<TaskRecord>>(collection =>
                collection.Count == 2 && collection.All(t => t.ComputerId == "comp-2" && t.AgentId == "agent-b")),
            It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        taskStore.Setup(s => s.MarkTasksMigratedAsync("comp-1", "agent-a", It.Is<IEnumerable<string>>(ids => ids.SequenceEqual(payload.TaskIds)), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        jobStore.Setup(s => s.UpdateStatusAsync(command.MigrationId, MigrationJobStatus.InProgress, null, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        jobStore.Setup(s => s.UpdateStatusAsync(command.MigrationId, MigrationJobStatus.Completed, null, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        logQueue.Setup(q => q.EnqueueAsync(It.IsAny<MigrationLogEvent>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        var service = new BalMigrationService(
            agentStore.Object,
            taskStore.Object,
            jobStore.Object,
            Options.Create(CreateConfig()),
            Mock.Of<Microsoft.Extensions.Logging.ILogger<BalMigrationService>>(),
            logQueue.Object);

        await service.TransferTaskBatchAsync(command, payload, CancellationToken.None);

        agentStore.Verify(s => s.EnsureDestinationAgentAsync("comp-2", "agent-b", It.IsAny<CancellationToken>()), Times.Once);
        taskStore.Verify(s => s.StreamTaskBatchAsync("comp-1", "agent-a", payload.TaskIds, It.IsAny<CancellationToken>()), Times.Once);
        taskStore.Verify(s => s.MarkTasksMigratedAsync("comp-1", "agent-a", It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    private static async IAsyncEnumerable<TaskRecord> ToAsyncEnumerable(IEnumerable<TaskRecord> records)
    {
        foreach (var record in records)
        {
            yield return record;
            await Task.Yield();
        }
    }

    private static BrokerOptions CreateConfig() => new()
    {
        Connection = new BrokerConnectionOptions
        {
            ConnectionString = "amqp://guest:guest@localhost/",
            Host = "localhost",
            Port = 5672,
            UserName = "guest",
            Password = "guest",
            ClientProvidedName = "test-worker",
        },
        WorkEndpoint = new WorkEndpointOptions
        {
            Exchange = new ExchangeOptions { Name = "ex", Type = "direct" },
            Queue = new QueueOptions { Name = "queue" },
            RoutingKey = "key",
            DeclareInfrastructure = true,
        },
        DefaultBatchLimit = 10,
        MaxBatchLimit = 200,
        DefaultConcurrency = 5,
        MaxConcurrency = 32,
        PullTimeoutSeconds = 1,
        RequeueOnError = true,
            MaxRequeueAttempts = 5,
        LogSequenceChannelName = "log-channel",
    };
}
