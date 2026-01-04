using WorkerHost.Api.Migrations;
using WorkerHost.Messaging;
using Xunit;

namespace WorkerHost.Tests.Api;

public class MigrationRequestMapperTests
{
    [Fact]
    public void ToCommand_Computerscope_MapsPayload()
    {
        var request = new MigrationRequestDto
        {
            RequestedBy = "tester",
            Scope = MigrationScope.Computer,
            Transfer = new TransferRequestDto
            {
                Source = new TransferEndpointRequestDto
                {
                    ComputerId = "comp-1",
                    AgentIds = new[] { "agent-1" },
                },
                Destination = new TransferEndpointRequestDto
                {
                    ComputerId = "comp-2",
                },
            },
            Notes = "move all",
        };

        var command = MigrationRequestMapper.ToCommand(request);

        Assert.Equal(MigrationScope.Computer, command.MigrationScope);
        Assert.Equal("comp-1", command.Transfer.Source.ComputerId);
        Assert.Equal("comp-2", command.Transfer.Destination.ComputerId);
        Assert.Contains("agent-1", command.Transfer.Source.AgentIds!);
        Assert.Equal("move all", command.Metadata?.Notes);
    }

    [Fact]
    public void ToCommand_TaskBatchScope_MapsPayload()
    {
        var request = new MigrationRequestDto
        {
            RequestedBy = "tester",
            Scope = MigrationScope.TaskBatch,
            Transfer = new TransferRequestDto
            {
                Source = new TransferEndpointRequestDto
                {
                    ComputerId = "comp-1",
                    AgentId = "agent-a",
                    TaskIds = new[] { "task-1", "task-2" },
                },
                Destination = new TransferEndpointRequestDto
                {
                    ComputerId = "comp-2",
                    AgentId = "agent-b",
                },
            },
        };

        var command = MigrationRequestMapper.ToCommand(request);

        Assert.Equal(MigrationScope.TaskBatch, command.MigrationScope);
        Assert.Equal("comp-1", command.Transfer.Source.ComputerId);
        Assert.Equal("agent-a", command.Transfer.Source.AgentId);
        Assert.Contains("task-2", command.Transfer.Source.TaskIds!);
        Assert.Equal("agent-b", command.Transfer.Destination.AgentId);
    }
}
