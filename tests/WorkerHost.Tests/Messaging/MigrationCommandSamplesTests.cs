using System;
using System.Linq;
using System.Text.Json;
using WorkerHost.Messaging;
using Xunit;

namespace WorkerHost.Tests.Messaging;

public class MigrationCommandSamplesTests
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    [Fact]
    public void ComputerSample_Deserializes_WithExpectedPayload()
    {
        var command = LoadSample("computer-migration-command.json");

        Assert.Equal(MigrationScope.Computer, command.MigrationScope);
        Assert.Equal("comp-01", command.Transfer.Source.ComputerId);
        Assert.Equal("comp-99", command.Transfer.Destination.ComputerId);
        Assert.Equal("scheduler-api", command.RequestedBy);
        Assert.Equal(MigrationPriority.High, command.Priority);
    }

    [Fact]
    public void AgentSample_Deserializes_WithExpectedPayload()
    {
        var command = LoadSample("agent-migration-command.json");

        Assert.Equal(MigrationScope.Agent, command.MigrationScope);
        Assert.Equal("comp-10", command.Transfer.Source.ComputerId);
        Assert.Equal("agent-x", command.Transfer.Source.AgentId);
        Assert.Equal("comp-20", command.Transfer.Destination.ComputerId);
        Assert.Equal("agent-y", command.Transfer.Destination.AgentId);
    }

    [Fact]
    public void TaskBatchSample_Deserializes_WithExpectedPayload()
    {
        var command = LoadSample("taskbatch-migration-command.json");

        Assert.Equal(MigrationScope.TaskBatch, command.MigrationScope);
        Assert.Equal("comp-30", command.Transfer.Source.ComputerId);
        Assert.Equal("agent-z", command.Transfer.Source.AgentId);
        Assert.Equal(3, command.Transfer.Source.TaskIds?.Count);
        Assert.Equal("agent-r", command.Transfer.Destination.AgentId);
    }

    private static MigrationCommand LoadSample(string fileName)
    {
        var repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../../"));
        var filePath = Path.Combine(repoRoot, "docs", "rabbitmq-messages", fileName);
        var json = string.Join(
            Environment.NewLine,
            File.ReadLines(filePath).Where(line => !line.TrimStart().StartsWith("//", StringComparison.Ordinal)));
        var command = JsonSerializer.Deserialize<MigrationCommand>(json, SerializerOptions);
        Assert.NotNull(command);
        return command!;
    }
}
