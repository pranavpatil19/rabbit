using System;
using System.IO;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using WorkerHost.Configuration;
using WorkerHost.Dal;
using Xunit;

namespace WorkerHost.Tests.Dal;

public sealed class TestDataContextTests
{
    [Fact]
    public void Reload_ReplacesExistingComputers()
    {
        var tempFile = CreateTempDataFile("""
        {
          "computers": [
            {
              "id": "computer-1",
              "agents": [
                {
                  "id": "agent-a",
                  "tasks": [
                    {"id": "task-1", "status": "Pending", "payloadJson": "{}"}
                  ]
                }
              ]
            }
          ]
        }
        """);

        try
        {
            var context = CreateContext(tempFile);
            Assert.NotNull(context.GetAgent("computer-1", "agent-a"));

            var payload = new TestDataPayload
            {
                Computers = new[]
                {
                    new TestDataComputer
                    {
                        Id = "computer-2",
                        Agents = new[]
                        {
                            new TestDataAgent
                            {
                                Id = "agent-z",
                                Tasks = new[]
                                {
                                    new TestDataTask { Id = "task-9", Status = "Pending", PayloadJson = "{}" }
                                }
                            }
                        }
                    }
                }
            };

            var result = context.Reload(payload, "tests");

            Assert.Equal(1, result.Computers);
            Assert.Equal(1, result.Agents);
            Assert.Null(context.GetAgent("computer-1", "agent-a"));
            Assert.NotNull(context.GetAgent("computer-2", "agent-z"));
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public void Reload_ThrowsWhenPayloadMissingComputers()
    {
        var tempFile = CreateTempDataFile("""
        {
          "computers": []
        }
        """);

        try
        {
            var context = CreateContext(tempFile);
            var payload = new TestDataPayload();
            Assert.Throws<ArgumentException>(() => context.Reload(payload, "tests"));
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    private static TestDataContext CreateContext(string filePath)
    {
        var options = Options.Create(new TestDataOptions { DataFilePath = filePath });
        return new TestDataContext(options, NullLogger<TestDataContext>.Instance);
    }

    private static string CreateTempDataFile(string json)
    {
        var tempFile = Path.GetTempFileName();
        File.WriteAllText(tempFile, json);
        return tempFile;
    }
}
