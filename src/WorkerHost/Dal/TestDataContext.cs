using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WorkerHost.Common.Models;
using WorkerHost.Configuration;
using WorkerHost.Messaging;

namespace WorkerHost.Dal;

public sealed class TestDataContext
{
    private readonly ConcurrentDictionary<string, ComputerState> _computers = new(StringComparer.OrdinalIgnoreCase);
    private readonly ILogger<TestDataContext> _logger;
    private readonly object _reloadLock = new();

    public TestDataContext(IOptions<TestDataOptions> options, ILogger<TestDataContext> logger)
    {
        _logger = logger;
        var path = options.Value.DataFilePath;
        if (!Path.IsPathRooted(path))
        {
            path = Path.Combine(AppContext.BaseDirectory, path);
        }

        if (!File.Exists(path))
        {
            _logger.LogWarning("Test data file {Path} not found; starting empty", path);
            return;
        }

        using var stream = File.OpenRead(path);
        var payload = DeserializePayload(stream);
        if (payload?.Computers == null || payload.Computers.Count == 0)
        {
            _logger.LogWarning("Test data file {Path} had no computers", path);
            return;
        }

        ApplyPayload(payload.Computers, path, replaceExisting: true);
    }

    public (int Computers, int Agents) Reload(TestDataPayload payload, string source)
    {
        if (payload?.Computers == null || payload.Computers.Count == 0)
        {
            throw new ArgumentException("Test data payload must contain at least one computer.", nameof(payload));
        }

        return ApplyPayload(payload.Computers, source, replaceExisting: true);
    }

    public IReadOnlyCollection<AgentInfo> GetAgents(string computerId, IReadOnlyCollection<string>? includeAgentIds)
    {
        if (!_computers.TryGetValue(computerId, out var computer))
        {
            return Array.Empty<AgentInfo>();
        }

        var query = computer.Agents.Values.AsEnumerable();
        if (includeAgentIds is { Count: > 0 })
        {
            var set = includeAgentIds.Select(id => id.ToLowerInvariant()).ToHashSet();
            query = query.Where(agent => set.Contains(agent.AgentId.ToLowerInvariant()));
        }

        return query
            .Select(agent => new AgentInfo(computer.Id, agent.AgentId, agent.IsLocked, agent.Tasks.Count))
            .ToArray();
    }

    public AgentInfo? GetAgent(string computerId, string agentId)
    {
        if (!_computers.TryGetValue(computerId, out var computer))
        {
            return null;
        }

        if (!computer.Agents.TryGetValue(agentId, out var agent))
        {
            return null;
        }

        return new AgentInfo(computerId, agent.AgentId, agent.IsLocked, agent.Tasks.Count);
    }

    public AgentInfo EnsureDestinationAgent(string computerId, string agentId)
    {
        var computer = _computers.GetOrAdd(computerId, id => new ComputerState(id));
        var agent = computer.Agents.GetOrAdd(agentId, id => new AgentState(id, computerId, Enumerable.Empty<TaskRecord>()));
        return new AgentInfo(computerId, agent.AgentId, agent.IsLocked, agent.Tasks.Count);
    }

    public void LockAgent(string computerId, string agentId)
    {
        var agent = RequireAgent(computerId, agentId);
        agent.Lock();
    }

    public void UnlockAgent(string computerId, string agentId)
    {
        if (_computers.TryGetValue(computerId, out var computer) && computer.Agents.TryGetValue(agentId, out var agent))
        {
            agent.Unlock();
        }
    }

    public IEnumerable<TaskRecord> GetAgentTasks(string computerId, string agentId, TaskFilter? filter)
    {
        var agent = RequireAgent(computerId, agentId);
        return ApplyFilter(agent.Tasks, filter);
    }

    public IEnumerable<TaskRecord> GetTaskBatch(string computerId, string agentId, IReadOnlyCollection<string> taskIds)
    {
        var agent = RequireAgent(computerId, agentId);
        var idSet = taskIds.ToHashSet(StringComparer.OrdinalIgnoreCase);
        return agent.Tasks.Where(t => idSet.Contains(t.TaskId));
    }

    public void InsertTasks(string computerId, string agentId, IReadOnlyCollection<TaskRecord> tasks)
    {
        var agent = RequireAgent(computerId, agentId);
        foreach (var task in tasks)
        {
            agent.AddTask(task);
        }
    }

    public void RemoveTasks(string computerId, string agentId, IEnumerable<string> taskIds)
    {
        var agent = RequireAgent(computerId, agentId);
        agent.RemoveTasks(taskIds);
    }

    public static void ValidateTransfer(AgentInfo source, AgentInfo destination)
    {
        if (destination.IsLocked)
        {
            throw new InvalidOperationException($"Destination agent {destination.AgentId} is locked");
        }
    }

    private AgentState RequireAgent(string computerId, string agentId)
    {
        if (!_computers.TryGetValue(computerId, out var computer))
        {
            throw new InvalidOperationException($"Computer {computerId} not found");
        }

        if (!computer.Agents.TryGetValue(agentId, out var agent))
        {
            throw new InvalidOperationException($"Agent {agentId} not found on computer {computerId}");
        }

        return agent;
    }

    private static IEnumerable<TaskRecord> ApplyFilter(IEnumerable<TaskRecord> source, TaskFilter? filter)
    {
        var query = source;
        if (filter is null)
        {
            return query;
        }

        if (!string.IsNullOrWhiteSpace(filter.Status))
        {
            query = query.Where(t => string.Equals(t.Status, filter.Status, StringComparison.OrdinalIgnoreCase));
        }

        if (!string.IsNullOrWhiteSpace(filter.TaskType))
        {
            query = query.Where(t => t.PayloadJson.Contains(filter.TaskType, StringComparison.OrdinalIgnoreCase));
        }

        return query;
    }

    private static TestDataPayload? DeserializePayload(Stream stream)
    {
        return JsonSerializer.Deserialize<TestDataPayload>(stream, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
        });
    }

    private (int Computers, int Agents) ApplyPayload(IReadOnlyCollection<TestDataComputer> computers, string source, bool replaceExisting)
    {
        var snapshot = new Dictionary<string, ComputerState>(StringComparer.OrdinalIgnoreCase);
        var agentCount = 0;

        foreach (var computer in computers)
        {
            if (string.IsNullOrWhiteSpace(computer.Id))
            {
                continue;
            }

            var state = new ComputerState(computer.Id);
            if (computer.Agents != null)
            {
                foreach (var agent in computer.Agents)
                {
                    if (string.IsNullOrWhiteSpace(agent.Id))
                    {
                        continue;
                    }

                    agentCount++;
                    var tasks = (agent.Tasks ?? Array.Empty<TestDataTask>())
                        .Select(t => new TaskRecord(
                            string.IsNullOrWhiteSpace(t.Id) ? Guid.NewGuid().ToString("N") : t.Id,
                            computer.Id,
                            agent.Id,
                            string.IsNullOrWhiteSpace(t.Status) ? "Unknown" : t.Status,
                            string.IsNullOrWhiteSpace(t.PayloadJson) ? "{}" : t.PayloadJson));

                    state.Agents[agent.Id] = new AgentState(agent.Id, computer.Id, tasks);
                }
            }

            snapshot[computer.Id] = state;
        }

        lock (_reloadLock)
        {
            if (replaceExisting)
            {
                _computers.Clear();
            }

            foreach (var kvp in snapshot)
            {
                _computers[kvp.Key] = kvp.Value;
            }
        }

        _logger.LogInformation("Loaded {ComputerCount} computers and {AgentCount} agents from test data {Source}",
            snapshot.Count,
            agentCount,
            source);

        return (snapshot.Count, agentCount);
    }

    private sealed class ComputerState
    {
        public ComputerState(string id)
        {
            Id = id;
        }

        public string Id { get; }
        public ConcurrentDictionary<string, AgentState> Agents { get; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private sealed class AgentState
    {
        private int _locked;
        private readonly object _sync = new();

        public AgentState(string agentId, string computerId, IEnumerable<TaskRecord> tasks)
        {
            AgentId = agentId;
            ComputerId = computerId;
            Tasks = new List<TaskRecord>(tasks);
        }

        public string AgentId { get; }
        public string ComputerId { get; }
        public List<TaskRecord> Tasks { get; }
        public bool IsLocked => Interlocked.CompareExchange(ref _locked, 0, 0) == 1;

        public void Lock()
        {
            if (Interlocked.Exchange(ref _locked, 1) == 1)
            {
                throw new InvalidOperationException($"Agent {AgentId} already locked");
            }
        }

        public void Unlock() => Interlocked.Exchange(ref _locked, 0);

        public void AddTask(TaskRecord task)
        {
            lock (_sync)
            {
                Tasks.RemoveAll(t => string.Equals(t.TaskId, task.TaskId, StringComparison.OrdinalIgnoreCase));
                Tasks.Add(task);
            }
        }

        public void RemoveTasks(IEnumerable<string> taskIds)
        {
            var ids = taskIds.ToHashSet(StringComparer.OrdinalIgnoreCase);
            lock (_sync)
            {
                Tasks.RemoveAll(t => ids.Contains(t.TaskId));
            }
        }
    }
}
