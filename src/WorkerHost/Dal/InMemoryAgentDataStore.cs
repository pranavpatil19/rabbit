using WorkerHost.Common.Models;
using WorkerHost.Messaging;

namespace WorkerHost.Dal;

public sealed class InMemoryAgentDataStore : IAgentDataStore
{
    private readonly TestDataContext _context;

    public InMemoryAgentDataStore(TestDataContext context)
    {
        _context = context;
    }

    public Task<IReadOnlyCollection<AgentInfo>> GetAgentsByComputerAsync(string computerId, IReadOnlyCollection<string>? includeAgentIds, CancellationToken cancellationToken)
    {
        var agents = _context.GetAgents(computerId, includeAgentIds);
        return Task.FromResult(agents);
    }

    public Task<AgentInfo?> GetAgentAsync(string computerId, string agentId, CancellationToken cancellationToken)
    {
        var agent = _context.GetAgent(computerId, agentId);
        return Task.FromResult(agent);
    }

    public Task LockAgentAsync(string computerId, string agentId, CancellationToken cancellationToken)
    {
        _context.LockAgent(computerId, agentId);
        return Task.CompletedTask;
    }

    public Task UnlockAgentAsync(string computerId, string agentId, CancellationToken cancellationToken)
    {
        _context.UnlockAgent(computerId, agentId);
        return Task.CompletedTask;
    }

    public Task<AgentInfo> EnsureDestinationAgentAsync(string computerId, string agentId, CancellationToken cancellationToken)
    {
        var agent = _context.EnsureDestinationAgent(computerId, agentId);
        return Task.FromResult(agent);
    }

    public Task ValidateAgentTransferAsync(AgentInfo sourceAgent, AgentInfo destinationAgent, CancellationToken cancellationToken)
    {
        TestDataContext.ValidateTransfer(sourceAgent, destinationAgent);
        return Task.CompletedTask;
    }
}
