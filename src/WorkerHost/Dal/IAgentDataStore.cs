using WorkerHost.Common.Models;
using WorkerHost.Messaging;

namespace WorkerHost.Dal;

public interface IAgentDataStore
{
    Task<IReadOnlyCollection<AgentInfo>> GetAgentsByComputerAsync(
        string computerId,
        IReadOnlyCollection<string>? includeAgentIds,
        CancellationToken cancellationToken);

    Task<AgentInfo?> GetAgentAsync(string computerId, string agentId, CancellationToken cancellationToken);

    Task LockAgentAsync(string computerId, string agentId, CancellationToken cancellationToken);

    Task UnlockAgentAsync(string computerId, string agentId, CancellationToken cancellationToken);

    Task<AgentInfo> EnsureDestinationAgentAsync(string computerId, string agentId, CancellationToken cancellationToken);

    Task ValidateAgentTransferAsync(AgentInfo sourceAgent, AgentInfo destinationAgent, CancellationToken cancellationToken);
}
