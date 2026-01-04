namespace WorkerHost.Common.Models;

public sealed record AgentInfo(
    string ComputerId,
    string AgentId,
    bool IsLocked,
    int TaskCount);
