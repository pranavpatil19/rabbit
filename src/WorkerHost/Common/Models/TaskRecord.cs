namespace WorkerHost.Common.Models;

public sealed record TaskRecord(
    string TaskId,
    string ComputerId,
    string AgentId,
    string Status,
    string PayloadJson);
