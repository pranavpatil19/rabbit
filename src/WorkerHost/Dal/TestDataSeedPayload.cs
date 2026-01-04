using System;
using System.Collections.Generic;

namespace WorkerHost.Dal;

public sealed class TestDataPayload
{
    public IReadOnlyCollection<TestDataComputer> Computers { get; init; } = Array.Empty<TestDataComputer>();
}

public sealed class TestDataComputer
{
    public string Id { get; init; } = string.Empty;
    public IReadOnlyCollection<TestDataAgent> Agents { get; init; } = Array.Empty<TestDataAgent>();
}

public sealed class TestDataAgent
{
    public string Id { get; init; } = string.Empty;
    public IReadOnlyCollection<TestDataTask> Tasks { get; init; } = Array.Empty<TestDataTask>();
}

public sealed class TestDataTask
{
    public string Id { get; init; } = string.Empty;
    public string? Status { get; init; }
    public string? PayloadJson { get; init; }
}
