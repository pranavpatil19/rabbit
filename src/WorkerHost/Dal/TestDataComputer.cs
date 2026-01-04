using System.Collections.Generic;

namespace WorkerHost.Dal;

public sealed class TestDataComputer
{
    public string Id { get; init; } = string.Empty;

    public IReadOnlyCollection<TestDataAgent> Agents { get; init; } = Array.Empty<TestDataAgent>();
}
