using System.Collections.Generic;

namespace WorkerHost.Dal;

public sealed class TestDataAgent
{
    public string Id { get; init; } = string.Empty;

    public IReadOnlyCollection<TestDataTask> Tasks { get; init; } = Array.Empty<TestDataTask>();
}
