namespace WorkerHost.Dal;

public sealed class TestDataTask
{
    public string Id { get; init; } = string.Empty;

    public string? Status { get; init; }

    public string? PayloadJson { get; init; }
}
