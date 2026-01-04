namespace WorkerHost.Configuration;

public sealed record TestDataOptions
{
    public string DataFilePath { get; init; } = "testdata/sample_data.json";
}
