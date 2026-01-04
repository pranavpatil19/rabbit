namespace WorkerHost.Logging;

/// <summary>
/// Central store for logging templates related to migration publishing and processing.
/// </summary>
public static class MigrationLogTemplates
{
    public const string PublishSuccess =
        "Queued migration {MigrationId} ({Scope}) priority {Priority} in {ElapsedMs:F2} ms (payload {PayloadBytes} bytes)";

    public const string WorkerSuccess =
        "Migration message processed successfully in {ElapsedMs:F2} ms (payload {PayloadBytes} bytes)";

    public const string WorkerFailure =
        "Migration message failed after {ElapsedMs:F2} ms: {Error}";

    public const string WorkerAck = "Acked migration {MigrationId}";

    public const string WorkerNack = "Nacked migration {MigrationId}. Requeue={Requeue}";
}
