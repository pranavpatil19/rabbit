namespace WorkerHost.Messaging;

/// <summary>
/// Identifies which type of migration the command represents.
/// </summary>
public enum MigrationScope
{
    Computer,
    Agent,
    TaskBatch,
}
