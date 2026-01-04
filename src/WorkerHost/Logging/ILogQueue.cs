using System.Collections.Generic;

namespace WorkerHost.Logging;

public interface ILogQueue
{
    ValueTask EnqueueAsync(MigrationLogEvent logEvent, CancellationToken cancellationToken = default);

    IAsyncEnumerable<MigrationLogEvent> ReadAllAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Retrieves the next available log event, awaiting data when necessary.
    /// </summary>
    ValueTask<MigrationLogEvent?> ReadNextAsync(CancellationToken cancellationToken);
}
