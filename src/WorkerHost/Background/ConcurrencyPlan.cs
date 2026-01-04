namespace WorkerHost.Background;

/// <summary>
/// Simple concurrency plan with a global semaphore and worker count. No per-scope throttling.
/// </summary>
public sealed class ConcurrencyPlan : IAsyncDisposable
{
    /// <summary>
    /// Creates a concurrency plan using the configured default and max concurrency.
    /// </summary>
    public ConcurrencyPlan(int defaultConcurrency, int maxConcurrency)
    {
        var boundedDefault = Math.Clamp(defaultConcurrency, 1, maxConcurrency);
        WorkerCount = boundedDefault;
        GlobalSemaphore = new SemaphoreSlim(boundedDefault, maxConcurrency);
    }

    /// <summary>Semaphore that caps total concurrent handlers.</summary>
    public SemaphoreSlim GlobalSemaphore { get; }

    /// <summary>Number of workers to spin up for handling.</summary>
    public int WorkerCount { get; }

    /// <summary>Dispose the semaphore created by this plan.</summary>
    public ValueTask DisposeAsync()
    {
        GlobalSemaphore.Dispose();
        return ValueTask.CompletedTask;
    }
}
