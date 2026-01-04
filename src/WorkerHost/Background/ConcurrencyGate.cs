using System;
using System.Threading;
using System.Threading.Tasks;

namespace WorkerHost.Background;

/// <summary>
/// Provides a simple wrapper around <see cref="SemaphoreSlim"/> so concurrency throttling reads fluently.
/// </summary>
internal sealed class ConcurrencyGate
{
    private readonly SemaphoreSlim _semaphore;

    public ConcurrencyGate(SemaphoreSlim semaphore)
    {
        _semaphore = semaphore ?? throw new ArgumentNullException(nameof(semaphore));
    }

    public async Task ExecuteAsync(Func<Task> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await action().ConfigureAwait(false);
        }
        finally
        {
            _semaphore.Release();
        }
    }
}
