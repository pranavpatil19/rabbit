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

    /// <summary>Initializes a new instance of the <see cref="ConcurrencyGate"/> class.</summary>
    /// <param name="semaphore">Semaphore that enforces concurrency.</param>
    public ConcurrencyGate(SemaphoreSlim semaphore)
    {
        _semaphore = semaphore ?? throw new ArgumentNullException(nameof(semaphore));
    }

    /// <summary>
    /// Executes the provided asynchronous action while holding the semaphore.
    /// </summary>
    /// <param name="action">Operation to run under the concurrency gate.</param>
    /// <param name="cancellationToken">Token used to cancel waiting or execution.</param>
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
