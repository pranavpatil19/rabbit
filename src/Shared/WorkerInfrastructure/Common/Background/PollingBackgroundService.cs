using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace WorkerHost.Common.Background;

/// <summary>
/// Template for services that repeatedly poll work, providing consistent cancellation + idle-delay behaviour.
/// </summary>
public abstract class PollingBackgroundService : BackgroundService
{
    private readonly ILogger _logger;
    private readonly TimeSpan _idleDelay;

    protected PollingBackgroundService(ILogger logger, TimeSpan idleDelay)
    {
        _logger = logger;
        _idleDelay = idleDelay < TimeSpan.Zero ? TimeSpan.Zero : idleDelay;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var suppressDelay = false;
            try
            {
                suppressDelay = await ExecuteIterationAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex) when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogWarning(ex, "{Service} stopping due to cancellation", GetType().Name);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{Service} iteration failed", GetType().Name);
            }

            if (!suppressDelay && _idleDelay > TimeSpan.Zero)
            {
                try
                {
                    await Task.Delay(_idleDelay, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException ex) when (stoppingToken.IsCancellationRequested)
                {
                    _logger.LogWarning(ex, "{Service} delay cancelled during shutdown", GetType().Name);
                    break;
                }
            }
        }
    }

    /// <summary>
    /// Performs a unit of work. Return true if the iteration has already waited and no idle delay should be applied.
    /// </summary>
    protected abstract Task<bool> ExecuteIterationAsync(CancellationToken cancellationToken);
}
