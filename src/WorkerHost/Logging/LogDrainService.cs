using Microsoft.Extensions.Logging;
using WorkerHost.Common.Background;

namespace WorkerHost.Logging;

public sealed class LogDrainService : PollingBackgroundService
{
    private readonly ILogQueue _queue;
    private readonly ILoggerFactory _loggerFactory;

    public LogDrainService(
        ILogQueue queue,
        ILoggerFactory loggerFactory,
        ILogger<LogDrainService> logger)
        : base(logger, TimeSpan.Zero)
    {
        _queue = queue;
        _loggerFactory = loggerFactory;
    }

    protected override async Task<bool> ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        var logEvent = await _queue.ReadNextAsync(cancellationToken).ConfigureAwait(false);
        if (logEvent is null)
        {
            return false;
        }

        var logger = _loggerFactory.CreateLogger(logEvent.Category);
        IDisposable? scope = null;
        if (logEvent.Properties is not null && logEvent.Properties.Count > 0)
        {
            scope = logger.BeginScope(logEvent.Properties);
        }

        try
        {
            logger.Log(
                logEvent.Level,
                logEvent.Exception,
                logEvent.MessageTemplate,
                logEvent.Args);
        }
        finally
        {
            scope?.Dispose();
        }

        return true;
    }
}
