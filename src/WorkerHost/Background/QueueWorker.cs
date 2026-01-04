using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using WorkerHost.Messaging;
using WorkerHost.RabbitMq.Configuration;

namespace WorkerHost.Background;

/// <summary>
/// Worker pool that drains the shared <see cref="DeliveryQueue"/> and dispatches messages via <see cref="DeliveryProcessor"/>.
/// </summary>
public sealed class QueueWorker : BackgroundService
{
    private readonly ILogger<QueueWorker> _logger;
    private readonly DeliveryQueue _deliveryQueue;
    private readonly DeliveryProcessor _deliveryProcessor;
    private readonly ConcurrencyPlan _concurrencyPlan;
    private readonly ConcurrencyGate _concurrencyGate;
    private readonly int _workerCount;
    private Task[]? _workerTaskPool;

    public QueueWorker(
        IOptions<BrokerOptions> workerConfig,
        ILogger<QueueWorker> logger,
        DeliveryQueue deliveryQueue,
        DeliveryProcessor deliveryProcessor)
    {
        var config = workerConfig.Value;
        _logger = logger;
        _deliveryQueue = deliveryQueue;
        _deliveryProcessor = deliveryProcessor;

        _concurrencyPlan = new ConcurrencyPlan(
            config.DefaultConcurrency,
            config.MaxConcurrency);
        _concurrencyGate = new ConcurrencyGate(_concurrencyPlan.GlobalSemaphore);
        _workerCount = _concurrencyPlan.WorkerCount;

        _logger.LogInformation(
            "QueueWorker configured: workers={WorkerCount}, defaultConcurrency={DefaultConcurrency}, maxConcurrency={MaxConcurrency}",
            _workerCount,
            config.DefaultConcurrency,
            config.MaxConcurrency);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("QueueWorker starting worker pool.");

        _workerTaskPool = Enumerable.Range(0, _workerCount)
            .Select(_ => Task.Run(() => RunWorkerLoopAsync(stoppingToken), CancellationToken.None))
            .ToArray();

        try
        {
            await Task.WhenAll(_workerTaskPool).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogDebug(ex, "QueueWorker worker tasks cancelled.");
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("QueueWorker stopping.");
        await base.StopAsync(cancellationToken).ConfigureAwait(false);

        if (_workerTaskPool is not null)
        {
            try
            {
                await Task.WhenAll(_workerTaskPool).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex)
            {
                _logger.LogDebug(ex, "Worker task pool cancelled during shutdown.");
            }
        }

        await _concurrencyPlan.DisposeAsync().ConfigureAwait(false);
    }

    private async Task RunWorkerLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var delivery in _deliveryQueue.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                await ProcessDeliveryAsync(delivery, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogDebug(ex, "QueueWorker worker loop cancelled.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected failure in QueueWorker worker loop.");
        }
    }

    private Task ProcessDeliveryAsync(IInboundDelivery delivery, CancellationToken cancellationToken) =>
        _concurrencyGate.ExecuteAsync(() => _deliveryProcessor.ProcessAsync(delivery, cancellationToken), cancellationToken);
}
