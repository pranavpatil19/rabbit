using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using WorkerHost.Common.Background;
using WorkerHost.Messaging;
using WorkerHost.RabbitMq.Listener;

namespace WorkerHost.Background;

/// <summary>
/// Pulls deliveries from RabbitMQ and pushes them into the shared delivery buffer.
/// </summary>
public sealed class RabbitDeliveryIngestService : PollingBackgroundService
{
    private readonly IMessageListener _messageListener;
    private readonly DeliveryQueue _deliveryQueue;
    private readonly ILogger<RabbitDeliveryIngestService> _logger;
    private IAsyncEnumerator<IInboundDelivery>? _inboundStream;

    public RabbitDeliveryIngestService(
        IMessageListener messageListener,
        DeliveryQueue deliveryQueue,
        ILogger<RabbitDeliveryIngestService> logger)
        : base(logger, TimeSpan.Zero)
    {
        _messageListener = messageListener;
        _deliveryQueue = deliveryQueue;
        _logger = logger;
    }

    protected override async Task<bool> ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        _inboundStream ??= _messageListener.ReadAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
        IInboundDelivery delivery;
        try
        {
            if (!await _inboundStream.MoveNextAsync().ConfigureAwait(false))
            {
                return false;
            }

            delivery = _inboundStream.Current;
        }
        catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogDebug(ex, "Listener pump cancelled by token.");
            return false;
        }

        try
        {
            await _deliveryQueue.EnqueueAsync(delivery, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (ChannelClosedException ex)
        {
            _logger.LogWarning(ex, "Delivery queue closed while pumping messages; shutting down listener pump.");
            return false;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _deliveryQueue.Complete();

        if (_inboundStream is not null)
        {
            await _inboundStream.DisposeAsync().ConfigureAwait(false);
        }

        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }
}
