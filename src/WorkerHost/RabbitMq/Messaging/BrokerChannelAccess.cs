using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using WorkerHost.Messaging;

namespace WorkerHost.RabbitMq.Messaging;

/// <summary>
/// Synchronizes channel-level acknowledgement operations so multiple workers do not violate RabbitMQ threading rules.
/// </summary>
public sealed class BrokerChannelAccess : IBrokerChannelAccess, IDisposable
{
    private readonly SemaphoreSlim _channelGate = new(1, 1);
    private readonly ILogger<BrokerChannelAccess> _logger;
    private bool _disposed;

    /// <summary>
    /// Initializes a new channel access coordinator.
    /// </summary>
    /// <param name="logger">Logger used to capture reset warnings.</param>
    public BrokerChannelAccess(ILogger<BrokerChannelAccess> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Sends a Basic.Ack for the provided delivery while ensuring only one thread interacts with the channel at a time.
    /// </summary>
    /// <param name="delivery">Inbound message delivery to acknowledge.</param>
    /// <param name="cancellationToken">Token used to cancel the wait or broker operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when the delivery is not a RabbitMQ-backed message.</exception>
    public async Task AcknowledgeAsync(IInboundDelivery delivery, CancellationToken cancellationToken = default)
    {
        if (delivery is not MessageDelivery rabbitDelivery)
        {
            throw new InvalidOperationException("Unsupported delivery type for broker acknowledgements.");
        }

        await _channelGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await rabbitDelivery.Channel.BasicAckAsync(
                    deliveryTag: rabbitDelivery.DeliveryTag,
                    multiple: false,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            _channelGate.Release();
        }
    }

    /// <summary>
    /// Issues a Basic.Nack for the delivery, optionally requeueing it, while serializing channel access.
    /// </summary>
    /// <param name="delivery">Inbound delivery to negative-acknowledge.</param>
    /// <param name="requeue">Whether the broker should requeue the message.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Raised if the delivery is not understood by this accessor.</exception>
    public async Task RejectAsync(IInboundDelivery delivery, bool requeue, CancellationToken cancellationToken = default)
    {
        if (delivery is not MessageDelivery rabbitDelivery)
        {
            throw new InvalidOperationException("Unsupported delivery type for broker acknowledgements.");
        }

        await _channelGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await rabbitDelivery.Channel.BasicNackAsync(
                    deliveryTag: rabbitDelivery.DeliveryTag,
                    multiple: false,
                    requeue: requeue,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            _channelGate.Release();
        }
    }

    /// <summary>
    /// Forces the provided channel to close and dispose under the channel gate, preventing concurrent users from racing.
    /// </summary>
    /// <param name="channel">Channel instance to reset.</param>
    /// <param name="cancellationToken">Token used to cancel the wait.</param>
    public async Task ResetAsync(IChannel channel, CancellationToken cancellationToken = default)
    {
        await _channelGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Exception? failure = null;

            if (channel.IsOpen)
            {
                try
                {
                    await channel.CloseAsync().ConfigureAwait(false);
                }
                catch (Exception closeEx)
                {
                    failure = closeEx;
                }
            }

            try
            {
                await channel.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception disposeEx)
            {
                failure = disposeEx;
            }

            if (failure is not null)
            {
                _logger.LogWarning(failure, "Failed to reset broker channel");
            }
        }
        finally
        {
            _channelGate.Release();
        }
    }

    /// <summary>
    /// Disposes the semaphore gate guarding channel operations.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _channelGate.Dispose();
        _disposed = true;
    }
}
