using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using WorkerHost.Messaging;
using WorkerHost.RabbitMq.Configuration;
using WorkerHost.RabbitMq.Infrastructure;

namespace WorkerHost.RabbitMq.Messaging;

public interface IMessageListener : IAsyncDisposable
{
    IAsyncEnumerable<IInboundDelivery> ReadAsync(CancellationToken cancellationToken);
}

public readonly record struct MessageDelivery(IChannel Channel, ulong DeliveryTag, ReadOnlyMemory<byte> Body) : IInboundDelivery;

/// <summary>
/// Maintains a single broker connection/channel and exposes an async stream of deliveries for the worker to consume.
/// </summary>
internal sealed class MessageListener : IMessageListener
{
    private readonly ILogger<MessageListener> _logger;
    private readonly Channel<IInboundDelivery> _deliveryChannel;
    private readonly object _listenerLoopLock = new();
    private readonly int _prefetchCount;
    private readonly string _queueName;
    private readonly ListenerChannelManager _channelManager;
    private Task? _listenerLoopTask;
    private CancellationTokenSource? _listenerLoopCts;

    /// <summary>
    /// Creates a listener that will consume from the configured queue.
    /// </summary>
    public MessageListener(
        IOptions<BrokerOptions> config,
        ILogger<MessageListener> logger)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(logger);

        _logger = logger;
        var settings = config.Value;

        _prefetchCount = CalculatePrefetch(settings);
        _deliveryChannel = CreateDeliveryChannel(_prefetchCount);

        var connectionFactory = ConnectionFactoryBuilder.Create(
            settings,
            $"{settings.Connection.ClientProvidedName}.consumer");
        var bindingsManager = new QueueBindingsManager(settings);
        _queueName = bindingsManager.QueueName;
        _channelManager = new ListenerChannelManager(connectionFactory, bindingsManager, logger);

        LogConfiguration(settings);
    }

    /// <summary>
    /// Streams deliveries from RabbitMQ until the cancellation token is triggered.
    /// </summary>
    public async IAsyncEnumerable<IInboundDelivery> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        EnsureListenerLoopStarted();

        while (await _deliveryChannel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (_deliveryChannel.Reader.TryRead(out var delivery))
            {
                yield return delivery;
            }
        }
    }

    /// <summary>
    /// Lazily spins up the background consumer so we only start pulling when someone is reading.
    /// </summary>
    private void EnsureListenerLoopStarted()
    {
        if (_listenerLoopTask is { IsCompleted: false })
        {
            return;
        }

        lock (_listenerLoopLock)
        {
            if (_listenerLoopTask is { IsCompleted: false })
            {
                return;
            }

            _listenerLoopCts?.Cancel();
            _listenerLoopCts?.Dispose();
            _listenerLoopCts = new CancellationTokenSource(); // fresh CTS per run so dispose can cancel it.
            _listenerLoopTask = Task.Run(() => RunConsumerLoopAsync(_listenerLoopCts.Token), CancellationToken.None);
        }
    }

    private int CalculatePrefetch(BrokerOptions settings)
    {
        var clamped = Math.Clamp(settings.DefaultConcurrency, 1, settings.MaxConcurrency);
        if (clamped != settings.DefaultConcurrency)
        {
            _logger.LogWarning(
                "Prefetch clamped from {Configured} to {Prefetch} based on MaxConcurrency {MaxConcurrency}",
                settings.DefaultConcurrency,
                clamped,
                settings.MaxConcurrency);
        }

        return clamped;
    }

    private static Channel<IInboundDelivery> CreateDeliveryChannel(int prefetch)
        => Channel.CreateBounded<IInboundDelivery>(new BoundedChannelOptions(prefetch * 4)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });

    private void LogConfiguration(BrokerOptions settings)
    {
        _logger.LogInformation(
            "MessageListener configured: prefetchCount={Prefetch}, defaultConcurrency={DefaultConcurrency}, maxConcurrency={MaxConcurrency}",
            _prefetchCount,
            settings.DefaultConcurrency,
            settings.MaxConcurrency);
    }

    /// <summary>
    /// Core consumer loop: registers an AsyncEventingBasicConsumer, processes shutdown, and retries on faults.
    /// </summary>
    private async Task RunConsumerLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            AsyncEventingBasicConsumer? consumer = null;
            AsyncEventHandler<ShutdownEventArgs>? shutdownHandler = null;
            try
            {
                var channel = await _channelManager.GetOrCreateChannelAsync(cancellationToken).ConfigureAwait(false); // ensure connection/channel exist
                await channel.BasicQosAsync(
                        prefetchSize: 0,
                        prefetchCount: (ushort)_prefetchCount,
                        global: false,
                        cancellationToken: cancellationToken)
                    .ConfigureAwait(false);

                consumer = new AsyncEventingBasicConsumer(channel); // rabbitmq client helper that raises events for deliveries
                consumer.ReceivedAsync += OnMessageReceivedAsync; // push deliveries into the internal channel

                var shutdownTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously); // signals when RabbitMQ shuts the consumer down
                Task HandleShutdownAsync(object? _, ShutdownEventArgs args)
                {
                    _logger.LogWarning(args.Exception, "Message consumer shutdown: {ReplyText}", args.ReplyText);

                    shutdownTcs.TrySetResult();
                    return Task.CompletedTask;
                }

                shutdownHandler = HandleShutdownAsync;
                consumer.ShutdownAsync += shutdownHandler;

                await channel.BasicConsumeAsync(
                        queue: _queueName,
                        autoAck: false,
                        consumer: consumer,
                        cancellationToken: cancellationToken)
                    .ConfigureAwait(false);

                await Task.WhenAny(Task.Delay(Timeout.Infinite, cancellationToken), shutdownTcs.Task) // wait until cancellation or shutdown
                    .ConfigureAwait(false);

                consumer.ReceivedAsync -= OnMessageReceivedAsync;
                if (shutdownHandler is not null)
                {
                    consumer.ShutdownAsync -= shutdownHandler;
                }
                await _channelManager.ResetAsync().ConfigureAwait(false); // reconnect before retrying
            }
            catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogWarning(ex, "MessageListener cancellation requested; stopping consumer loop");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Message consumer faulted; restarting.");
                if (consumer is not null)
                {
                    consumer.ReceivedAsync -= OnMessageReceivedAsync;
                    if (shutdownHandler is not null)
                    {
                        consumer.ShutdownAsync -= shutdownHandler;
                    }
                }

                await _channelManager.ResetAsync().ConfigureAwait(false);
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false); // small backoff before retrying
            }
        }

        _deliveryChannel.Writer.TryComplete();
    }

    /// <summary>
    /// Forwards a RabbitMQ delivery into the bounded channel used by QueueWorker.
    /// </summary>
    private async Task OnMessageReceivedAsync(object? sender, BasicDeliverEventArgs args)
    {
        var consumer = (AsyncEventingBasicConsumer)sender!;
        var delivery = new MessageDelivery(consumer.Channel, args.DeliveryTag, args.Body);
        await _deliveryChannel.Writer.WriteAsync(delivery).ConfigureAwait(false);
    }

    /// <summary>
    /// Stops the listener loop and disposes its channel resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_listenerLoopCts is not null)
        {
            await _listenerLoopCts.CancelAsync().ConfigureAwait(false);
            try
            {
                if (_listenerLoopTask is not null)
                {
                    await _listenerLoopTask.ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException ex)
            {
                _logger.LogWarning(ex, "MessageListener dispose cancelled listener loop task");
            }

            _listenerLoopCts.Dispose();
        }

        _deliveryChannel.Writer.TryComplete();
        await _channelManager.DisposeAsync().ConfigureAwait(false);
    }
}
