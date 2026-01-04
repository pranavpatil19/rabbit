using System;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using RabbitMQ.Client;

using WorkerHost.RabbitMq.Channels;
using WorkerHost.RabbitMq.Infrastructure;

namespace WorkerHost.RabbitMq.Listener;

/// <summary>
/// Encapsulates connection/channel lifecycle for the message listener so the outer type can focus on buffering.
/// </summary>
internal sealed class ListenerChannelManager : IAsyncDisposable
{
    private readonly SemaphoreSlim _channelLock = new(1, 1);
    private readonly IConnectionFactory _connectionFactory;
    private readonly QueueBindingsManager _bindingsManager;
    private readonly ILogger _logger;
    private IConnection? _connection;
    private IChannel? _channel;

    public ListenerChannelManager(
        IConnectionFactory connectionFactory,
        QueueBindingsManager bindingsManager,
        ILogger logger)
    {
        _connectionFactory = connectionFactory;
        _bindingsManager = bindingsManager;
        _logger = logger;
    }

    /// <summary>
    /// Returns an open channel, creating (and declaring infrastructure) if the current one was closed.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the wait when the listener is shutting down.</param>
    /// <returns>Existing or newly created channel ready for consumption.</returns>
    public async Task<IChannel> GetOrCreateChannelAsync(CancellationToken cancellationToken)
    {
        await _channelLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_channel is { IsOpen: true })
            {
                return _channel;
            }

            await ResetCoreAsync().ConfigureAwait(false);

            _connection = await _connectionFactory.CreateConnectionAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            _channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            await _bindingsManager.EnsureAsync(_channel, cancellationToken).ConfigureAwait(false);
            return _channel;
        }
        finally
        {
            _channelLock.Release();
        }
    }

    /// <summary>
    /// Closes and disposes the active connection/channel under lock so the listener can reconnect cleanly.
    /// </summary>
    public async Task ResetAsync()
    {
        await _channelLock.WaitAsync().ConfigureAwait(false);
        try
        {
            await ResetCoreAsync().ConfigureAwait(false);
        }
        finally
        {
            _channelLock.Release();
        }
    }

    private async Task ResetCoreAsync()
    {
        if (_channel is not null)
        {
            await BrokerResourceCleaner.CloseAndDisposeChannelAsync(_channel, _logger, "ListenerChannelManager").ConfigureAwait(false);
            _channel = null;
        }

        if (_connection is not null)
        {
            await BrokerResourceCleaner.CloseAndDisposeConnectionAsync(_connection, _logger, "ListenerChannelManager").ConfigureAwait(false);
            _connection = null;
        }
    }

    /// <summary>
    /// Releases all managed resources by resetting the channel and disposing the internal lock.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await ResetAsync().ConfigureAwait(false);
        _channelLock.Dispose();
    }
}
