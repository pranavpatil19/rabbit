using System.Collections.Concurrent;
using RabbitMQ.Client;
using Microsoft.Extensions.Logging;
using WorkerHost.RabbitMq.Infrastructure;

namespace WorkerHost.RabbitMq.Messaging;

/// <summary>
/// Keeps a small pool of open RabbitMQ channels so publishing stays fast without repeating setup.
/// One connection/channel wrapper is reused until it goes stale or the pool is full.
/// </summary>
internal sealed class PublisherChannelPool : IAsyncDisposable
{
#region Fields
    // Dependencies
    private readonly IConnectionFactory _connectionFactory; // Builds broker connections/channels.
    private readonly QueueBindingsManager _bindingsManager; // Ensures exchanges/queues exist before publish.
    private readonly ILogger _logger; // Logs channel/connection lifecycle issues.

    // Pool state
    private readonly ConcurrentBag<PooledChannel> _pool = new(); // Reusable channels.
    private readonly int _maxPoolSize; // Upper bound for pooled entries.
#endregion

#region Constructor
    /// <summary>
    /// Creates a publisher channel pool with the configured max size and binding manager.
    /// </summary>
    public PublisherChannelPool(
        IConnectionFactory connectionFactory,
        QueueBindingsManager bindingsManager,
        int maxPoolSize,
        ILogger logger)
    {
        _connectionFactory = connectionFactory;
        _bindingsManager = bindingsManager;
        _maxPoolSize = maxPoolSize;
        _logger = logger;
    }
#endregion

#region Channel access
    /// <summary>
    /// Returns an open channel from the pool or builds a fresh connection/channel when none are available.
    /// </summary>
    public async ValueTask<PooledChannel> GetChannelAsync(CancellationToken cancellationToken)
    {
        while (_pool.TryTake(out var pooled))
        {
            if (pooled.IsOpen)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug("PublisherChannelPool: reusing open channel from pool");
                }
                return pooled;
            }

            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("PublisherChannelPool: discarding stale channel");
            }
            await pooled.DisposeAsync().ConfigureAwait(false);
        }

        // New connection/channel when pool is empty or entries are stale.
        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("PublisherChannelPool: creating new connection/channel");
        }
        var connection = await _connectionFactory.CreateConnectionAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        await _bindingsManager.EnsureAsync(channel, cancellationToken).ConfigureAwait(false);
        return new PooledChannel(connection, channel, _logger);
    }

    /// <summary>
    /// Returns a healthy channel to the pool or disposes it if stale or pool is full.
    /// </summary>
    public async ValueTask ReleaseChannelAsync(PooledChannel pooled)
    {
        // Dispose closed/stale entries or when the pool is at capacity.
        if (!pooled.IsOpen || _pool.Count >= _maxPoolSize)
        {
            await pooled.DisposeAsync().ConfigureAwait(false);
            return;
        }

        _pool.Add(pooled);
    }

    public async ValueTask DisposeAsync()
    {
        while (_pool.TryTake(out var pooled))
        {
            await pooled.DisposeAsync().ConfigureAwait(false);
        }
    }
#endregion

    /// <summary>
    /// Wrapper for an open connection/channel pair that can be returned to the pool.
    /// </summary>
    internal sealed class PooledChannel : IAsyncDisposable
    {
        private readonly ILogger _logger;

        /// <summary>Constructs a pooled channel wrapper.</summary>
        public PooledChannel(IConnection connection, IChannel channel, ILogger logger)
        {
            Connection = connection;
            Channel = channel;
            _logger = logger;
        }

        /// <summary>Underlying AMQP connection.</summary>
        public IConnection Connection { get; }
        /// <summary>Underlying AMQP channel.</summary>
        public IChannel Channel { get; }

        /// <summary>True when both connection and channel remain open.</summary>
        public bool IsOpen => Connection.IsOpen && Channel.IsOpen;

        /// <summary>Closes and disposes the channel/connection, logging failures.</summary>
        public async ValueTask DisposeAsync()
        {
            await BrokerResourceCleaner.CloseAndDisposeChannelAsync(Channel, _logger, "PublisherChannelPool").ConfigureAwait(false);
            await BrokerResourceCleaner.CloseAndDisposeConnectionAsync(Connection, _logger, "PublisherChannelPool").ConfigureAwait(false);
        }

    }
}
