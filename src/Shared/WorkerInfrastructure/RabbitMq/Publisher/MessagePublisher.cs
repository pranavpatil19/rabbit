using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using WorkerHost.Messaging;
using WorkerHost.Logging;
using WorkerHost.RabbitMq.Configuration;
using WorkerHost.RabbitMq.Infrastructure;

namespace WorkerHost.RabbitMq.Publisher;

/// <summary>
/// Publishes migration messages to the broker using the async client APIs.
/// </summary>
public sealed class MessagePublisher : IMessagePublisher, IAsyncDisposable
{
#region Fields
    // Dependencies
    private readonly ILogger<MessagePublisher> _logger;

    // Messaging config/state
    private readonly JsonSerializerOptions _serializerOptions;
    private readonly QueueBindingsManager _bindingsManager;
    private readonly PublisherChannelPool _channelPool;
#endregion

#region Constructor
    public MessagePublisher(
        IOptions<BrokerOptions> workerConfig,
        ILogger<MessagePublisher> logger)
    {
        _logger = logger;
        var config = workerConfig.Value;

        _serializerOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false,
        };

        var connectionFactory = ConnectionFactoryBuilder.Create(config);
        _bindingsManager = new QueueBindingsManager(config);
        _channelPool = new PublisherChannelPool(
            connectionFactory,
            _bindingsManager,
            Math.Clamp(config.DefaultConcurrency, 1, config.MaxConcurrency),
            _logger);
    }
#endregion

#region Public API
    public async Task PublishAsync(MigrationCommand command, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        cancellationToken.ThrowIfCancellationRequested();

        var startedAt = Stopwatch.GetTimestamp();
        // Serialize directly to UTF-8 bytes to avoid the intermediate string allocation.
        var body = JsonSerializer.SerializeToUtf8Bytes(command, _serializerOptions);
        var payloadBytes = body.Length;

        var pooledChannel = await _channelPool.GetChannelAsync(cancellationToken).ConfigureAwait(false);
        var properties = BuildBasicProperties(command);

        // Publish asynchronously; confirms are not available on the async channel yet, so rely on logging + retries upstream.
        var exchangeName = _bindingsManager.ExchangeName;
        var routingKey = _bindingsManager.RoutingKey;
        try
        {
            await pooledChannel.Channel.BasicPublishAsync(
                exchange: exchangeName,
                routingKey: routingKey,
                mandatory: true,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await _channelPool.ReleaseChannelAsync(pooledChannel).ConfigureAwait(false);
        }

        var elapsed = Stopwatch.GetElapsedTime(startedAt);
        _logger.LogDebug(
            MigrationLogTemplates.PublishSuccess,
            command.MigrationId,
            command.MigrationScope,
            command.Priority,
            elapsed.TotalMilliseconds,
            payloadBytes);
    }

    private static BasicProperties BuildBasicProperties(MigrationCommand command) =>
        new()
        {
            Persistent = true,
            ContentType = "application/json",
            MessageId = command.MigrationId.ToString(),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            Headers = new Dictionary<string, object?>
            {
                ["migration-scope"] = command.MigrationScope.ToString(),
                ["priority"] = command.Priority.ToString(),
            },
        };

    public ValueTask DisposeAsync() => _channelPool.DisposeAsync();
#endregion
}
