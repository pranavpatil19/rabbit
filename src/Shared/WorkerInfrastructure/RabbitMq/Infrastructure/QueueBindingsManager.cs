using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using RabbitMQ.Client;

using WorkerHost.RabbitMq.Configuration;

namespace WorkerHost.RabbitMq.Infrastructure;

/// <summary>
/// Centralizes the logic for declaring and referencing the worker exchange/queue bindings.
/// </summary>
public sealed class QueueBindingsManager
{
    private readonly BrokerOptions _config;
    private readonly DeadLetterConfigurator _deadLetterConfigurator;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _infrastructureReady;

    public QueueBindingsManager(BrokerOptions config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _deadLetterConfigurator = new DeadLetterConfigurator(config.WorkEndpoint.DeadLetter);
    }

    public string ExchangeName => ResolveOrDefault(_config.WorkEndpoint.Exchange.Name, BrokerDefaults.WorkExchange);

    public string QueueName => ResolveOrDefault(_config.WorkEndpoint.Queue.Name, BrokerDefaults.WorkQueue);

    public string RoutingKey => ResolveOrDefault(_config.WorkEndpoint.RoutingKey, BrokerDefaults.WorkRoutingKey);

    /// <summary>
    /// Declares the exchange, queue, and binding once per process (when enabled via config).
    /// </summary>
    public async Task EnsureAsync(IChannel channel, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(channel);

        if (!_config.WorkEndpoint.DeclareInfrastructure || _infrastructureReady)
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_infrastructureReady)
            {
                return;
            }

            var exchange = _config.WorkEndpoint.Exchange;
            var queue = _config.WorkEndpoint.Queue;

            await _deadLetterConfigurator.EnsureAsync(channel, cancellationToken).ConfigureAwait(false);

            await channel.ExchangeDeclareAsync(
                ExchangeName,
                exchange.Type,
                durable: exchange.Durable,
                autoDelete: exchange.AutoDelete,
                arguments: exchange.Arguments,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            var queueArguments = CreateQueueArguments();

            await channel.QueueDeclareAsync(
                queue: QueueName,
                durable: queue.Durable,
                exclusive: queue.Exclusive,
                autoDelete: queue.AutoDelete,
                arguments: queueArguments,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            await channel.QueueBindAsync(
                queue: QueueName,
                exchange: ExchangeName,
                routingKey: RoutingKey,
                arguments: CloneDictionary(_config.WorkEndpoint.BindingArguments),
                noWait: false,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            _infrastructureReady = true;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Creates a defensive copy of binding arguments for scenarios that need to re-bind dynamically.
    /// </summary>
    public IDictionary<string, object?>? CreateBindingArgumentsCopy() => CloneDictionary(_config.WorkEndpoint.BindingArguments);

    /// <summary>
    /// Builds the queue arguments applied when declaring the primary work queue (includes DLX/TTL defaults).
    /// </summary>
    public IDictionary<string, object?> CreateQueueArguments() => BuildQueueArguments();

    private static IDictionary<string, object?>? CloneDictionary(IReadOnlyDictionary<string, object?>? source)
        => source is null || source.Count == 0
            ? null
            : new Dictionary<string, object?>(source);

    private static string ResolveOrDefault(string? value, string fallback)
        => string.IsNullOrWhiteSpace(value) ? fallback : value;

    private IDictionary<string, object?> BuildQueueArguments()
    {
        var args = CloneDictionary(_config.WorkEndpoint.Queue.Arguments) ?? new Dictionary<string, object?>();
        return _deadLetterConfigurator.ApplyDefaults(args);
    }
}
