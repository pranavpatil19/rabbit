using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using RabbitMQ.Client;

using WorkerHost.RabbitMq.Configuration;

namespace WorkerHost.RabbitMq.Infrastructure;

/// <summary>
/// Small helper that encapsulates DLX/TTL declaration logic so QueueBindingsManager stays focused.
/// </summary>
internal sealed class DeadLetterConfigurator
{
    private readonly DeadLetterOptions _options;

    public DeadLetterConfigurator(DeadLetterOptions options)
    {
        _options = options;
    }

    public bool Enabled => _options.Enabled;

    public Task EnsureAsync(IChannel channel, CancellationToken cancellationToken) =>
        !_options.Enabled
            ? Task.CompletedTask
            : DeclareAsync(channel, cancellationToken);

    public IDictionary<string, object?> ApplyDefaults(IDictionary<string, object?> sourceArguments)
    {
        if (!_options.Enabled)
        {
            return sourceArguments;
        }

        var args = new Dictionary<string, object?>(sourceArguments);
        AddIfMissing(args, "x-dead-letter-exchange", _options.ExchangeName);
        AddIfMissing(args, "x-dead-letter-routing-key", _options.RoutingKey);
        if (_options.MessageTtlMilliseconds > 0)
        {
            AddIfMissing(args, "x-message-ttl", _options.MessageTtlMilliseconds);
        }
        return args;
    }

    private static void AddIfMissing(IDictionary<string, object?> target, string key, object? value)
    {
        if (!target.ContainsKey(key) && value is not null)
        {
            target[key] = value;
        }
    }

    private async Task DeclareAsync(IChannel channel, CancellationToken cancellationToken)
    {
        await channel.ExchangeDeclareAsync(
            _options.ExchangeName,
            _options.ExchangeType,
            durable: true,
            autoDelete: false,
            arguments: _options.ExchangeArguments,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await channel.QueueDeclareAsync(
            _options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: _options.QueueArguments,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await channel.QueueBindAsync(
            _options.QueueName,
            _options.ExchangeName,
            _options.RoutingKey,
            _options.BindingArguments,
            noWait: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
