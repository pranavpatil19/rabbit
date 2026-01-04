using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using WorkerHost.Bal;
using WorkerHost.Logging;
using WorkerHost.Messaging;
using WorkerHost.RabbitMq.Channels;
using WorkerHost.RabbitMq.Configuration;

namespace WorkerHost.Background;

/// <summary>
/// Encapsulates the per-delivery processing pipeline (deserialize → BAL → ack/nack → logging).
/// </summary>
public sealed class DeliveryProcessor
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IBrokerChannelAccess _channelAccess;
    private readonly BrokerOptions _options;
    private readonly ILogger<DeliveryProcessor> _logger;
    private readonly JsonSerializerOptions _serializerOptions;
    private readonly ConcurrentDictionary<Guid, int> _failureCounts = new();

    public DeliveryProcessor(
        IServiceScopeFactory scopeFactory,
        IBrokerChannelAccess channelAccess,
        IOptions<BrokerOptions> options,
        ILogger<DeliveryProcessor> logger)
    {
        _scopeFactory = scopeFactory;
        _channelAccess = channelAccess;
        _logger = logger;
        _options = options.Value;
        _serializerOptions = JsonOptionsFactory.CreateWorkerDeserializerOptions();
    }

    public async Task ProcessAsync(IInboundDelivery delivery, CancellationToken stoppingToken)
    {
        var payloadBytes = delivery.Body.Length;
        var stopwatch = Stopwatch.StartNew();
        MigrationCommand? command = null;
        IDisposable? migrationScope = null;

        try
        {
            command = DeserializeCommand(delivery);
            if (command == null)
            {
                _logger.LogWarning("Unable to deserialize migration payload; dropping message");
                await AcknowledgeMessageAsync(delivery, command, stoppingToken).ConfigureAwait(false);
                return;
            }

            migrationScope = MigrationLogContext.Push(command);
            _logger.LogInformation(
                "Dequeued migration message for processing (payload {PayloadBytes} bytes)",
                payloadBytes);

            await using var scope = _scopeFactory.CreateAsyncScope();
            var balService = scope.ServiceProvider.GetRequiredService<IBalMigrationService>();
            await MigrationDispatchHelper.DispatchAsync(balService, command, stoppingToken).ConfigureAwait(false);

            await AcknowledgeMessageAsync(delivery, command, stoppingToken).ConfigureAwait(false);
            _failureCounts.TryRemove(command.MigrationId, out _);

            using (MigrationLogContext.PushProperties(
                       new Dictionary<string, object?>
                       {
                           ["ElapsedMs"] = stopwatch.Elapsed.TotalMilliseconds,
                           ["PayloadBytes"] = payloadBytes,
                       }))
            {
                _logger.LogInformation(
                    MigrationLogTemplates.WorkerSuccess,
                    stopwatch.Elapsed.TotalMilliseconds,
                    payloadBytes);
            }
        }
        catch (OperationCanceledException ex) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogDebug(ex, "Cancellation requested while handling message");
        }
        catch (Exception ex)
        {
            await HandleProcessingFailureAsync(
                    delivery,
                    command,
                    payloadBytes,
                    stopwatch.Elapsed.TotalMilliseconds,
                    ex,
                    stoppingToken)
                .ConfigureAwait(false);
        }
        finally
        {
            stopwatch.Stop();
            var migrationIdForLog = command is null ? "<unknown>" : command.MigrationId.ToString();
            _logger.LogDebug(
                "Completed migration {MigrationId} in {ElapsedMs:F2} ms (payload {PayloadBytes} bytes)",
                migrationIdForLog,
                stopwatch.Elapsed.TotalMilliseconds,
                payloadBytes);
            migrationScope?.Dispose();
        }
    }

    private MigrationCommand? DeserializeCommand(IInboundDelivery delivery)
    {
        try
        {
            return JsonSerializer.Deserialize<MigrationCommand>(delivery.Body.Span, _serializerOptions);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "JSON deserialization error for migration message");
            return null;
        }
    }

    private async Task AcknowledgeMessageAsync(IInboundDelivery delivery, MigrationCommand? command, CancellationToken cancellationToken)
    {
        await _channelAccess.AcknowledgeAsync(delivery, cancellationToken).ConfigureAwait(false);
        _logger.LogDebug(
            MigrationLogTemplates.WorkerAck,
            command?.MigrationId);
    }

    private async Task RejectMessageAsync(IInboundDelivery delivery, MigrationCommand? command, bool requeue, CancellationToken cancellationToken)
    {
        await _channelAccess.RejectAsync(delivery, requeue, cancellationToken).ConfigureAwait(false);
        _logger.LogWarning(
            MigrationLogTemplates.WorkerNack,
            command?.MigrationId,
            requeue);
    }

    private bool ShouldRequeue(int attempts)
    {
        if (!_options.RequeueOnError)
        {
            return false;
        }

        if (!_options.MaxRequeueAttempts.HasValue)
        {
            return true;
        }

        return attempts < _options.MaxRequeueAttempts.Value;
    }

    private static async Task BackoffAsync(CancellationToken stoppingToken)
    {
        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(200), stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Ignore cancellation during backoff; shutdown will proceed.
        }
    }

    private async Task HandleProcessingFailureAsync(
        IInboundDelivery delivery,
        MigrationCommand? command,
        int payloadBytes,
        double elapsedMilliseconds,
        Exception exception,
        CancellationToken stoppingToken)
    {
        _logger.LogError(exception, "Failed to handle migration message");

        var attempts = command is null
            ? 0
            : _failureCounts.AddOrUpdate(command.MigrationId, 1, static (_, current) => current + 1);

        var requeue = ShouldRequeue(attempts);
        await RejectMessageAsync(delivery, command, requeue, stoppingToken).ConfigureAwait(false);

        if (!requeue && command is not null)
        {
            _failureCounts.TryRemove(command.MigrationId, out _);
        }

        if (requeue)
        {
            await BackoffAsync(stoppingToken).ConfigureAwait(false);
        }

        if (command is not null)
        {
            using var propertyScope = MigrationLogContext.PushProperties(
                new Dictionary<string, object?>
                {
                    ["Requeued"] = requeue,
                    ["ElapsedMs"] = elapsedMilliseconds,
                    ["PayloadBytes"] = payloadBytes,
                    ["Attempts"] = attempts,
                });

            _logger.LogError(
                exception,
                MigrationLogTemplates.WorkerFailure,
                elapsedMilliseconds,
                exception.Message);
        }
    }
}
