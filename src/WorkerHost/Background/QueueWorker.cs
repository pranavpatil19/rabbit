using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WorkerHost.Bal;
using WorkerHost.Common.Background;
using WorkerHost.Logging;
using WorkerHost.Messaging;
using WorkerHost.RabbitMq.Channels;
using WorkerHost.RabbitMq.Configuration;
using WorkerHost.RabbitMq.Listener;

namespace WorkerHost.Background;

/// <summary>
/// Background consumer that drains the RabbitMQ queue and invokes BAL operations with bounded concurrency.
/// </summary>
public sealed class QueueWorker : PollingBackgroundService
{
#region Fields
    // Dependencies (interfaces/DI)
    private readonly ILogger<QueueWorker> _logger; // Diagnostics and operational visibility.
    private readonly IServiceScopeFactory _scopeFactory; // Creates per-message scopes for BAL/DAL services.
    private readonly IMessageListener _messageListener; // Async stream of inbound deliveries.
    private readonly IBrokerChannelAccess _channelAccess; // Serializes Ack/Nack calls to broker.

    // Configuration/state
    private readonly BrokerOptions _workerConfig; // Holds concurrency + requeue settings.
    private readonly JsonSerializerOptions _serializerOptions; // Message deserialization settings.
    private readonly ConcurrencyPlan _concurrencyPlan; // Encapsulates concurrency semaphores and worker count.
    private readonly SemaphoreSlim _globalConcurrencySemaphore; // Caps total concurrent message handlers.
    private readonly IReadOnlyDictionary<MigrationScope, SemaphoreSlim> _scopeSemaphores = new Dictionary<MigrationScope, SemaphoreSlim>(); // No per-scope throttling in use.
    private readonly Channel<IInboundDelivery> _processingChannel; // Internal bounded buffer between listener and workers.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<Guid, int> _failureCounts = new(); // Tracks per-migration failure attempts to prevent infinite requeues.
    private readonly object _workerInitLock = new(); // Guards worker start.
    private readonly int _workerCount; // Number of worker tasks running message handlers.

    // Runtime-managed state
    private Task[]? _workerTasks;
    private CancellationTokenSource? _workerCts;
    private IAsyncEnumerator<IInboundDelivery>? _listenerEnumerator;
    private bool _startupLogged;
#endregion

#region Constructor
    public QueueWorker(
        IServiceScopeFactory scopeFactory,
        IOptions<BrokerOptions> workerConfig,
        ILogger<QueueWorker> logger,
        IMessageListener messageListener,
        IBrokerChannelAccess channelAccess)
        : base(logger, TimeSpan.Zero)
    {
        _scopeFactory = scopeFactory;
        _workerConfig = workerConfig.Value;
        _logger = logger;
        _messageListener = messageListener;
        _channelAccess = channelAccess;

        _concurrencyPlan = new ConcurrencyPlan(
            _workerConfig.DefaultConcurrency,
            _workerConfig.MaxConcurrency);
        _globalConcurrencySemaphore = _concurrencyPlan.GlobalSemaphore;

        _serializerOptions = JsonOptionsFactory.CreateWorkerDeserializerOptions();
        _workerCount = _concurrencyPlan.WorkerCount;
        var channelCapacity = CalculateChannelCapacity(_workerConfig, _workerCount, _logger);
        _processingChannel = CreateProcessingChannel(channelCapacity);

        _logger.LogInformation(
            "QueueWorker configured: workers={WorkerCount}, channelCapacity={ChannelCapacity}, defaultConcurrency={DefaultConcurrency}, maxConcurrency={MaxConcurrency}",
            _workerCount,
            channelCapacity,
            _workerConfig.DefaultConcurrency,
            _workerConfig.MaxConcurrency);
    }
#endregion

#region Initialization helpers
    private static Channel<IInboundDelivery> CreateProcessingChannel(int capacity)
    {
        return Channel.CreateBounded<IInboundDelivery>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });
    }
#endregion

    private static int CalculateChannelCapacity(BrokerOptions config, int workerCount, ILogger logger)
    {
        var maxCapacity = Math.Max(workerCount, config.MaxConcurrency * 4);
        var capacity = Math.Clamp(config.WorkerChannelCapacity, workerCount, maxCapacity);
        if (capacity != config.WorkerChannelCapacity)
        {
            logger.LogWarning(
                "WorkerChannelCapacity {ConfiguredCapacity} is outside [{Min}, {Max}]; clamped to {Clamped}.",
                config.WorkerChannelCapacity,
                workerCount,
                maxCapacity,
                capacity);
        }

        return capacity;
    }

#region BackgroundService overrides
    protected override async Task<bool> ExecuteIterationAsync(CancellationToken cancellationToken)
    {
        if (!_startupLogged)
        {
            _logger.LogInformation("Migration worker starting");
            _startupLogged = true;
        }

        EnsureWorkersStarted();

        _listenerEnumerator ??= _messageListener.ReadAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
        IInboundDelivery delivery;
        try
        {
            if (!await _listenerEnumerator.MoveNextAsync().ConfigureAwait(false))
            {
                return false;
            }

            delivery = _listenerEnumerator.Current;
        }
        catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning(ex, "QueueWorker iteration cancelled by token");
            return false;
        }

        try
        {
            await _processingChannel.Writer.WriteAsync(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException ex)
        {
            _logger.LogWarning(ex, "QueueWorker channel closed while writing delivery; shutting down writer");
            return false;
        }

        return true;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _processingChannel.Writer.TryComplete();

        if (_workerCts is not null)
        {
            await _workerCts.CancelAsync().ConfigureAwait(false);
        }

        if (_workerTasks is not null)
        {
            try
            {
                await Task.WhenAll(_workerTasks).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex)
            {
                // Shutdown canceled in-flight handlers; warn so ops can trace it in production.
                _logger.LogWarning(ex, "Worker stop canceled in-flight handlers during shutdown.");
            }
        }

        if (_listenerEnumerator is not null)
        {
            await _listenerEnumerator.DisposeAsync().ConfigureAwait(false);
        }

        _workerCts?.Dispose();

        await base.StopAsync(cancellationToken).ConfigureAwait(false);
        await _concurrencyPlan.DisposeAsync().ConfigureAwait(false);
    }
#endregion

#region Worker coordination
    private void EnsureWorkersStarted()
    {
        if (_workerTasks is not null)
        {
            return;
        }

        lock (_workerInitLock)
        {
            if (_workerTasks is not null)
            {
                return;
            }

            _workerCts = new CancellationTokenSource();
            _workerTasks = Enumerable.Range(0, _workerCount)
                .Select(_ => Task.Run(() => WorkerLoopAsync(_workerCts.Token), CancellationToken.None))
                .ToArray();
        }
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _processingChannel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (_processingChannel.Reader.TryRead(out var delivery))
                {
                    await ProcessDeliveryAsync(delivery, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning(ex, "QueueWorker worker loop cancelled");
        }
    }
#endregion

#region Message processing pipeline
    private async Task ProcessDeliveryAsync(IInboundDelivery delivery, CancellationToken cancellationToken)
    {
        await _globalConcurrencySemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await HandleMessageAsync(delivery, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _globalConcurrencySemaphore.Release();
        }
    }

    /// <summary>
    /// Deserializes a message, runs the BAL operation inside a scoped provider, and then ACKs/NACKs plus logs.
    /// </summary>
    private async Task HandleMessageAsync(
        IInboundDelivery delivery,
        CancellationToken stoppingToken)
    {
        // Capture payload size/timing up front so we can log resource usage even if deserialization fails.
        var payloadBytes = delivery.Body.Length;
        var startedAt = Stopwatch.GetTimestamp();
        MigrationCommand? command = null; // Keep a reference so we can log/nack even if exceptions happen later.
        IDisposable? migrationScope = null;
        SemaphoreSlim? scopeSemaphore = null;
        TimeSpan elapsed = TimeSpan.Zero;
        try
        {
            command = DeserializeCommand(delivery); // JSON → MigrationCommand.
            if (command == null)
            {
                _logger.LogWarning("Unable to deserialize migration payload; dropping message"); // Bad payload.
                await AcknowledgeMessageAsync(delivery, command, stoppingToken).ConfigureAwait(false); // Remove from queue.
                return;
            }

            migrationScope = MigrationLogContext.Push(command);
            scopeSemaphore = await AcquireScopeSemaphoreAsync(command, stoppingToken).ConfigureAwait(false);

            LogWorkerEvent(command, LogLevel.Information, "Dequeued migration message for processing", null, null); // Inform observers this migration started.

            using var scope = _scopeFactory.CreateAsyncScope(); // Create scoped services for DAL/BAL.
            var balService = scope.ServiceProvider.GetRequiredService<IBalMigrationService>(); // Resolve BAL orchestrator.
            await MigrationDispatchHelper.DispatchAsync(balService, command, stoppingToken).ConfigureAwait(false); // Run the appropriate migration handler.

            elapsed = Stopwatch.GetElapsedTime(startedAt);
            await AcknowledgeMessageAsync(delivery, command, stoppingToken).ConfigureAwait(false); // Remove message from queue.
            _failureCounts.TryRemove(command.MigrationId, out _); // Clear any tracked failures on success.
            LogWorkerEvent(
                command,
                LogLevel.Information,
                MigrationLogTemplates.WorkerSuccess,
                null,
                new Dictionary<string, object?> { ["ElapsedMs"] = elapsed.TotalMilliseconds, ["PayloadBytes"] = payloadBytes },
                elapsed.TotalMilliseconds,
                payloadBytes); // Emit ordered success log with duration and payload size.
        }
        catch (OperationCanceledException ex) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogDebug(ex, "Cancellation requested while handling message"); // Graceful shutdown path.
        }
        catch (Exception ex)
        {
            elapsed = Stopwatch.GetElapsedTime(startedAt);
            _logger.LogError(ex, "Failed to handle migration message"); // Surface exception details.
            var attempts = command is null
                ? 0
                : _failureCounts.AddOrUpdate(command.MigrationId, 1, static (_, current) => current + 1);
            var requeue = _workerConfig.RequeueOnError && (!_workerConfig.MaxRequeueAttempts.HasValue || attempts < _workerConfig.MaxRequeueAttempts.Value);
            await RejectMessageAsync(delivery, command, requeue, stoppingToken).ConfigureAwait(false); // Requeue or drop.
            if (!requeue && command is not null)
            {
                _failureCounts.TryRemove(command.MigrationId, out _);
            }
            // Small backoff to avoid immediate requeue churn when failures occur.
            if (requeue)
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
            if (command is not null)
            {
                LogWorkerEvent(
                    command,
                    LogLevel.Error,
                    MigrationLogTemplates.WorkerFailure,
                    ex,
                    new Dictionary<string, object?>
                    {
                        ["Requeued"] = requeue,
                        ["ElapsedMs"] = elapsed.TotalMilliseconds,
                        ["PayloadBytes"] = payloadBytes,
                        ["Attempts"] = attempts,
                    },
                    elapsed.TotalMilliseconds,
                    ex.Message); // Emit ordered failure log with metadata.
            }
        }
        finally
        {
            scopeSemaphore?.Release();

            if (elapsed == TimeSpan.Zero)
            {
                elapsed = Stopwatch.GetElapsedTime(startedAt);
            }
            var migrationIdForLog = command is null ? "<unknown>" : command.MigrationId.ToString();
            _logger.LogDebug(
                "Completed migration {MigrationId} in {ElapsedMs:F2} ms (payload {PayloadBytes} bytes)",
                migrationIdForLog,
                elapsed.TotalMilliseconds,
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

    /// <summary>
    /// Sends a Basic.Nack to RabbitMQ (optionally requeueing the message) using the shared channel accessor.
    /// </summary>
    private async Task RejectMessageAsync(IInboundDelivery delivery, MigrationCommand? command, bool requeue, CancellationToken cancellationToken)
    {
        await _channelAccess.RejectAsync(delivery, requeue, cancellationToken).ConfigureAwait(false);
        _logger.LogWarning(
            MigrationLogTemplates.WorkerNack,
            command?.MigrationId,
            requeue);
    }
#endregion

#region Logging helpers
    private void LogWorkerEvent(
        MigrationCommand command,
        LogLevel level,
        string template,
        Exception? exception,
        IReadOnlyDictionary<string, object?>? properties,
        params object?[] args)
    {
        ArgumentNullException.ThrowIfNull(command);
        using var scope = MigrationLogContext.PushProperties(properties);
        _logger.Log(level, exception, template, args);
    }
#endregion

#region Concurrency helpers
    private async Task<SemaphoreSlim?> AcquireScopeSemaphoreAsync(MigrationCommand command, CancellationToken cancellationToken)
    {
        if (!_scopeSemaphores.TryGetValue(command.MigrationScope, out var semaphore))
        {
            return null;
        }

        await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        return semaphore;
    }
#endregion
}
