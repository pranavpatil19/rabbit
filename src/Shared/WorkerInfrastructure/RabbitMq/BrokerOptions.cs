using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace WorkerHost.RabbitMq.Configuration;

/// <summary>
/// Consolidated broker + worker settings with nested options for clarity.
/// </summary>
public sealed record BrokerOptions
{
    /// <summary>
    /// Connection-related settings shared by publishers and consumers.
    /// </summary>
    [Required]
    public BrokerConnectionOptions Connection { get; init; } = new();

    /// <summary>
    /// Exchange/queue/routing-key definitions for the work queue.
    /// </summary>
    [Required]
    public WorkEndpointOptions WorkEndpoint { get; init; } = new();

    /// <summary>
    /// Default chunk size when breaking large transfers into batches.
    /// </summary>
    [Range(1, 5000)]
    public int DefaultBatchLimit { get; init; } = BrokerDefaults.DefaultBatchLimit;

    /// <summary>
    /// Upper guardrail for chunk size to prevent excessive memory usage.
    /// </summary>
    [Range(1, 5000)]
    public int MaxBatchLimit { get; init; } = BrokerDefaults.MaxBatchLimit;

    /// <summary>
    /// Baseline worker concurrency when no scope override is provided.
    /// </summary>
    [Range(1, 500)]
    public int DefaultConcurrency { get; init; } = BrokerDefaults.DefaultConcurrency;

    /// <summary>
    /// Hard ceiling on worker concurrency even if overrides request more.
    /// </summary>
    [Range(1, 500)]
    public int MaxConcurrency { get; init; } = BrokerDefaults.MaxConcurrency;

    /// <summary>
    /// Maximum number of messages buffered between the listener and worker tasks before back-pressure engages.
    /// </summary>
    [Range(1, 5000)]
    public int WorkerChannelCapacity { get; init; } = BrokerDefaults.WorkerChannelCapacity;

    /// <summary>
    /// Delay (seconds) before polling the broker again when the queue is empty.
    /// </summary>
    [Range(0.1, 30)]
    public double PullTimeoutSeconds { get; init; } = BrokerDefaults.PullTimeoutSeconds;

    /// <summary>
    /// Determines whether failed messages should be requeued.
    /// </summary>
    public bool RequeueOnError { get; init; } = BrokerDefaults.RequeueOnError;

    /// <summary>
    /// Optional cap on how many times a failed message is requeued. Null = unlimited retries.
    /// </summary>
    [Range(1, 100)]
    public int? MaxRequeueAttempts { get; init; } = BrokerDefaults.MaxRequeueAttempts;

    /// <summary>
    /// Channel identifier used by the log queue to keep per-migration logs ordered.
    /// </summary>
    [Required]
    public string LogSequenceChannelName { get; init; } = BrokerDefaults.LogSequenceChannelName;

    /// <summary>
    /// Optional per-scope concurrency limits keyed by MigrationScope name (Computer/Agent/TaskBatch).
    /// </summary>
    public Dictionary<string, int> ScopeConcurrencyLimits { get; init; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Health check threshold; if no message is processed within this window the listener is marked unhealthy.
    /// </summary>
    [Range(1, 3600)]
    public double ListenerInactivityThresholdSeconds { get; init; } = BrokerDefaults.ListenerInactivityThresholdSeconds;

    /// <summary>
    /// Interval (seconds) for logging message-listener metrics snapshots.
    /// </summary>
    [Range(5, 3600)]
    public double ListenerMetricsLogIntervalSeconds { get; init; } = BrokerDefaults.ListenerMetricsLogIntervalSeconds;
}

public sealed record BrokerConnectionOptions
{
    /// <summary>
    /// Optional AMQP URI (scheme, credentials, host, vhost) used by the API publisher and worker.
    /// When not supplied, host/port/credential fields are used.
    /// </summary>
    public string? ConnectionString { get; init; }

    [Required]
    public string Host { get; init; } = BrokerDefaults.Host;

    [Range(1, 65535)]
    public int Port { get; init; } = BrokerDefaults.Port;

    [Required]
    public string UserName { get; init; } = BrokerDefaults.UserName;

    [Required]
    public string Password { get; init; } = BrokerDefaults.Password;

    [Required]
    public string ClientProvidedName { get; init; } = BrokerDefaults.WorkerClientProvidedName;

    /// <summary>
    /// Enables the RabbitMQ .NET client's automatic connection recovery.
    /// </summary>
    public bool AutomaticRecoveryEnabled { get; init; } = BrokerDefaults.AutomaticRecoveryEnabled;

    /// <summary>
    /// Recreates exchanges/queues/bindings after a recovered connection.
    /// </summary>
    public bool BindingsRecoveryEnabled { get; init; } = BrokerDefaults.BindingsRecoveryEnabled;
}

public sealed record WorkEndpointOptions
{
    [Required]
    public ExchangeOptions Exchange { get; init; } = new();

    [Required]
    public QueueOptions Queue { get; init; } = new();

    [Required]
    public string RoutingKey { get; init; } = BrokerDefaults.WorkRoutingKey;

    /// <summary>
    /// When true, the app declares the exchange/queue/binding on startup.
    /// </summary>
    public bool DeclareInfrastructure { get; init; } = BrokerDefaults.DeclareInfrastructure;

    /// <summary>
    /// Optional arguments supplied when binding the queue to the exchange (e.g., x-match headers).
    /// </summary>
    public Dictionary<string, object?> BindingArguments { get; init; } = BrokerDefaults.CreateArgumentsDictionary();

    /// <summary>
    /// Dead-letter / TTL configuration applied to the primary work queue.
    /// </summary>
    [Required]
    public DeadLetterOptions DeadLetter { get; init; } = new();
}

public sealed record ExchangeOptions
{
    [Required]
    public string Name { get; init; } = BrokerDefaults.WorkExchange;

    [Required]
    public string Type { get; init; } = BrokerDefaults.WorkExchangeType;

    public bool Durable { get; init; } = true;

    public bool AutoDelete { get; init; } = false;

    public Dictionary<string, object?> Arguments { get; init; } = BrokerDefaults.CreateArgumentsDictionary();
}

public sealed record DeadLetterOptions
{
    /// <summary>
    /// Enables declaration of a dedicated DLX + queue and wires the primary queue to route poison messages there.
    /// </summary>
    public bool Enabled { get; init; } = false;

    /// <summary>
    /// Exchange used as the DLX target.
    /// </summary>
    [Required]
    public string ExchangeName { get; init; } = BrokerDefaults.DeadLetterExchange;

    /// <summary>
    /// Exchange type for the DLX (default direct).
    /// </summary>
    [Required]
    public string ExchangeType { get; init; } = BrokerDefaults.DeadLetterExchangeType;

    /// <summary>
    /// Queue that stores poison messages routed via the DLX.
    /// </summary>
    [Required]
    public string QueueName { get; init; } = BrokerDefaults.DeadLetterQueue;

    /// <summary>
    /// Routing key supplied when binding the primary queue to the DLX.
    /// </summary>
    [Required]
    public string RoutingKey { get; init; } = BrokerDefaults.DeadLetterRoutingKey;

    /// <summary>
    /// TTL applied to messages that remain on the primary work queue (milliseconds). Zero disables TTL injection.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int MessageTtlMilliseconds { get; init; } = BrokerDefaults.PrimaryQueueMessageTtlMilliseconds;

    public Dictionary<string, object?> ExchangeArguments { get; init; } = BrokerDefaults.CreateArgumentsDictionary();

    public Dictionary<string, object?> QueueArguments { get; init; } = BrokerDefaults.CreateArgumentsDictionary();

    public Dictionary<string, object?> BindingArguments { get; init; } = BrokerDefaults.CreateArgumentsDictionary();
}

public sealed record QueueOptions
{
    [Required]
    public string Name { get; init; } = BrokerDefaults.WorkQueue;

    public bool Durable { get; init; } = true;

    public bool Exclusive { get; init; } = false;

    public bool AutoDelete { get; init; } = false;

    public Dictionary<string, object?> Arguments { get; init; } = BrokerDefaults.CreateArgumentsDictionary();
}
