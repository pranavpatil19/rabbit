using System.Collections.Generic;

namespace WorkerHost.RabbitMq.Configuration;

/// <summary>
/// Central place to store broker-related default values so configuration and runtime code stay in sync.
/// </summary>
internal static class BrokerDefaults
{
    // Fallback values that BrokerOptions picks up when config/env values are absent.
    // Connection/login defaults (map to BrokerOptions.Connection.*)
    public const string Host = "localhost"; // Connection.Host
    public const int Port = 5672; // Connection.Port
    public const string UserName = "guest"; // Connection.UserName
    public const string Password = "guest"; // Connection.Password
    public const string WorkerClientProvidedName = "WorkerHost.QueueWorker"; // Connection.ClientProvidedName

    // Primary exchange/queue topology (map to BrokerOptions.WorkEndpoint.*)
    public const string WorkExchange = "work-commands"; // WorkEndpoint.Exchange.Name
    public const string WorkExchangeType = "direct"; // WorkEndpoint.Exchange.Type
    public const string WorkQueue = "work-requests"; // WorkEndpoint.Queue.Name
    public const string WorkRoutingKey = "work-route"; // WorkEndpoint.RoutingKey
    public const string DeadLetterExchange = "work-requests.dlx"; // DeadLetter.ExchangeName
    public const string DeadLetterExchangeType = "direct"; // DeadLetter.ExchangeType
    public const string DeadLetterQueue = "work-requests.dlx.queue"; // DeadLetter.QueueName
    public const string DeadLetterRoutingKey = "work-requests.dlx"; // DeadLetter.RoutingKey
    public const int PrimaryQueueMessageTtlMilliseconds = 600000; // DeadLetter.MessageTtlMilliseconds

    // Operational wiring defaults (misc root-level flags)
    public const int PublishConfirmTimeoutSeconds = 5; // future BrokerOptions flag
    public const bool DeclareInfrastructure = true; // WorkEndpoint.DeclareInfrastructure

    // Worker throughput limits (BrokerOptions root-level knobs)
    public const int DefaultBatchLimit = 10; // DefaultBatchLimit
    public const int MaxBatchLimit = 200; // MaxBatchLimit
    public const int DefaultConcurrency = 5; // DefaultConcurrency
    public const int MaxConcurrency = 32; // MaxConcurrency
    public const int WorkerChannelCapacity = DefaultConcurrency * 4; // WorkerChannelCapacity
    public const double PullTimeoutSeconds = 1.0; // PullTimeoutSeconds
    public const bool RequeueOnError = true; // RequeueOnError
    public static readonly int? MaxRequeueAttempts = null; // MaxRequeueAttempts
    public const string LogSequenceChannelName = "migration-log-channel"; // LogSequenceChannelName
    public const double ListenerInactivityThresholdSeconds = 120; // ListenerInactivityThresholdSeconds
    public const double ListenerMetricsLogIntervalSeconds = 60; // ListenerMetricsLogIntervalSeconds

    /// <summary>
    /// Helper to create a dictionary for RabbitMQ arguments so defaults stay centralized.
    /// </summary>
    public static Dictionary<string, object?> CreateArgumentsDictionary() => new();

    public const bool AutomaticRecoveryEnabled = true;
    public const bool BindingsRecoveryEnabled = true;
}
