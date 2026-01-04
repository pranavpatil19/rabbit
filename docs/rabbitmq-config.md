# RabbitMQ Configuration Reference

All messaging settings live under the `BrokerOptions` section of `appsettings*.json`. This file explains every option so operators can tune environments without touching code.

| Setting | Description | Example |
|---------|-------------|---------|
| `Connection.ConnectionString` | Optional AMQP URI. When provided it overrides host/port/user/password. | `"amqp://user:pass@host:5672/vhost"` |
| `Connection.Host` / `Port` / `UserName` / `Password` | Traditional connection fields used when `ConnectionString` is empty. | `"localhost"`, `5672`, `"guest"`, `"guest"` |
| `Connection.ClientProvidedName` | Friendly identifier used by RabbitMQ to trace connections (publisher/consumer use variants of this value). | `"WorkerHost.QueueWorker"` |
| `Connection.AutomaticRecoveryEnabled` | Enables the .NET client’s automatic connection recovery. Keep `true` unless broker policies forbid it. | `true` |
| `Connection.BindingsRecoveryEnabled` | Restores queues/exchanges/bindings after reconnection. Disable only when custom scripts manage bindings externally. | `true` |
| `WorkEndpoint.Exchange.*` | Name/type/durability/autodelete/arguments for the exchange that receives migration commands. | `{ "Name": "work-commands", "Type": "direct" }` |
| `WorkEndpoint.Queue.*` | Name/durability/exclusive/autodelete/arguments for the queue consumed by `QueueWorker`. | `{ "Name": "work-requests", "Durable": true }` |
| `WorkEndpoint.RoutingKey` | Routing key used by the publisher when sending messages. | `"work-route"` |
| `WorkEndpoint.DeclareInfrastructure` | When `true`, both publisher and consumer ensure the exchange/queue/binding exist on startup. Set `false` if infra is managed externally. | `true` |
| `WorkEndpoint.BindingArguments` | Optional dictionary applied when binding the queue. Useful for header exchanges, DLX, TTL, etc. | `{ "x-match": "all" }` |
| `DefaultBatchLimit` | Default chunk size inside BAL task transfer loops. Each chunk is read, rewritten, and inserted atomically. | `10` |
| `MaxBatchLimit` | Hard upper limit for chunk size. Prevents accidental OOM if a large override slips through. | `200` |
| `DefaultConcurrency` | Base number of concurrent migrations the worker runs when no scope-specific override applies. | `5` |
| `MaxConcurrency` | Absolute ceiling on worker concurrency. Even per-scope limits cannot exceed this value. | `32` |
| `ScopeConcurrencyLimits` | Optional map of `MigrationScope` → concurrency. Allows slowing down Computer moves while letting TaskBatch spike higher. | `{ "Computer": 2, "TaskBatch": 16 }` |
| `PullTimeoutSeconds` | Idle delay between queue polls when no messages are available. | `1.0` |
| `RequeueOnError` | If `true`, worker NACKs failed messages with requeue so RabbitMQ redelivers. Set `false` to dead-letter instead. | `true` |
| `MaxRetryCount` | Maximum micro-retries inside BAL before surfacing a failure (current implementation keeps it for future use). | `5` |
| `LogSequenceChannelName` | Identifier used by the log drain service to keep per-migration logs ordered. | `"migration-log-channel"` |
| `ListenerInactivityThresholdSeconds` | Health check threshold. If no message is processed within this window, `/health` reports the message listener as unhealthy. | `120` |
| `ListenerMetricsLogIntervalSeconds` | Interval for `MessageListenerMetricsReporter` to log snapshot metrics. Adjust to balance observability vs. noise. | `60` |

## Recommended Practices
1. **Environment Overrides:** Store production credentials via environment variables (e.g., `BrokerOptions__Connection__Password`) instead of committing them.  
2. **Bindings Management:** Leave `DeclareInfrastructure=true` in development/test. In production, set it to `false` once exchanges/queues are provisioned by Terraform/Ansible to prevent accidental drift.  
3. **Concurrency Tuning:** Start with conservative values (e.g., `DefaultConcurrency=5`, `Computer=2`, `TaskBatch=10`) and monitor listener metrics/health before raising limits.  
4. **Observability:** Scrape `/metrics` for listener stats and `/health` for quick liveness checks. Enable Serilog sinks (Application Insights, Seq, etc.) to capture the periodic listener logs.  
5. **Safety:** When experimenting with binding arguments (DLX, TTL), test via the JSON samples in `docs/rabbitmq-messages/` so the worker sees the same contract as the publisher.
