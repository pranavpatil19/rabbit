# RabbitMQ Processing Architecture

This document captures the high‑level design that drives the WorkerHost queue consumer and how it satisfies the requirements from `requirements_spec.txt`.

## 1. Configuration Flow
1. All RabbitMQ and worker settings live under `BrokerOptions` in `appsettings*.json`.  
2. `BrokerConnectionOptions` defines connection string/host/port/user/password plus advanced flags (`AutomaticRecoveryEnabled`, `BindingsRecoveryEnabled`, `ClientProvidedName`).  
3. `WorkEndpointOptions` groups the exchange, queue, routing key, and binding arguments. No defaults are baked into the worker; if a value is omitted the `BrokerDefaults` helper is used only at config‑binding time to keep serialization stable.  
4. Additional worker controls (`DefaultConcurrency`, `MaxConcurrency`, `ScopeConcurrencyLimits`, `PullTimeoutSeconds`, `ListenerInactivityThresholdSeconds`, `MaxBatchLimit`, etc.) also live inside `BrokerOptions` so both the API and background worker honor the same settings.  
5. `WorkEndpoint.DeadLetter` injects DLX/TTL defaults (declares `work-requests.dlx` + queue, sets `x-dead-letter-*`, `x-message-ttl`) so poison messages never clog the main queue. Per-environment overrides are handled by merging `Queue.Arguments`.  
6. Options are bound/validated in `MessagingServiceCollectionExtensions.AddBrokerMessaging`, guaranteeing an invalid config fails fast during startup.

## 2. Messaging Components
1. **QueueBindingsManager** keeps the exchange/queue/binding names in sync across publisher and consumer. It is responsible for declaring the infrastructure once per process when `DeclareInfrastructure` is enabled.  
2. **MessagePublisher** serializes `MigrationCommand` to JSON (respecting the finalized `transfer.source/destination` contract) and publishes to the configured exchange/routing key. It delegates binding/queue declaration to the shared manager to avoid duplicating the logic.  
3. **MessageListener** maintains a long‑lived connection/channel for the consumer side. It exposes an `IAsyncEnumerable<IInboundDelivery>` that the worker can `await foreach`, encapsulating retry/reconnect logic and honoring the configured idle delay between empty pulls.  
4. **QueueWorker** is the background service that consumes the listener. It bounds concurrency via a `SemaphoreSlim`, deserializes each delivery into `MigrationCommand`, dispatches to `IBalMigrationService`, and sends ACK/NACK using the channel delivered by the listener. Ordered logging is preserved via `ILogQueue`.

## 3. Concurrency & Ordering
1. The worker calculates the global degree of parallelism using `Math.Clamp(DefaultConcurrency, 1, MaxConcurrency)` and also honors optional `ScopeConcurrencyLimits` from config (e.g., limit Computer jobs to 2 while allowing TaskBatch to spike higher).  
2. Each dequeued delivery is handled on a detached task. The shared `_channelOperationLock` inside the worker serializes ACK/NACK operations to avoid concurrent writes on the same channel.  
3. The BAL layer streams data using `IAsyncEnumerable` + `ChunkAsync` and clamps batch sizes using `DefaultBatchLimit/MaxBatchLimit`, enabling high throughput without unbounded memory usage.  
4. `ILogQueue` captures log entries per migration ID; `LogDrainService` outputs them sequentially so logs from concurrent migrations never interleave.  
5. Failures bubble back through the worker which uses `RequeueOnError` to decide whether to NACK with requeue or drop.

## 4. Extensibility Points
1. Additional exchanges/queues can be described entirely in `appsettings` by supplying new names/types/arguments. No code changes are required because the bindings manager copies the binding dictionaries and the message listener uses those resolved names.  
2. Prefetch limits, QoS, DLX arguments, or headers routing can be layered on by populating `BindingArguments`, `Queue.Arguments`, or `Exchange.Arguments`.  
3. The `MessageListener` abstraction allows specialized workers (e.g., for separate queues) by registering another listener with its own config section.  
4. Publisher and consumer share the same configuration and serialization settings, ensuring any future metadata added to `MigrationCommand` automatically flows through without duplicating mapping logic.  
5. BAL methods (`TransferComputerAsync`, `TransferAgentAsync`, `TransferTaskBatchAsync`) are invoked through a single dispatch method, making it straightforward to add new migration scopes while reusing the same worker infrastructure.

## 5. Validation & Health
1. The message samples under `docs/rabbitmq-messages/*.json` provide canonical payloads for each migration scope, including integer values for `migrationScope` (Computer=0, Agent=1, TaskBatch=2) and `priority` (Low=0, Normal=1, High=2).  
2. `MigrationCommandSamplesTests` deserialize the sample JSON to guarantee schema drift is caught during CI.  
3. `dotnet test` runs analyzer‑clean builds so Sonar/Microsoft analyzer warnings remain surfaced.  
4. Operationally, `/health` is backed by `MessageListenerHealthCheck`, `/metrics` aggregates migration + listener stats, and `MessageListenerMetricsReporter` logs periodic snapshots every `ListenerMetricsLogIntervalSeconds` so operators get a rolling history without enabling RabbitMQ-level tracing.
5. Test data can be refreshed on demand via `POST /testdata/load`, allowing local flood tests (or `tools/MigrationFloodPublisher --seed-endpoint ...`) to sync the in-memory DAL with `testdata/sample_data.json` before measurements.

This architecture keeps RabbitMQ integration fully configuration‑driven, enforces strict hierarchy rules defined in the requirements, and leaves room for future performance work (prefetch, parallel BAL pipelines, etc.) without rewriting the worker. 
