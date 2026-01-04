# QueueWorker Simplification Plan

## 1. Current Flow (textual flowchart)
```
+------------------+     +--------------------+     +----------------------+
| RabbitMQ Broker  | --> | IMessageListener   | --> | QueueWorker.Enqueue  |
+------------------+     +--------------------+     +----------------------+
                                                         |
                                                         v
                                                +----------------------+
                                                | DeliveryQueue        |
                                                | (Channel<IInbound>)  |
                                                +----------------------+
                                                         |
                                                         v
                            +------------------- WorkerLoopAsync (N tasks) -------------------+
                            |                                                                |
                            +--> ProcessDeliveryAsync --> HandleDeliveryAsync --> Ack/Nack --+
```

Detailed steps per message:
1. `ExecuteIteration` pumps the listener and enqueues deliveries.
2. `WorkerLoopAsync` drains the buffer with `await foreach`.
3. `ProcessDeliveryAsync` enforces concurrency via semaphore and delegates to the delivery processor.

Semaphores used:
- `_globalConcurrencySemaphore` throttles total handlers to the configured concurrency.
- `_scopeSemaphores` is always an empty dictionary (no-op today).
- `_workerInitLock` prevents double-starting the worker task array.

Pain points:
1. `QueueWorker` controls *everything* (listener pump, channel buffer, worker task lifecycle, failure policy, logging). This produces a 250+ line class that is hard to read or test.
2. Channel/backpressure logic is duplicated (nested `WaitToReadAsync`/`TryRead` loops) instead of using `ReadAllAsync`.
3. Runtime state fields have non-intuitive names (`_processingTasks`, `_workerCts`, `_listenerEnumerator`, `_startupLogged`) and expose implementation details rather than intent.
4. `ProcessDeliveryAsync` mixes concurrency gating, BAL invocation, and logging. There is no separation of concerns.

## 2. Target Structure & Naming

```
RabbitDeliveryIngestService : BackgroundService
  - owns IMessageListener enumerator
  - writes deliveries to DeliveryQueue (Channel abstraction)

DeliveryWorkerPool : BackgroundService
  - reads from DeliveryQueue (await foreach)
  - creates DeliveryProcessor instances (scoped) via factory
  - uses ConcurrencyGate (SemaphoreSlim wrapper) to limit parallel handlers

DeliveryProcessor (scoped service)
  - Deserialize payload
  - Invoke MigrationServiceFacade (BAL)
  - Apply RetryPolicy/RequeuePolicy
  - Ack/Nack through BrokerChannelAccess
  - Emit MigrationLog events via MigrationLogContext

Helpers:
  - ConcurrencyPlan (existing) -> expose `ReadCapacity`, `WorkerParallelism`
  - RequeuePolicy (wraps _failureCounts + BrokerOptions)
  - DeliveryQueue (wraps Channel<T> with `EnqueueAsync`, `ReadAllAsync`)
```

Field renames in the shorter-lived version (if we keep single class):
- `_listenerEnumerator` -> `_inboundStream`
- `_processingChannel` -> `_deliveryBuffer`
- `_processingTasks` -> `_workerTaskPool`
- `_workerCts` -> `_workerCancellation`
 - `_startupLogged` -> `_startupNotified`

## 3. Refactor Steps
1. **Introduce DeliveryQueue abstraction** (Channel wrapper) and move channel creation + read/write logic there. QueueWorker then calls `_deliveryQueue.EnqueueAsync` and `await foreach (var delivery in _deliveryQueue.ReadAllAsync(ct))`.
2. **Extract DeliveryProcessor class** that encapsulates `HandleMessageAsync` logic (deserialization, scope creation, ack/nack, logging). Inject it into QueueWorker via factory.
3. **Encapsulate concurrency gates** in a `ConcurrencyGate` type (wrapper around `SemaphoreSlim`) so `ProcessDeliveryAsync` becomes:
   ```csharp
   await _concurrencyGate.ExecuteAsync(() => _deliveryProcessor.ProcessAsync(delivery, ct), ct);
   ```
   ✅ Implemented (`src/WorkerHost/Background/ConcurrencyGate.cs` + usage inside `QueueWorker`).
4. Remove unused `_scopeSemaphores` (or wire it properly via config).
5. Rename runtime state fields to intent-revealing names (`_workerTaskPool`, `_workerCancellation`, `_inboundStream`, `_startupNotified`).
6. Once isolated, consider splitting into two hosted services (listener pump + worker pool). This reduces the class size and aligns with S in SOLID. ✅ Implemented via `RabbitDeliveryIngestService` (writer) + `QueueWorker` (reader).

### Proposed Function & Field Naming

| Current name | Proposed name | Rationale |
|--------------|---------------|-----------|
| `_processingTasks` | `_workerTaskPool` | Describes pooled worker tasks |
| `_workerCts` | `_workerCancellation` | Highlights cancellation purpose |
| `_listenerEnumerator` | `_inboundStream` | Communicates async stream intent |
| `_startupLogged` | `_startupNotified` | Indicates it is a guard flag |
| `EnsureWorkersStarted` | `StartWorkerPoolIfNeeded` | Verb describes action |
| `WorkerLoopAsync` | `RunWorkerLoopAsync` | Clarifies loop role |
| `HandleMessageAsync` | `ProcessDeliveryCoreAsync` | Emphasizes domain action |
| `_processingChannel` | `_deliveryBuffer` | Domain-friendly |

## 4. Semaphore Usage Recommendation
- Keep a single `GlobalConcurrencyGate` (wrapper around `SemaphoreSlim`) injected via `ConcurrencyPlan`. Dispose it in a central place.
- If per-scope throttling is needed later, introduce a `IScopeConcurrencySelector` service that returns the correct `SemaphoreSlim`. Avoid storing empty dictionaries in the worker.

## 5. Benefits
- **Maintainability**: Each class has a single responsibility; new retry logic or logging can be added without touching the pump.
- **Testability**: DeliveryProcessor, RequeuePolicy, and DeliveryQueue can be unit-tested independently.
- **Naming clarity**: Fields and methods describe intent (`_deliveryBuffer`, `_inboundStream`, `StartWorkerPoolAsync`, `StopWorkerPoolAsync`).
- **Extensibility**: Additional processing (e.g., metrics, tracing) can plug into the processor without bloating QueueWorker.

## 6. Folder Layout (post-reorg)
```
Background/
  Infrastructure/
    BackgroundServiceCollectionExtensions.cs
    ConcurrencyGate.cs
    ConcurrencyPlan.cs
  Ingestion/
    DeliveryQueue.cs
    RabbitDeliveryIngestService.cs
  Processing/
    DeliveryProcessor.cs
    MigrationDispatchHelper.cs
  Workers/
    QueueWorker.cs
    WorkerBufferPlanner.cs
```
This layout mirrors the responsibilities described above: infrastructure helpers, ingestion pipeline, per-message processing, and worker orchestration live in their own spaces, making it easier to find related code.
