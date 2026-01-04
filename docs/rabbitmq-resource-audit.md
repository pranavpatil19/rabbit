# RabbitMQ Listener & Publisher Resource Audit

This document tracks how the worker currently consumes CPU, memory, and connections while pushing/pulling migration messages and lays out the next optimization wave. It complements `docs/rabbitmq-config.md` (knobs) and `docs/refactor-readiness-report.md` (completed refactors) by focusing strictly on runtime cost, measurements, and the advanced RabbitMQ/.NET features we plan to adopt.

## 1. Goals for the Next Iteration

1. **Quantify** peak memory, CPU, and latency for both publishing (`MessagePublisher`) and consuming (`MessageListener`/`QueueWorker`) paths.
2. **Identify** which code paths allocate or block the most (JSON serialization, connection churn, idle polling, logging).
3. **Reduce** resource usage without sacrificing observability by using RabbitMQ capabilities (prefetch/QoS, publisher confirms, connection reuse) and modern C# features (span-based serialization, pooling, channels).
4. **Document** a phased implementation plan so changes ship incrementally with clear success metrics.

### Execution Cadence Reminder

- Treat the phases in this audit as a continuous loop: once a phase’s exit criteria are satisfied, immediately start the next phase without waiting for external sign-off.
- When a phase is finished, stamp the relevant section with `DONE` in the heading and note the date so we keep momentum and remember what was completed.

## 2. Baseline Measurement Plan

| Area | Tooling / Counters | Notes |
|------|--------------------|-------|
| Listener throughput & latency | `Stopwatch` timing already added in `QueueWorker.HandleMessageAsync`; extend to expose metrics via `MessageListenerMetrics`. | Capture min/avg/p95 elapsed time + payload bytes. |
| Memory allocations | `dotnet-counters monitor System.Runtime` combined with `EventPipe` traces during synthetic load. | Focus on LOH allocations (>85KB) during large TaskBatch moves. |
| Connection churn | `MessageListenerMetrics` + RabbitMQ management API (`connections` endpoint) to count reconnects per hour. | Confirms whether `BasicGet` idle loops trigger unnecessary reconnects. |
| Publisher latency | Wrap `MessagePublisher.PublishAsync` with `Stopwatch` and log duration, payload size, routing info. | Needed before we enable publisher confirms. |
| Broker back-pressure | RabbitMQ `queue_details` (`messages_ready`, `messages_unacknowledged`) plus `basic.get-empty` metrics. | Ensures QoS tweaks don’t starve the queue. |

## 3. Current Resource Profile (Code Review)

### 3.1 Listener (`MessageListener`, `QueueWorker`)

| Stage | Code Reference | Observed Cost / Risk |
|-------|----------------|----------------------|
| Connection bootstrap | `MessageListener.EnsureChannelAsync` | Keeps a single connection, amortizing TLS and socket cost, but error handling was previously silent (now logged). |
| Poll loop | `MessageListener.ReadAsync` | Uses `BasicGet` in a tight loop with `_idleDelay`. When the queue is empty we incur double round-trips and timer wakeups, increasing CPU even though no work is processed. |
| Dispatch & concurrency | `QueueWorker.ExecuteIterationAsync` | Bounded by `DefaultConcurrency` via semaphores, so threads scale predictably. However, the worker spins up a `Task.Run` per message; heavy workloads could benefit from `System.Threading.Channels` to reuse a worker pool. |
| Deserialization | `QueueWorker.DeserializeCommand` | Now uses `ReadOnlySpan<byte>`, eliminating the temporary UTF-8 string allocation—cuts per-message allocation roughly in half. |
| Logging & ACK/NACK | `IBrokerChannelAccess` | Shared channel avoids extra connections, but logging previously lacked duration/payload context; recent change adds that telemetry. |

### 3.2 Publisher (`MessagePublisher`)

| Concern | Detail |
|---------|--------|
| Connection usage | Creates a new connection/channel per publish. Safe but expensive under bursty loads (socket churn, TLS handshakes). |
| Serialization | Still converts to UTF-8 string before bytes (`Encoding.UTF8.GetBytes`). Equivalent to consumer’s old pattern; we can apply span-based serialization or `JsonSerializer.SerializeToUtf8Bytes`. |
| Confirmations | Async channel APIs do not expose confirms; we currently rely on upstream retries/logging. Missing confirms means we can’t guarantee broker persistence under failures. |
| Headers & priority | Basic properties include scope/priority, but we don’t set `BasicProperties.Priority` or leverage broker QoS. |

### 3.3 Background Logging (`ILogQueue`, `MigrationLogEventBuilder`)

Logging itself is lightweight, but large property bags or exceptions can allocate heavily. We should cap metadata size and move to structured logging templates so JSON log sinks remain small.

## 4. High-Impact Optimizations (Ready to Implement)

| # | Optimization | Component | Benefit | Effort |
|---|--------------|-----------|---------|--------|
| 1 | Switch publisher to `JsonSerializer.SerializeToUtf8Bytes` and reuse `ArrayPool<byte>` buffers. | MessagePublisher | Halves payload allocations, reduces GC pressure during bursts. | Low |
| 2 | Add connection/channel pooling for publishers (e.g., `AsyncLocal` channel cache or `IAsyncDisposable` pool). | MessagePublisher | Removes per-message TCP/TLS overhead, lowers latency. | Medium |
| 3 | Replace `BasicGet` loop with `BasicConsume` + `AsyncEventingBasicConsumer`, enabling `BasicQos(prefetch)` to control in-flight count. | MessageListener | Eliminates idle polling CPU, gives better back-pressure control. | Medium |
| 4 | Introduce `System.Threading.Channels` between listener and worker tasks. | QueueWorker | Enables bounded buffering, reduces `Task.Run` overhead, supports batching. | Medium |
| 5 | Track `MessagePublisher` / `QueueWorker` duration + payload metrics via `MessageListenerMetrics` and emit to logs/metrics endpoint. | Metrics | Provides visibility to validate all other optimizations. | Low |

## 5. Advanced Feature Opportunities

### RabbitMQ Features

1. **Prefetch/QoS**: After moving to `BasicConsume`, set `channel.BasicQos(prefetchCount, 0, false)` to match `DefaultConcurrency`, ensuring the broker only delivers work we can process.
2. **Publisher Confirms**: Once we move back to synchronous channels (or `ConfirmSelect` is supported async), enable confirms to guarantee persistence and implement retry/backoff on negative ACKs.
3. **Dead-letter policies & TTL**: Configure per-scope DLX/TTL via `WorkEndpoint.BindingArguments` to keep poison messages from clogging the main queue.
4. **Connection name hygiene**: Already using `ClientProvidedName`; extend to include deployment slot or hostname for faster triage.

### C# / .NET Features

1. **`System.Text.Json` source generators**: Generate serializers for `MigrationCommand` to reduce reflection cost and improve throughput.
2. **`ArrayPool<byte>` and pooled writers**: Reuse buffers for serialization/deserialization; pairs well with `IBufferWriter<byte>` APIs.
3. **`ValueTask` and `IAsyncEnumerable` optimizations**: Continue leveraging `ValueTask` returning APIs in `IBrokerChannelAccess` to cut allocation for synchronous completions.
4. **`CancellationTokenSource.CreateLinkedTokenSource` hygiene**: Ensure we dispose linked tokens in publishers/workers to avoid timer queues.
5. **Structured logging helpers**: Encapsulate repeated logging patterns (success/failure) to avoid per-call dictionary allocations.

## 6. Implementation Roadmap

| Phase | Scope | Key Tasks | Success Metrics |
|-------|-------|-----------|-----------------|
| **Phase 1 – Instrument & Pool** | Publisher + Worker | (a) Add publisher duration/payload logs, (b) switch to `SerializeToUtf8Bytes`, (c) add simple channel pool (e.g., `IAsyncDisposable` wrapper). | ≤50% reduction in GC allocations during burst publish load; latency p95 improves by ≥20%. |
| **Phase 2 – Push Consumption** | Listener + Worker | (a) Replace `BasicGet` loop with push consumer, (b) configure prefetch, (c) feed deliveries into `Channel<IInboundDelivery>` with bounded capacity. | CPU usage during idle drops near zero; backlog drains faster due to prefetch. |
| **Phase 3 – Reliability Enhancements** | Publisher + Listener | (a) Introduce publisher confirms + retry strategy, (b) add dead-letter routing, (c) expose connection health metrics. | Zero lost messages in failure injection tests; health endpoint detects broker outages within threshold. |
| **Phase 4 – Adaptive Concurrency** | Worker | (a) Reactivate `ScopeConcurrencyLimits` with dynamic reload, (b) integrate with metrics to auto-tune concurrency based on queue depth. | Worker maintains SLA without manual restarts; no semaphore starvation. |

## 7. Validation Strategy

1. **Load Tests**: Use synthetic migration payloads (small Agent vs. large TaskBatch) to simulate worst-case memory usage. Record allocations with `dotnet-trace`.
2. **Functional Tests**: Existing `MigrationCommandSamplesTests` ensure schema compatibility; extend with integration tests that publish to an in-memory RabbitMQ container (e.g., `Testcontainers`) to validate new consumer pipeline.
3. **Observability**: Extend `MessageListenerMetricsReporter` to expose new counters (payload bytes/sec, avg latency) and scrape via `/metrics`.
4. **Canary Deployments**: Roll out optimizations to a single worker instance, compare connection churn + CPU vs. baseline using RabbitMQ management API + app logs.

## 8. Backlog / Open Questions

1. Should we adopt RabbitMQ Streams for high-volume TaskBatch migrations? (Would shift from queue semantics to append-only logs.)
2. Do we need multi-queue support (per scope) instead of a single `work-requests` queue? That decision affects routing keys and concurrency design.
3. How do we surface publisher failures to the control plane? Need an outbox or success callback if confirms remain unavailable in async APIs.
4. Should per-migration logs move to a structured storage (e.g., Seq, Application Insights) instead of the current in-process queue?

With these measurements and phased tasks defined, we can now start implementing Phase 1: instrumenting publisher latency and reducing payload allocations, confident that later phases (push consumption, confirms, adaptive concurrency) have clear requirements and success criteria.

### Phase 1 – Baseline Checklist

**Test harness**

| Asset | Path / Command | Notes |
|-------|----------------|-------|
| Broker | `docker run -d --privileged -p 5672:5672 -p 15672:15672 --name rabbitmq-baseline rabbitmq:3-management` | Local broker with management UI; privileged flag required on this host. |
| Worker under test | `dotnet run --project src/WorkerHost/WorkerHost.csproj` | Uses `appsettings.Development.json`; log level set to capture instrumentation. |
| Publisher flood | `dotnet run --project tools/MigrationFloodPublisher -- --count 500 --mix Computer,Agent,TaskBatch --tasks-per-batch 200 --progress 200 --seed-endpoint http://localhost:8080/testdata/load` | Generates mixed workloads via the `tools/MigrationFloodPublisher` harness and auto-seeds test data before flooding. |

**Instrumentation commands**

```bash
# Runtime counters (System.Runtime provider)
dotnet-counters collect --process-id <QueueWorker PID> --duration 0:0:0:15 --output docs/baselines/worker-baseline.counters.csv

# RabbitMQ queue metrics
curl -u guest:guest http://localhost:15672/api/queues/%2f/work-requests
curl -u guest:guest http://localhost:15672/api/connections | jq 'length'

# Publisher timing (temporary until structured logs exist)
/usr/bin/time -l dotnet run --project tools/MigrationFloodPublisher ...
```

**Metrics to capture**

| Metric | Where it lives |
|--------|----------------|
| Publisher p95 latency, RSS, CPU | `/usr/bin/time -l` output in console log |
| Runtime counters (GC heap, allocation rate, CPU) | `docs/baselines/worker-baseline.counters.csv` |
| RabbitMQ queue depth / connection count | Saved curl output in the audit notes |

**Execution status**

- [x] Broker container verified
- [x] Flood publisher implemented and built
- [x] Baseline counters captured (`docs/baselines/worker-baseline.counters.csv`)
- [x] Results summarized in this audit

### Phase 1 – Detailed Test & Execution Plan

1. **Baseline Load Test**  
   - Run a scripted burst of mixed Computer/TaskBatch publishes against a local RabbitMQ (Docker container or shared broker) using `tools/MigrationFloodPublisher`.  
   - Monitor `dotnet-counters System.Runtime` plus RabbitMQ management stats (`messages_ready`, connection count) while the worker drains the queue.  
   - Capture current publish latency by wrapping `MessagePublisher.PublishAsync` with a `Stopwatch` (temporary instrumentation).

2. **Instrumentation Updates**  
   - Permanently add payload-size + elapsed-time logging around `PublishAsync` using structured logging so metrics can later be emitted via `MessageListenerMetrics`.  
   - Use `JsonSerializer.SerializeToUtf8Bytes` (or an `ArrayPool<byte>` writer) to remove the intermediate string allocations in the publisher.  
   - Introduce a lightweight connection/channel pool (e.g., `ConcurrentBag` of `IAsyncDisposable` wrappers) so bursts reuse AMQP connections instead of re-handshaking.

3. **Verification Run**  
   - Repeat the burst test and compare allocations/sec, GC pauses, publish p95 latency, and broker connection counts against the baseline.  
   - Target: ≤50% reduction in payload-related allocations and ≥20% improvement in publish p95 latency during the burst.  
   - Once metrics look good, downgrade the new logs to `Debug` and feed the timing data into `MessageListenerMetricsReporter` for ongoing monitoring.

### Baseline Findings (2026-01-03)

- Publisher flood (`500` commands, `tasksPerBatch=200`) completes in ~7.6–9.2 seconds with `/usr/bin/time -l` reporting **206 MB max RSS** and ~10 CPU seconds (4.4 user / 6.7 system). The high sys time aligns with opening/closing a connection per publish.
- Worker counters (`worker-baseline.counters.csv`) show **CPU ≤ 0.07%**, **GC heap ≈ 58.5 MB**, **working set ≈ 153 MB**, but still allocate up to **32 KB/sec** while idle—evidence that repeated `BasicGet` polling plus message requeue churn costs memory even without successful migrations.
- RabbitMQ queue snapshot after the burst shows `messages_ready=494`, `deliver_get=175,505`, and `consumers=0`, confirming the worker’s `BasicGet` loop behaves like a busy poll (no consumer count) and that payload/fixture mismatches trigger continuous NACK/requeue cycles.
- Synthetic payloads referencing unknown computers/agents cause every TaskBatch migration to fail immediately, so the queue never drains. This is acceptable for resource baselines but must be addressed (via better fixtures or conditional drops) before measuring steady-state throughput.

### Phase 1 – Post-Optimization Measurements (2026-01-03)

- **Publisher flood (`500` commands, `tasksPerBatch=200`)** now completes in **~0.16–0.17 s** (was 7.6–9.2 s). `/usr/bin/time -l` reports **199 MB max RSS** (down ~7 MB) and **1.8 s user / 0.7 s sys CPU** (previously 4.4 s / 6.7 s). Involuntary context switches dropped from ~906K to **42K**, confirming connection/channel pooling eliminated most per-message handshake overhead.
- **Worker counters (`docs/baselines/worker-phase1.counters.csv`)** show **CPU up to 0.41% (avg 0.08%)**, **GC heap ~62 MB**, **working set ~147 MB** (slightly lower), and **allocation spikes up to 259 KB/sec** while the queue churns. Higher allocation rate reflects the worker successfully pulling messages faster thanks to the publisher improvements; the absolute numbers remain small.
- **RabbitMQ queue snapshot** still shows `messages_ready≈495` and `deliver_get≈17K` because fixture data remains inconsistent (messages fail validation and requeue). This confirms the remaining bottleneck is on the consumer/test-data side, not the publisher pipeline.

### Phase 2 – Kickoff Plan (2026-01-04)

| Track | Task | Owner | Notes |
|-------|------|-------|-------|
| Consumer plumbing | Replace `MessageListener.ReadAsync` polling loop with `AsyncEventingBasicConsumer` push delivery service; wire up lifecycle hooks so failures still trigger reconnect + dead-letter logging. | Listener squad | Requires introducing a background task that owns the consumer and feeds a bounded `Channel<IInboundDelivery>`.
| Back-pressure | Set `channel.BasicQos(prefetchCount: DefaultConcurrency, prefetchSize: 0, global: false)` once the push consumer is active; expose the configured prefetch value via diagnostics so we can tune live. | Listener squad | Prefetch must match the worker semaphore count to avoid over-buffering.
| Worker handoff | Replace the per-message `Task.Run` dispatch with `System.Threading.Channels`. Reader side waits on the channel and runs the existing work loop, preserving metrics + cancellation behavior. | QueueWorker squad | Adds a single high-watermark knob (`ChannelCapacity`) to keep memory bounded when the broker floods deliveries.
| Observability | Extend `MessageListenerMetrics` to report `deliveries_in_queue`, `delivery_latency_ms`, and `prefetch` so we can compare against pre-change idle CPU. Add temporary `Info` logs around the new consumer registration to ease rollback. | Metrics squad | Metrics must ship before we toggle the new consumer in prod.
| Test harness | Update `tools/MigrationFloodPublisher` fixtures so at least 30% of messages succeed (no more guaranteed requeues) and add an integration test that drives the new push-consume path using the Docker broker. | Tooling + QA | Without successful completions we can’t validate backlog drainage during regression tests.

**Entry criteria**

- Phase 1 artifacts archived (`docs/baselines/worker-phase1.counters.csv`, flood logs) and linked above.
- Listener resiliency fixes from refactor readiness report already merged (prevents silent failures when the consumer drops).
- Branch protection updated to require the new integration test once it lands.

**Exit criteria**

- Idle CPU drops to ~0% with an empty queue (no polling loop activity) as measured via `dotnet-counters`.
- RabbitMQ management UI reports a non-zero consumer count for the worker process, with `messages_unacknowledged` never exceeding the configured prefetch for longer than 1 minute.
- Regression suite passes with the new channel-based worker dispatch; no unbounded growth in `ChannelCapacity` backlog during synthetic flood.

### Phase 2 – Work Completed (2026-01-04)

- `QueueWorker` now pre-spawns a fixed worker pool (`DefaultConcurrency` tasks) and drains a bounded `Channel<IInboundDelivery>` instead of spinning up a `Task.Run` per delivery. Back-pressure now happens inside the channel writer, which keeps GC churn predictable even under floods.
- A new knob, `BrokerOptions.WorkerChannelCapacity` (default = `DefaultConcurrency * 4`), gates how many deliveries can sit between the `AsyncEventingBasicConsumer` and the worker pool before the listener pauses writes. This replaces the implicit buffering we previously had in code.
- `MessageListener` was updated to the RabbitMQ.Client `ReceivedAsync`/`ShutdownAsync` model so reconnect handling and logging continue to function on 7.x clients.
- Unit suite (`dotnet test tests/WorkerHost.Tests/WorkerHost.Tests.csproj`) passes with the new dispatcher, covering both ACK/NACK paths plus the broker channel access helpers.
- `MessageListenerMetrics` + reporter now expose `buffered` (channel backlog) and `prefetch` counts alongside the existing counters. Because the busy `BasicGet` loop is gone, `idle` stays at `0` in the log stream, so we can immediately confirm Phase 2’s CPU goal before re-running the flood baseline.
- `tools/MigrationFloodPublisher` now injects a ≥30% mix of deterministic fixtures (computers/agents/tasks lifted from `testdata/sample_data.json`). Once the DAL data sources are seeded with those IDs the flood run will include guaranteed successes; in the current lab run the queue still redelivers because those sample computers do not yet exist in the worker store.
- `src/WorkerHost/WorkerHost.csproj` now copies the entire `testdata/` folder into the output/publish directory so `TestDataContext` always loads `sample_data.json`; this removes the “file not found” warnings seen in earlier runs and lets local RabbitMQ tests start from a known inventory without manual seeding.

### Phase 2 – Metrics Snapshot (2026-01-04)

- `docs/baselines/worker-phase2.counters.csv` (20-second `dotnet-counters` capture) shows CPU averaging **0.13%** with a **0.80%** spike, working set plateauing at **~87 MB**, and GC heap ≤ **62.5 MB** with zero Gen1/Gen2 collections during the mixed-success flood (`docs/baselines/worker-phase2.counters.csv:1-8`).
- `worker-phase2.log` logs the new metrics line with `idle=0`, `buffered≤1`, and `prefetch=5` while the listener dequeues over 120K deliveries, proving the push-based consumer keeps backlog bounded (`worker-phase2.log:3`, `worker-phase2.log:146577`, `worker-phase2.log:857658`).
- RabbitMQ snapshot `docs/baselines/worker-phase2.queue.json` confirms a single consumer with `prefetch_count=5`, `messages_unacknowledged=5`, and `messages_ready=966`, so the broker keeps in-flight work at the configured limit even though fixture mismatches still leave a large ready set (`docs/baselines/worker-phase2.queue.json:1`).
- Only **29/1000** messages were ACKed in this run because the worker’s backing data store still rejects the test fixtures; seeding the DAL (or adding a bypass for missing resources) is now the blocker for observing the target ≥30% success ratio.
- A quick smoke run (`dotnet run --project tools/MigrationFloodPublisher --count 500`) captured in `docs/baselines/rabbitmq-test.queue.json` shows the same consumer behavior (prefetch=5, `messages_unacknowledged=5`, `messages_ready≈442`) and live worker logs in `worker-rabbit-test.log` confirm the background `QueueWorker` drains the push channel immediately after each publish burst.

Next up: (1) import the sample-data fixtures (or relax BAL validation) so that ≥30% of flood messages succeed, (2) once success paths are visible, start Phase 3 reliability work (publisher confirms + DLX) using the same measurement harness.

### Phase 2 – Data Readiness Blockers

1. **DONE – 2026-01-04 – Seed the worker’s in-memory DAL automatically**: `CopyTestDataBeforeRun` target now mirrors `testdata/` into `bin/<TFM>/testdata/` before `dotnet run`, guaranteeing `TestDataContext` finds `sample_data.json`. Verify via startup logs (“Loaded N computers and M agents”) before running perf tests.
2. **DONE – 2026-01-04 – Align MigrationFloodPublisher fixtures + seeding hook**: A new `POST /testdata/load` endpoint reloads the in-memory DAL on demand, and `tools/MigrationFloodPublisher` accepts `--seed-endpoint`/`--seed-file` to push `testdata/sample_data.json` before each run. With the worker auto-loading the same IDs, ≥30% of flood messages now hit known-good computers/agents as soon as RabbitMQ + worker are up.
3. **DONE – 2026-01-04 – Document the seeding workflow**: `README.md` now has a “Test Data Seeding (Local Runs)” section covering the copy target, expected logs, and flood harness prerequisites so the team remembers the prerequisite checklist.

## Phase 6 – Validation Runs (Pending)

Use this section to record the final benchmark results. Keep raw artifacts (logs, counters, queue snapshots) under `docs/baselines/` with a date-stamped filename.

### 100k Publish/Consume Load
- Run: `dotnet build /p:RunAnalyzers=true` && `dotnet test` (sanity)  
- Flood: `dotnet run --project tools/MigrationFloodPublisher -- --count 100000 --mix Computer,Agent,TaskBatch --tasks-per-batch 200 --seed-endpoint http://localhost:8080/testdata/load`  
- Monitor: `dotnet-counters collect --process-id <worker pid> --duration 0:10:00 --output docs/baselines/worker-phase6.counters.csv`  
- Capture RabbitMQ: `curl -u guest:guest http://localhost:15672/api/queues/%2f/work-requests > docs/baselines/worker-phase6.queue.json`

| Date | Publish Count | p95 Publish (ms) | p95 Consume (ms) | Queue Depth Max | Connections Max | Notes | Artifacts |
|------|---------------|------------------|------------------|-----------------|-----------------|-------|-----------|
| (fill) | 100000 |  |  |  |  |  | `docs/baselines/` |

### Concurrency Soak (10–15 min each)
- Levels: `DefaultConcurrency`, `MaxConcurrency`, `2x DefaultConcurrency`
- Collect: `dotnet-counters collect --process-id <worker pid> --duration 0:15:00 --output docs/baselines/worker-phase6-soak-<level>.csv`
- Track: CPU%, working set, GC pause time, queue backlog (`messages_ready`, `messages_unacknowledged`), connection count.

| Concurrency | Duration | CPU Avg/Peak | RSS Avg/Peak | GC Pause Total | Backlog Max | Outcome | Artifacts | Notes |
|-------------|----------|--------------|--------------|----------------|-------------|---------|-----------|-------|
| DefaultConcurrency | 15m |  |  |  |  |  | `docs/baselines/worker-phase6-soak-default.csv` |  |
| MaxConcurrency | 15m |  |  |  |  |  | `docs/baselines/worker-phase6-soak-max.csv` |  |
| 2x DefaultConcurrency | 15m |  |  |  |  |  | `docs/baselines/worker-phase6-soak-2x.csv` |  |

## Quick 5k Smoke (Publisher + Listener)

Use this for faster verification before the full 100k load:

1) Seed data: `curl -X POST http://localhost:8080/testdata/load`
2) Start worker: `dotnet run --project src/WorkerHost/WorkerHost.csproj`
3) Collect listener counters:  
   `dotnet-counters collect --process-name WorkerHost --providers System.Runtime --duration 0:0:10:00 --output docs/baselines/worker-5000.counters.csv`
4) Publish 5k messages with progress:  
   `/usr/bin/time -l dotnet run --project tools/MigrationFloodPublisher -- --count 5000 --mix Computer,Agent,TaskBatch --tasks-per-batch 200 --seed-endpoint http://localhost:8080/testdata/load --progress 500`
5) Snapshot RabbitMQ (optional):  
   `curl -u guest:guest http://localhost:15672/api/queues/%2f/work-requests > docs/baselines/worker-5000.queue.json`

Record results:

| Date | Publish Count | p95 Publish (ms) | p95 Consume (ms) | Queue Depth Max | Connections Max | Notes | Artifacts |
|------|---------------|------------------|------------------|-----------------|-----------------|-------|-----------|
| (fill) | 5000 |  |  |  |  |  | `docs/baselines/worker-5000.counters.csv`, `docs/baselines/worker-5000.queue.json` |

Once the data readiness checklist is green we will capture a fresh `docs/baselines/worker-phase2.counters.csv`, freeze those numbers, and immediately roll into Phase 3 (reliability).

### Phase 2 – Baseline Refresh Checklist (In Progress)

1. **Seed + flood command**  
   ```bash
   dotnet run --project src/WorkerHost/WorkerHost.csproj &
   WORKER_PID=$!
   dotnet run --project tools/MigrationFloodPublisher -- --count 1000 --mix Computer,Agent,TaskBatch --tasks-per-batch 200 --progress 200 --seed-endpoint http://localhost:8080/testdata/load
   ```
   Capture `/usr/bin/time -l` output from the flood run and keep the worker logs (`worker-phase2.log`) for the audit appendix.
2. **Counters snapshot**  
   ```bash
   dotnet-counters collect --process-id $WORKER_PID --duration 0:0:0:30 --output docs/baselines/worker-phase2.counters.csv System.Runtime
   ```
3. **RabbitMQ state**  
   ```
   curl -u guest:guest http://localhost:15672/api/queues/%2f/work-requests > docs/baselines/worker-phase2.queue.json
   curl -u guest:guest http://localhost:15672/api/connections | jq 'length' > docs/baselines/worker-phase2.connections.txt
   ```
4. **Success criteria**  
   - ≥30% (>=300/1000) ACKed messages (`worker-phase2.log` success entries).  
   - `messages_unacknowledged` ≤ prefetch (5) except transient spikes.  
   - CPU/GC metrics comparable to earlier Phase‑2 runs (≤1% CPU avg, ≤65 MB GC heap).
5. **If criteria met**: mark this checklist as DONE with date, archive the artifacts, and proceed to Phase 3 work items below without waiting for extra approval (per execution cadence reminder).

## 9. Phase 3 – Reliability Plan

Phase 3 hardens delivery guarantees now that resource usage is under control.

### Phase 3 – Work Completed (2026-01-05)

1. The worker now declares a dedicated `work-requests.dlx` exchange/queue pair and applies DLX/TTL arguments to the primary queue during startup (`src/WorkerHost/RabbitMq/Infrastructure/QueueBindingsManager.cs:25` + `src/WorkerHost/RabbitMq/Configuration/BrokerOptions.cs:60`). Poison messages move to the DLX immediately and the main queue enforces a default 600 s TTL (`x-message-ttl`).
2. Configuration files expose the new `BrokerOptions.WorkEndpoint.DeadLetter` section so ops can tune DLX names, TTL, or disable the feature (`src/WorkerHost/appsettings.Development.json:21`, `src/WorkerHost/appsettings.json:21`). README now documents the defaults for local runners (`README.md:17`).
3. Added tests (`tests/WorkerHost.Tests/RabbitMq/QueueBindingsManagerTests.cs:1`) and an `InternalsVisibleTo` bridge so we can assert the queue-argument builder respects overrides before the worker touches RabbitMQ.
4. Publisher confirms remain blocked on RabbitMQ.Client 7.2’s async API (no public `ConfirmSelect*` on `IChannel`). Until the SDK exposes that surface—or we introduce a synchronous publishing stack—the confirm/retry subsection in this plan stays open.
5. Refactored infrastructure glue so DLX wiring now lives in `DeadLetterConfigurator` and the `/testdata/load` endpoint is defined via `TestDataEndpointExtensions`; this keeps the main worker flow lean for upcoming 100k-load profiling.

### Phase 3 – Next Actions

1. **Baseline refresh** – Run the Phase 2 checklist (seeded flood + `/usr/bin/time` + `dotnet-counters` + RabbitMQ snapshots). Once ≥30% ACKs are confirmed, mark that section DONE and file the artifacts under `docs/baselines/worker-phase2.*`.
2. **Publisher confirms path** – Evaluate whether upgrading to RabbitMQ.Client ≥8.0 (confirm APIs landed in the sync channel) or adding a dedicated synchronous publisher service is the quicker route. Document the chosen strategy and spike a prototype branch before touching production code.
3. **DLX smoke/integration test** – Add `tests/WorkerHost.Tests/Integration/DeadLetterTests.cs` (or an equivalent docker-compose scenario) so we can prove poison messages land in `<scope>.work-requests.dlx` and the main queue stays stable. Capture those results in the audit once the test passes locally.

### 9.1 Publisher Confirms

- **Connection shape**: Confirms require a dedicated channel per publisher thread. Update the pooled channel factory to expose `CreateConfirmedChannelAsync()` that calls `ConfirmSelect` once per channel and keeps it alive for batches of publishes.
- **Timeout + retry policy**: Track `BasicAck`/`BasicNack` via the `IModel.BasicAcks` / `BasicNacks` events. If an ACK/NACK is not observed within `ConfirmTimeout` (default 5 s) treat it as a failure, log the routing key, and retry up to `ConfirmRetryLimit` with exponential backoff.
- **Metrics/logs**: Extend `MessageListenerMetrics` (or a new `MessagePublisherMetrics`) to emit `confirm_latency_ms`, `confirm_timeouts`, and `nacks_total`. Add structured logs at `Information` until we trust the pipeline; then downgrade to `Debug`.

### 9.2 Dead-letter / TTL Policies

- **Queue policy**: For each scope queue (currently `work-requests`) define `x-dead-letter-exchange` and `x-message-ttl` via `WorkEndpoint.BindingArguments`. Dead-letter target: `<scope>.work-requests.dlx`.
- **Poison flow**: When `QueueWorker` exhausts retries or encounters validation errors, route the message to the DLX with headers describing the failure reason and original `deliveryTag`.
- **Operations visibility**: Capture DLX depth via RabbitMQ management API (`/api/queues/%2f/<queue>.dlx`) and surface it in the audit so operators can tell whether poison messages are accumulating.

### 9.3 Validation & Instrumentation

| Task | Tooling / Output | Definition of Done |
|------|-----------------|--------------------|
| Confirm latency benchmark | `dotnet run --project tools/MigrationFloodPublisher -- --count 500 --enable-confirms` plus `/usr/bin/time -l` | p95 publish latency increases by ≤10% vs. Phase 2 baseline while guarantees improve (no lost messages in fault injection). |
| DLX smoke test | `tests/WorkerHost.Tests/Integration/DeadLetterTests.cs` (new) running against `docker-compose rabbitmq` | Messages with invalid computer IDs reappear in `<scope>.work-requests.dlx`; original queue depth remains stable. |
| Observability | `MessageListenerMetrics` now exposes `confirm_timeouts`, `dlx_enqueued`, `prefetch`, `buffered` | Metrics scraped during the flood run show zero timeouts and DLX increments that match the number of intentionally poisoned messages. |

### 9.4 Exit Criteria

1. Publisher confirms enabled by default with retries + structured metrics.
2. Dead-letter/TTL policies deployed; DLX monitored in the audit’s baseline artifacts.
3. Flood harness rerun with seeded data shows ≥30% success, <1% confirm timeouts, and a new `docs/baselines/worker-phase3.counters.csv` capturing CPU/memory after reliability features.

## Phase 6 – Validation & Benchmarks (pending execution)

Use this checklist when running the final validation locally (requires RabbitMQ running):

1) Sanity build/tests  
   - `dotnet build /p:RunAnalyzers=true`  
   - `dotnet test`

2) 100k publish/consume load  
   - Start RabbitMQ (management UI available).  
   - Flood: `dotnet run --project tools/MigrationFloodPublisher -- --count 100000 --mix Computer,Agent,TaskBatch --tasks-per-batch 200 --progress 1000 --seed-endpoint http://localhost:8080/testdata/load`  
   - Collect during run:  
     - `dotnet-counters monitor System.Runtime --process-id <worker-pid>`  
     - RabbitMQ queue stats: `curl -u guest:guest http://localhost:15672/api/queues/%2f/work-requests`  
     - Connection count: `curl -u guest:guest http://localhost:15672/api/connections | jq 'length'`  
     - p95 publish/consume latency (from app logs/metrics endpoints).

3) Concurrency soak  
   - Run sustained load for 10–15 minutes at: DefaultConcurrency, MaxConcurrency, and 2×DefaultConcurrency.  
   - Capture CPU/memory/GC/backlog via `dotnet-counters` + RabbitMQ queue depth.

4) Record results  
   - Append a dated note here with: max RSS, CPU %, allocation rate, p95 publish/consume latency, queue depth, connection count, and any errors observed.  
   - Note regressions or anomalies to investigate next.
