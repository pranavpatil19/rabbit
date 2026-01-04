# Performance Refactor Task List

Actionable items to streamline RabbitMQ publish/consume paths. Execute in order and record metrics after each phase.

## Execution Order (current plan)
1) Listener pump & prefetch (Section 1)
2) Worker processing hot path (Section 2)
3) Publisher + pooling optimizations (Section 3)
4) Config/knobs exposure (Section 4)
5) Logging/error handling standardization (Section 5)
6) Validation & benchmarks (Section 6)

## 1. MessageListener (consumer pump)
- [x] Replace polling with `AsyncEventingBasicConsumer` and ensure `BasicQos(prefetchCount: DefaultConcurrency)` is set.
- [x] Bound delivery buffering with `Channel<IInboundDelivery>` (cap = `DefaultConcurrency * 4`) and pause writes when full.
- [x] Centralize reconnect/backoff delays; avoid tight retry loops after shutdown.
- [x] Emit metrics: `prefetch`, `deliveries_buffered`, `reconnects`, `latency_ms` (from delivery to enqueue).

## 2. QueueWorker (processing path)
- [x] Keep worker count fixed (`DefaultConcurrency`), reuse the bounded channel; avoid per-message `Task.Run`.
- [x] Move BAL dispatch to a helper/service to shrink the hot path; keep only orchestration + logging in the worker.
- [x] Add structured success/failure logs with payload bytes and elapsed time; ensure NACKs include requeue decision.
- [x] Consider a lightweight backoff for repeated failures on the same migration to reduce churn.

## 3. Publisher & Pooling
- [x] Ensure `MessagePublisher` uses `JsonSerializer.SerializeToUtf8Bytes` (no intermediate strings) and reuses `BasicProperties`.
- [x] Keep `PublisherChannelPool` size tuned (`DefaultConcurrency`); log pool hits/misses at `Debug`.
- [x] Add a small publish metrics helper: `publish_latency_ms`, `payload_bytes` (pool hit logged at Debug).
- [x] Verify connections/channels are closed via `BrokerResourceCleaner` to prevent leaks.

## 4. Configuration & Defaults
- [x] Validate all RabbitMQ settings come from `appsettings`; fall back to `BrokerDefaults` when missing.
- [x] Expose key knobs via `/metrics` or logs: `defaultConcurrency`, `maxConcurrency`, `workerChannelCapacity`, `prefetch`.
- [x] Add guardrails for extreme values (e.g., cap prefetch/channel capacity at a sane upper bound).

## 5. Logging & Error Handling
- [x] Standardize catch blocks: `Warning` for expected shutdown, `Error` for processing faults; no empty catches.
- [x] Deduplicate log templates for publish success/failure and worker ACK/NACK to reduce string churn.
- [x] Keep shutdown logs minimal but present (one line per component).

## 6. Validation & Benchmarks
- [x] `dotnet build /p:RunAnalyzers=true` and `dotnet test` after each phase.
- [ ] Run a 100k publish/consume load; capture `dotnet-counters`, RabbitMQ queue depth, connection counts, and p95 latency for publish/consume.
- [ ] Record before/after metrics in `docs/rabbitmq-resource-audit.md` (or a dated appendix) to show gains.
- [ ] Concurrency soak test: run sustained load at `DefaultConcurrency`, `MaxConcurrency`, and 2x `DefaultConcurrency` for 10–15 minutes; track CPU/memory, GC pauses, and backlog growth to validate there are no deadlocks or leaks under pressure.
  - Reference commands/result tables: see `docs/rabbitmq-resource-audit.md` Phase 6 section.
  - Execution log placeholders (fill after each run):
    - [ ] 100k load: date ___, p95 publish ___ ms, p95 consume ___ ms, queue depth max ___, connections max ___ (artifacts: `docs/baselines/`).
    - [ ] Soak @ DefaultConcurrency: CPU avg/peak ___/___, RSS avg/peak ___/___, backlog max ___ (artifacts: `worker-phase6-soak-default.csv`).
    - [ ] Soak @ MaxConcurrency: CPU avg/peak ___/___, RSS avg/peak ___/___, backlog max ___ (artifacts: `worker-phase6-soak-max.csv`).
    - [ ] Soak @ 2x DefaultConcurrency: CPU avg/peak ___/___, RSS avg/peak ___/___, backlog max ___ (artifacts: `worker-phase6-soak-2x.csv`).
  - Status: blocked on running the load/soak locally; fill the placeholders above and mirror details into `docs/rabbitmq-resource-audit.md` to complete Section 6.
