# RabbitMQ Worker Refactor Plan

Goal: keep publish/listen/worker code simple, fast, and readable while avoiding duplicate logic and empty catches. Below is a step-by-step sequence you can execute incrementally.

## Phase 1 – Preparation
- [ ] Baseline run: execute 100k publish/consume run to capture CPU, memory, and broker stats. Keep logs for before/after comparisons.
- [ ] Snapshot current Sonar/IDE warnings to verify improvement later.

## Phase 2 – Reduce duplication
- [ ] Extract `MigrationPayloadFactory` (static class or service) from `QueueWorker` to build `Computer/Agent/TaskBatch` payloads. Replace inline `Create*Payload` methods with calls to this factory.
- [ ] Move deserialization options (`JsonSerializerOptions`) to a single shared factory/helper so publisher/consumer can reuse the same settings if needed.
- [ ] Wrap concurrency wiring in `WorkerConcurrencyPlan`:
  - Accept `BrokerOptions` in the constructor.
  - Expose `GlobalSemaphore`, `ScopeSemaphores`, `WorkerCount`.
  - Provide a `DisposeAsync` to clean up semaphores.
  - `QueueWorker` ctor uses the plan instead of hand-building semaphores and worker count.
- [ ] Override shared behaviors via small helpers instead of copy/paste:
  - Connection/Channel lifecycle: one helper that closes & logs failures; used by publisher pool + listener reset.
  - Logging templates: single helper for success/failure/NACK messages so worker/publisher share the same wording and properties.
  - Metrics emission: one helper to emit duration + payload size from both publish and consume paths.

## Phase 3 – Error handling and logging consistency
- [ ] Standardize catch blocks: log at `Warning` when shutdown cancels work; `Error` when processing fails; never leave a catch empty.
  - `MessageListener`, `BrokerChannelAccess`, `PublisherChannelPool`, `QueueWorker` should all follow the same pattern.
- [ ] Consider a small `IBrokerLifecycleLogger` helper to centralize channel/connection close logging (optional if logs are already minimal).

## Phase 4 – Structure and readability
- [ ] Keep regions minimal; prefer small private methods and purposeful naming over large regions.
- [ ] Move BAL payload dispatch logic (`DispatchAsync`) to a dedicated service if tests would be easier with that separation.
- [ ] Add XML summaries only where intent isn’t obvious; keep inline comments for non-obvious resource controls (e.g., why a semaphore exists).

## Phase 5 – Validation
- [ ] `dotnet build /p:RunAnalyzers=true` and `dotnet test`.
- [ ] Repeat the 100k publish/consume run; compare memory/CPU/broker metrics to the baseline from Phase 1.
- [ ] Verify no unhandled empty catches and no new Sonar warnings.

## Phase 6 – Rollout
- [ ] Update `docs/rabbitmq-resource-audit.md` with changes, results, and remaining risks.
- [ ] Keep the load-test artifacts (logs, metrics) with timestamps for traceability.
