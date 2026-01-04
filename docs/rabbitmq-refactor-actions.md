# RabbitMQ Refactor Action List (Fresh Copy)

This file lists concrete steps to reduce duplication and simplify the publish/listen/worker code. It is independent of prior plans—treat this as the current source of truth for refactor tasks.

## A. Preparation
- [ ] Capture a fresh 100k publish/consume baseline (CPU, memory, queue depth, connection count).
- [ ] Snapshot current analyzer/Sonar warnings.

## B. Deduplicate Shared Behaviors
- [ ] Extract `MigrationPayloadFactory` used by `QueueWorker` (and any future publisher tests) to build `Computer/Agent/TaskBatch` payloads. Remove inline `Create*Payload` methods.
- [ ] Create a common `JsonSerializerOptions` helper so publisher and consumer share the same settings.
- [ ] Introduce `WorkerConcurrencyPlan` that:
  - Accepts `BrokerOptions`.
  - Exposes `GlobalSemaphore`, `ScopeSemaphores`, `WorkerCount`.
  - Implements `DisposeAsync` for semaphore cleanup.
  - Is injected/used by `QueueWorker` instead of manual wiring.
- [ ] Add a small helper for connection/channel close with logging; reuse in `PublisherChannelPool` and `MessageListener` reset paths.
- [ ] Add shared logging templates for publish success/failure and worker ACK/NACK to eliminate duplicated message strings and property bags.
- [ ] Add a metrics helper that emits duration + payload size for both publish and consume paths.

## C. Reliability & Error Handling
- [ ] Standardize catch blocks: `Warning` for expected shutdown cancellation; `Error` for failures; no empty catches.
- [ ] Optionally add a toggle to rethrow fatal errors after logging (config-driven) if desired in production.

## D. Structure & Readability
- [ ] Keep regions minimal; prefer small private methods and meaningful names.
- [ ] Consider moving BAL dispatch (`DispatchAsync`) into a dedicated service to keep `QueueWorker` focused on orchestration.

## E. Validation
- [ ] `dotnet build /p:RunAnalyzers=true` and `dotnet test`.
- [ ] Repeat the 100k baseline and compare allocations/latency/CPU with the initial snapshot.
- [ ] Update docs with results and remaining risks.
