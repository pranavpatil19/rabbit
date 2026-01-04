# RabbitMQ · Logging · Background – Refactor Readiness Report

Deep-dive audit covering the messaging pipeline (`src/WorkerHost/RabbitMq`), logging stack (`src/WorkerHost/Logging`), and background services (`src/WorkerHost/Background`). Every observation below points to concrete files/behaviours so we can remediate systematically.

---

## 1. Findings – Current Pain Points

### 1.1 Channel lifecycle + confirmation handling duplicated
- `QueueWorker` (`Background/QueueWorker.cs:74-156`) manually guards `BasicAckAsync`/`BasicNackAsync` with a semaphore, while `MessageListener` (`RabbitMq/Messaging/MessageListener.cs:90-167`) owns its own channel-reset logic.  
- Tests reimplement fake channel flows (`tests/Background/QueueWorkerTests.cs:118-159`). Because this logic is scattered, diagnosing leaks or double ACKs requires tracing three separate implementations.

### 1.2 Logging lacks a unified contract
- BAL (`Bal/BalMigrationService.cs`), worker, and log drain service enqueue anonymous dictionaries shaped differently per callsite.  
- Structured metadata (migration id, scope, retry flags) is duplicated in every `EmitLogAsync` invocation, making log format changes risky and debugging harder when fields are accidentally omitted.

### 1.3 Background polling boilerplate everywhere
- `QueueWorker`, `LogDrainService`, `MessageListener`, and `MessageListenerMetricsReporter` each implement copy/pasted `while (!token.IsCancellationRequested)` loops with custom delay/exception handling.  
- Any change (e.g., instrumentation, jitter, backoff) requires touching every loop, and it’s easy to miss one (hidden bugs during graceful shutdown).

### 1.4 Configuration guidance fragmented
- `appsettings.json` and `appsettings.Development.json` contain long inline comments describing each option, while `docs/rabbitmq-config.md` repeats similar text.  
- `BrokerOptions` properties lack XML documentation, so IDE tooltips show nothing. Engineers must cross-reference multiple files, slowing debugging when a misconfigured value causes runtime failures.

### 1.5 Folder boundaries & coupling
- Background worker references RabbitMQ-specific classes (`MessageDelivery`, `IChannel`) directly. Logging references worker-specific DTOs. There is no thin abstraction between RabbitMQ and the consumer, making it hard to swap transport details or test components in isolation.

---

## 2. Refactor Direction – How We Fix It

1. **Central channel service:** Introduce an abstraction (`IBrokerChannelAccess`) that encapsulates connection management, message confirmations, and reconnection throttling. `QueueWorker` should depend on this instead of touching `IChannel` directly.
2. **Structured logging model:** Define `MigrationLogEvent` (with scope/id/severity/payload) plus a builder to ensure every log entry carries consistent metadata. `LogDrainService` becomes a simple serializer/dispatcher.
3. **Reusable polling helper:** Implement a `PollingBackgroundService` base (or helper) that standardizes loop lifecycle, cancellation, backoff, and exception logging. Derive `QueueWorker`, `LogDrainService`, and `MessageListenerMetricsReporter` from it.
4. **Single source of truth for configuration descriptions:** Keep reference docs in `docs/rabbitmq-config.md`, prune redundant JSON comments, and document `BrokerOptions` properties with XML summaries so IDE hints are accurate.
5. **Layered folder responsibilities:** RabbitMQ layer exposes interfaces (`IMessagePublisher`, `IMessageListener`, `IBrokerChannelAccess`). Background/logging layers consume only those interfaces and shared DTOs under `Common`, reducing cross-namespace leakage.

---

## 3. Implementation Steps (Detailed & Prioritized)

1. **Structured logging contract** ✅ *Completed*  
   - Added `MigrationLogEvent` + builder under `Logging/`.  
   - BAL + worker emit via the builder; `ILogQueue`/`LogDrainService` serialize the new event type.

2. **Broker channel abstraction** ✅ *Completed*  
   - Implemented `IBrokerChannelAccess`/`BrokerChannelAccess` to handle confirmations + channel resets.  
   - `QueueWorker` and tests now depend on the abstraction instead of raw `IChannel`.

3. **Reusable polling helper** ✅ *Completed*  
   - `PollingBackgroundService` lives in `Common/Background`.  
   - `MessageListenerMetricsReporter`, `LogDrainService`, and `QueueWorker` now inherit from it, so all background loops share the same lifecycle semantics.

4. **Config/documentation alignment** ✅ *Completed*  
   - `appsettings*.json` now contain only data (no descriptive comments).  
   - `BrokerOptions` and nested records expose XML `<summary>` docs for IDE hints.  
   - README points to `docs/rabbitmq-config.md`, which remains the authoritative reference table for all options.

5. **Folder/interface tidy-up** ✅ *Completed*  
   - Background worker/logging layers now depend only on `WorkerHost.Messaging` abstractions (`IInboundDelivery`, `IMessageListener`, `IBrokerChannelAccess`).  
   - RabbitMQ-specific types (`MessageDelivery`, `BasicGetResult`, `IChannel`) remain encapsulated under `RabbitMq/*`, and non-RabbitMQ folders no longer import `RabbitMQ.Client`.

Each step should land in separate commits/PRs wherever possible, accompanied by updated unit tests (`QueueWorkerTests`, `MessageListener*Tests`, logging tests) to prevent regressions.

---

## 4. Next Actions Before Coding

1. **Stakeholder review:** Confirm proposed abstractions match roadmap requirements (e.g., potential multi-queue support).
2. **Naming alignment:** Agree on interface/class names up front to avoid churn mid-refactor.
3. **Prioritization & rollout plan:** Follow the updated priority (finish polling-helper adoption → config/docs alignment → folder cleanup) and decide whether any changes need feature flags or incremental rollouts for production safety.

With the channel service and structured logging already merged, the immediate next milestone is finalizing the polling helper rollout (decision on `QueueWorker`) before moving to configuration/doc fixes.
