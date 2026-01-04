# RabbitMQ & Background Shared-Library Plan

Goal: extract reusable RabbitMQ + background infrastructure from `src/WorkerHost` into a shared class library while keeping the folder layout, logging model, and hosting glue consistent across future services. This document captures the current structure, desired end state, and the step-by-step rollout sequence. Each step must be executed and reviewed independently before moving to the next.

---

## 1. Current Layout (Snapshot)

| Folder | Purpose | Notes |
|--------|---------|-------|
| `src/WorkerHost/Api` | Minimal API endpoints + validators | No RabbitMQ dependencies |
| `src/WorkerHost/Background` | `QueueWorker`, `ConcurrencyPlan`, worker DI extensions | Depends on RabbitMQ messaging abstractions |
| `src/WorkerHost/Bal` | Business orchestration | Consumes DAL + logging helpers |
| `src/WorkerHost/Common` | Shared helpers (`PollingBackgroundService`, async extensions) | Already transport-agnostic |
| `src/WorkerHost/Configuration` | Option models (e.g., `TestDataOptions`, `BrokerOptions`) | Broker config lives here |
| `src/WorkerHost/Dal` | In-memory stores and seeding | No RabbitMQ coupling |
| `src/WorkerHost/Logging` | `MigrationLogContext`, log templates | No queue dependency |
| `src/WorkerHost/Messaging` | `MigrationCommand`, DTO factories, `IInboundDelivery` | Transport-neutral abstractions |
| `src/WorkerHost/RabbitMq` | Configuration defaults, DI extensions, listener/publisher/channel access | Tightly coupled to WorkerHost |

Pain points: RabbitMQ code, logging helpers, and background base classes all live under the WorkerHost app, making reuse tricky. We want a shared class library (e.g., `Rabbit.Infrastructure`) hosting transport + background primitives, and a lean WorkerHost referencing that library.

---

## 2. Target Architecture (High Level)

```
src/
  Shared/
    Rabbit/
      Configuration/ (BrokerOptions, defaults, extensions)
      Messaging/ (MessageListener, Publisher, BrokerChannelAccess, channel pool)
      Monitoring/ (health checks, metrics)
  Shared/
    Background/
      PollingBackgroundService, ConcurrencyPlan, utility channels
    Logging/
      MigrationLogContext, templates, Serilog helpers
  WorkerHost/
    Api/
    AppHost/ (Program.cs & DI wiring referencing shared libs)
    Bal/, Dal/, Common/, etc.
```

WorkerHost stays responsible for BAL/DAL/API wiring but consumes Rabbit/background/logging helpers from the shared library.

---

## Progress
- **Step 1**: ✅ `WorkerInfrastructure` shared project created (`src/Shared/WorkerInfrastructure`). `Common/Background`, `Common/Extensions`, `Common/Models`, and the shared `MigrationScope` enum now live there, and `WorkerHost` references the project.
- **Step 2**: ✅ Logging helpers (`MigrationLogContext`, `MigrationLogTemplates`) and the entire messaging DTO set (`src/Shared/WorkerInfrastructure/Messaging`) now reside in the shared library; WorkerHost consumes them via the project reference.
- **Step 3**: ✅ Broker configuration (`BrokerOptions`, `BrokerDefaults`) now live under `src/Shared/WorkerInfrastructure/RabbitMq`, keeping WorkerHost’s DI bindings pointed to the shared types.
- **Step 4**: ✅ RabbitMQ messaging infrastructure (listener, publisher, channel access, pools, resource cleaners, and queue-binding helpers) has moved into the shared project; WorkerHost references them via DI extensions.
- **Step 5**: ✅ Monitoring/health components (`MessageListenerHealthCheck`, metrics reporters, publisher metrics) now reside under `src/Shared/WorkerInfrastructure/RabbitMq/Monitoring`. Shared project references include `Microsoft.Extensions.Diagnostics.HealthChecks`.
- **Step 6**: ✅ Documentation + packaging updates finished; shared RabbitMQ folders are now grouped by feature (Channels, Listener, Publisher, Infrastructure, Monitoring) to make ownership obvious.

## Final Shared Folder Layout

```
src/Shared/WorkerInfrastructure/
  Common/             # PollingBackgroundService, ConcurrencyPlan, async helpers
  Logging/            # MigrationLogContext, templates
  Messaging/
    Abstractions/     # IMessagePublisher, IInboundDelivery
    Contracts/        # MigrationCommand + payload records/enums
    Serialization/    # Payload factories, Json options
  RabbitMq/
    Configuration/    # BrokerOptions + defaults
    Infrastructure/   # Queue bindings, dead-letter, connection builders
    Channels/         # IBrokerChannelAccess + implementations
    Listener/         # MessageListener, ListenerChannelManager
    Publisher/        # MessagePublisher, channel pool
    Monitoring/       # Metrics, reporters, health checks
```

All WorkerHost projects (API, background worker, tooling, tests) reference these shared components via `WorkerInfrastructure.csproj`.

## 3. Step-by-Step Execution Plan

### Step 1 – Extract Background/Common Layer
1. Create `src/Shared/Background` project (class library) that will also host the reusable pieces currently under `WorkerHost.Common`.
2. Move `PollingBackgroundService`, `ConcurrencyPlan`, `QueueWorker` channel helpers, and any other background-specific utilities.
3. Relocate truly generic helpers (`Common/Extensions`, `Common/Models` that are used outside WorkerHost) into the same shared project or a sibling `Shared/Common` folder so future services can reference them without pulling in the full app.
4. Update namespaces + references so WorkerHost consumes the new library.
4. Run `dotnet test`.  
✅ **Review checkpoint:** ensure other projects (future services) can reference the new library without WorkerHost dependencies.

### Step 2 – Extract Logging Helpers
1. Create `src/Shared/Logging` (or reuse background library) for `MigrationLogContext`, `MigrationLogTemplates`, and any logger extension helpers.
2. Update WorkerHost logging references; ensure Serilog configuration stays in appsettings.
3. Verify there are no remaining WorkerHost-specific hard references inside the shared logging folder.  
✅ **Review checkpoint:** confirm logs still contain `MigrationId`/`Scope` and ordering via `WriteTo.Async`.

### Step 3 – Modularize RabbitMQ Configuration & Abstractions
1. Create `src/Shared/Rabbit/Configuration` housing `BrokerOptions`, defaults, DI extensions, and XML docs.
2. Ensure `Program.cs` (WorkerHost) binds options via the shared project, not local files.
3. Move docs referencing config (e.g., `docs/rabbitmq-config.md`) under the shared module if they apply to multiple apps.  
✅ **Review checkpoint:** confirm no app-specific types leak into configuration models.

### Step 4 – Extract RabbitMQ Messaging Components
1. Move `MessageListener`, `ListenerChannelManager`, `MessagePublisher`, `BrokerChannelAccess`, `PublisherChannelPool`, and monitoring helpers under `src/Shared/Rabbit/Messaging`.
2. Adjust namespaces and DI wiring: WorkerHost references shared messaging DI extension methods.
3. Update tests to reference the shared project; keep WorkerHost tests focused on integration logic.  
✅ **Review checkpoint:** `dotnet test` across solution + sample integration ensures messaging works post-move.

### Step 5 – Shared Monitoring & Health Checks
1. Move `MessageListenerHealthCheck`, metrics reporters, and any Rabbit-specific telemetry into `Shared/Rabbit/Monitoring`.
2. Ensure WorkerHost registers the shared health checks via DI extension.  
✅ **Review checkpoint:** `/health` and `/metrics` still surface expected data.

### Step 6 – Documentation & Packaging
1. Update `docs/logging.md`, `docs/rabbitmq-processing.md`, and other references to point to the shared libraries.
2. Add README section describing how to consume the new shared packages from other services.
3. Optionally publish the shared projects as NuGet packages (internal feed) or link them as project references.
4. Final `dotnet test` + optional load test to validate nothing regressed.  
✅ **Review checkpoint:** confirm all references to old folders are removed; folder tree mirrors the target architecture diagram.

---

## 4. Rollout Notes
- Keep commits/PRs scoped per step to simplify review.
- Maintain backward compatibility during each step (e.g., keep old namespaces forwarding until consumer code migrates).
- After each step, update this document (Progress section) marking the step as completed.
- Document any breaking changes (namespace moves, DI extension renames) in the CHANGELOG so downstream services can consume the shared libraries smoothly.

This plan ensures we move RabbitMQ/background/logging infrastructure into a reusable shared library in a controlled, reviewable sequence without disrupting the WorkerHost application.
