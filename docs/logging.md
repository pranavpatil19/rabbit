Logging & Observability
=======================

Overview
--------
- Serilog is configured via `appsettings*.json` (see `Serilog` section) and wired up in `Program.cs` using `UseSerilog`.
- Logs are written in rendered compact JSON format to stdout, enriched with `ThreadId`, `Application`, and contextual properties (e.g., `MigrationId`, `Scope`).

Ordered Logging
---------------
- Serilog’s `WriteTo.Async(...)` sink serializes every log entry before it reaches the configured outputs, so even though worker/BAL code runs concurrently the emitted logs appear in chronological order.
- `MigrationLogContext.Push(...)` stamps `MigrationId` + `MigrationScope` into `LogContext`, and helpers such as `MigrationLogContext.PushProperties(...)` let callers add per-entry metadata (elapsed time, payload size, etc.).
- Producers (`QueueWorker`, `BalMigrationService`, and any future components) just use the normal `ILogger<T>` API; no custom DTOs or drain services remain in the pipeline.

Metrics & Health
----------------
- `/health` returns a simple status payload so orchestrators can probe the service.
- `/metrics` reads the in-memory migration job snapshots (`IMigrationJobStore.ListAsync`) and reports aggregate agent/task counts plus active job totals. This is a placeholder spot where Prometheus scraping can be added later.

Extending
---------
- Add additional sinks through `appsettings.*` (e.g., Seq, file) without code changes—wrap them in `WriteTo.Async` if ordering still matters.
- To include more context on specific log entries, push temporary properties via `MigrationLogContext.PushProperties(...)` or create custom Serilog enrichers; they automatically flow to every configured sink.
