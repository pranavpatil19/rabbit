Logging & Observability
=======================

Overview
--------
- Serilog is configured via `appsettings*.json` (see `Serilog` section) and wired up in `Program.cs` using `UseSerilog`.
- Logs are written in rendered compact JSON format to stdout, enriched with `ThreadId`, `Application`, and contextual properties (e.g., `MigrationId`, `Scope`).

Log Queue & Drain
-----------------
- Component classes enqueue `MigrationLogEvent` instances into `ILogQueue` (`ChannelLogQueue` implementation) to maintain ordering of per-migration events even when work executes concurrently.
- `LogDrainService` is a hosted background service that drains the channel and writes each entry through `ILogger`/Serilog with the original properties/arguments.
- Typical producers:
  - `QueueWorker` (queue events: dequeued, success, error, requeue flag).
  - `BalMigrationService` (job lifecycle events: InProgress, Completed, Failed + counts).

Metrics & Health
----------------
- `/health` returns a simple status payload so orchestrators can probe the service.
- `/metrics` reads the in-memory migration job snapshots (`IMigrationJobStore.ListAsync`) and reports aggregate agent/task counts plus active job totals. This is a placeholder spot where Prometheus scraping can be added later.

Extending
---------
- Add additional sinks through `appsettings.*` (e.g., Seq, file) without code changes.
- To augment per-message logging, enqueue more `MigrationLogEvent` instances with custom properties; they will flow through the same ordered drain and appear in Serilog output.
