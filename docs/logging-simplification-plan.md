# Logging Simplification Plan

Goal: keep logs in strict chronological order without inventing a custom file format. We’ll replace the bespoke queue/drain pipeline with Serilog’s async sink plus enrichers, but we will execute the transition in reviewable steps. Each step should be developed and reviewed before moving to the next.

## Progress
- **Step 1**: ✅ Serilog now writes through `WriteTo.Async(…)` (see `appsettings.json`) so log entries are serialized in emission order even under load.
- **Step 2**: ✅ Added `MigrationLogContext.Push(...)` helper (see `src/Shared/WorkerInfrastructure/Logging/MigrationLogContext.cs`) and wrapped `BalMigrationService` processing so every log it emits carries migration metadata via Serilog.
- **Step 3**: ✅ `BalMigrationService` and `QueueWorker` now log directly through `ILogger`, relying on `MigrationLogContext` (and optional `PushProperties`) so no production code touches `ILogQueue` anymore.
- **Step 4**: ✅ Removed `ILogQueue`, `ChannelLogQueue`, `LogDrainService`, `LoggingServiceCollectionExtensions`, and `MigrationLogEvent` + builder. Program no longer registers the custom logging pipeline; docs/tests updated accordingly.
- **Step 5**: Pending review before implementation.

## Step 1 – Enable ordered Serilog pipeline (no code removal yet)
1. Wire Serilog in `Program.cs` (or host builder) with `UseSerilog()`.
2. Configure `WriteTo.Async(a => a.File(...))` (or whichever sinks we need). `WriteTo.Async` gives us the single serialized writer that preserves order.
3. Add `Enrich.FromLogContext()` so migration metadata we push later flows automatically.
4. Verify locally that the app still boots and Serilog consumes existing `ILogger` calls.

✅ **Review checkpoint:** confirm logs appear in order even under load; no structural changes yet (complete).

## Step 2 – Introduce migration context enrichment helper
1. Add a helper/enricher (e.g., `MigrationLogContext.Push(metadata)`) that wraps `LogContext.PushProperty` for `MigrationId`, `Scope`, etc. (now part of the shared infrastructure library).
2. Update one pilot caller (e.g., `BalMigrationService`) to use the helper while keeping the rest of the logging pipeline unchanged so behavior stays identical during validation.
3. Confirm logs in Serilog now contain the enriched fields.

✅ **Review checkpoint:** ensure the helper API is acceptable and telemetry matches expectations (complete via `MigrationLogContext` pilot in BAL).

## Step 3 – Inline logging in BAL/worker components
1. Replace `_logQueue.EnqueueAsync` + `MigrationLogEventBuilder` usage in `BalMigrationService`, `QueueWorker`, and other producers with direct `ILogger` calls wrapped in the helper from Step 2.
2. Run tests to confirm business logic still triggers the same number of log writes.
3. Temporarily keep the legacy queue infrastructure registered but ensure no production code references it anymore.

✅ **Review checkpoint:** verify there are zero production references to `ILogQueue` (complete; only legacy infrastructure remains to be deleted in Step 4).

## Step 4 – Retire custom queue infrastructure
1. Delete `ILogQueue`, `ChannelLogQueue`, `LogDrainService`, `LoggingServiceCollectionExtensions`, `MigrationLogEvent`, and builder.
2. Remove service registrations and documentation that referenced the queue.
3. Update unit tests to assert against the new helper/logger instead of queue mocks.

✅ **Review checkpoint:** run `dotnet test`; ensure DI still builds and docs/tests reflect the new reality (complete).

## Step 5 – Cleanup and validation
1. Re-run full test suite and any integration scenario that stresses log throughput.
2. Sanity-check log files to confirm ordering and metadata.
3. Update `docs/logging.md`, requirements, and diagrams with the simplified flow.

Following these sequential steps keeps logs ordered via Serilog’s async sink, removes unnecessary custom types, and gives us clear checkpoints to review after each change. 
