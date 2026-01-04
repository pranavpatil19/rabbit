# Testing Strategy

This project follows the expectations in `requirements_spec.txt §14` by layering unit and integration tests across the API, BAL, DAL, and background worker. This document explains what currently exists so new contributors can extend coverage consistently.

## Unit Tests
- **Messaging contract** (`tests/WorkerHost.Tests/Messaging/MigrationCommandSamplesTests.cs`) – Deserializes the canonical JSON samples in `docs/rabbitmq-messages/*.json` to ensure the `MigrationCommand` model stays aligned with the published contract.
- **Configuration validation** (`tests/WorkerHost.Tests/Configuration/BrokerOptionsTests.cs`) – Verifies `BrokerOptions` binding rules, required fields, and default values.
- **BAL orchestration** (`tests/WorkerHost.Tests/Bal/BalMigrationServiceTests.cs`) – Exercises `TransferAgentAsync`, `TransferComputerAsync`, and `TransferTaskBatchAsync`, ensuring task streams, agent locking, and job progress updates behave as required.
- **RabbitMQ telemetry** (`tests/WorkerHost.Tests/RabbitMq/MessageListenerMetricsTests.cs`, `MessageListenerHealthCheckTests.cs`) – Validates listener metrics snapshots and the health check’s idle/connection logic.
- **Queue worker dispatcher** (`tests/WorkerHost.Tests/Background/QueueWorkerTests.cs`) – Spins up the actual `QueueWorker` with a stub `IMessageListener`, verifying successful messages ACK correctly and failures trigger NACK/requeue semantics.

## Integration / Smoke Tests
- `dotnet test` runs on every build to execute all of the above suites.
- `docker-compose up --build` launches WorkerHost + RabbitMQ for manual validation using the sample requests in `docs/examples/`. The smoke checklist in `README.md` walks through POST → GET → `/metrics`.

## Future Enhancements
1. **Listener integration harness:** Wire a throwaway RabbitMQ container in CI to push actual AMQP deliveries through `QueueWorker` end-to-end.
2. **API contract tests:** Use `WebApplicationFactory` to validate `POST /migrations` inputs/validation rules.
3. **DAL persistence tests:** When a persistent data store replaces `TestDataContext`, add fixtures that run migrations against a real database (or Dockerized equivalent).

Please keep this document updated whenever new suites are added so the testing story stays transparent.
