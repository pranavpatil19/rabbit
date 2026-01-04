# WorkerHost – RabbitMQ Migration Service

Background service + HTTP API built with ASP.NET Core 8 to migrate agent/task data between computers via RabbitMQ. Implements the requirements in `requirements_spec.txt` and architecture in `migration_approach.txt`.

## Features
- Background worker consumes migration commands from RabbitMQ and runs BAL/DAL orchestration with bounded concurrency.
- Minimal API exposes `POST /migrations` and `GET /migrations/{id}` plus health/metrics endpoints.
- Serilog-based logging with channel-backed queue for ordered per-migration logs; metrics summarize processed agents/tasks.
- In-memory test data context (see `testdata/sample_data.json`) simulates computers/agents/tasks for local testing.

## Getting Started
1. Install .NET 8 SDK and ensure RabbitMQ is reachable (defaults to `amqp://guest:guest@localhost/`).
2. Restore/build: `dotnet build src/WorkerHost/WorkerHost.csproj`.
3. Run the host: `dotnet run --project src/WorkerHost/WorkerHost.csproj`.
4. Submit migrations via `POST /migrations` (see sample payloads below); monitor status via `GET /migrations/{id}`, `GET /health`, `GET /metrics`.

## Test Data Seeding (Local Runs)
The worker ships with an in-memory DAL backed by `testdata/sample_data.json`. Local smoke/flood tests assume those IDs exist.

1. `dotnet run` (and `dotnet test`) now call a `CopyTestDataBeforeRun` MSBuild target that mirrors `testdata/` into the build output (`src/WorkerHost/bin/<TFM>/testdata/`), so no manual copy is required.
2. On startup the worker logs `Loaded <X> computers and <Y> agents from test data ...` or warns that the file is missing. If you see the warning, run `dotnet build src/WorkerHost/WorkerHost.csproj` once to regenerate the copied assets.
3. When using `tools/MigrationFloodPublisher`, ensure the worker started after the copy so fixture IDs resolve; otherwise every message will NACK and benchmarks remain inconclusive.
4. Need to refresh data mid-run? POST the payload to `POST /testdata/load` (the sample JSON matches `testdata/sample_data.json`) or add `--seed-endpoint http://localhost:8080/testdata/load` when running the flood tool to automate the seeding step.

## Dead-letter & TTL Defaults
`BrokerOptions.WorkEndpoint.DeadLetter` now declares a `work-requests.dlx` exchange/queue pair automatically and wires the primary `work-requests` queue with:
- `x-dead-letter-exchange = work-requests.dlx`
- `x-dead-letter-routing-key = work-requests.dlx`
- `x-message-ttl = 600000 ms`

Tweak these values (or disable the feature) via `appsettings*.json`. Any explicit `Queue.Arguments` override takes precedence if you need per-environment routing.

## Docker / Compose
To run the service with RabbitMQ locally:
```bash
docker-compose up --build
```
This launches RabbitMQ (ports 5672/15672) and WorkerHost (port 8080, pointing to the RabbitMQ container). Override config via environment variables (e.g., `BrokerOptions__Connection__Host`, `BrokerOptions__Connection__UserName`, or `BrokerOptions__DefaultConcurrency`).

## Smoke Test Checklist
1. Bring up RabbitMQ + WorkerHost (either locally or via `docker-compose up --build`).
2. POST a migration request (e.g., `docs/examples/migration-request.agent.json`) to `http://localhost:8080/migrations`.
3. Receive `202 Accepted` and note the `MigrationId`.
4. Poll `GET /migrations/{id}` until status is `Completed`.
5. Verify `/metrics` reflects processed agents/tasks and inspect Serilog output for ordered migration logs.

## Sample Requests
```
POST /migrations
{
  "requestedBy": "ops-user",
  "scope": "Agent",
  "agent": {
    "sourceComputerId": "computer-1",
    "destinationComputerId": "computer-2",
    "sourceAgentId": "agent-a",
    "destinationAgentId": "agent-b"
  }
}
```

## References
- Requirements: `requirements_spec.txt`
- Migration approach/message contract: `migration_approach.txt`
- Implementation steps/roadmap: `implementation_steps.txt`
- RabbitMQ processing overview: `docs/rabbitmq-processing.md`
- RabbitMQ configuration reference: `docs/rabbitmq-config.md`
- Testing strategy: `docs/testing-strategy.md`
- Logging strategy: `docs/logging.md`
- Sample data: `testdata/sample_data.json`
- Sample requests & queue payloads: `docs/examples/*.json`, `docs/rabbitmq-messages/*.json`
- Containers: `Dockerfile`, `docker-compose.yml`
