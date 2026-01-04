using System.Linq;
using FluentValidation;
using Serilog;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using WorkerHost.Api;
using WorkerHost.Api.Migrations;
using WorkerHost.Background;
using WorkerHost.Bal;
using WorkerHost.Common.Models;
using WorkerHost.Configuration;
using WorkerHost.Dal;
using WorkerHost.Logging;
using WorkerHost.Messaging;
using WorkerHost.RabbitMq.Extensions;

// Bootstrap the generic host + minimal API pipeline.
var builder = WebApplication.CreateBuilder(args);

// Centralized Serilog configuration (read from appsettings + DI context).
builder.Host.UseSerilog((context, services, loggerConfiguration) =>
{
    loggerConfiguration
        .ReadFrom.Configuration(context.Configuration)
        .ReadFrom.Services(services)
        .Enrich.FromLogContext();
});

// Strongly typed options (RabbitMQ + test data) with upfront validation.
builder.Services.AddBrokerMessaging(builder.Configuration);
builder.Services
    .AddOptions<TestDataOptions>()
    .Bind(builder.Configuration.GetSection(nameof(TestDataOptions)))
    .ValidateOnStart();

// API payload validation.
builder.Services.AddValidatorsFromAssemblyContaining<MigrationRequestValidator>();

// Messaging + BAL orchestration.
builder.Services.AddScoped<IBalMigrationService, BalMigrationService>();

// In-memory DAL + supporting infrastructure used by both API and worker.
builder.Services.AddSingleton<TestDataContext>();
builder.Services.AddSingleton<IAgentDataStore, InMemoryAgentDataStore>();
builder.Services.AddSingleton<ITaskDataStore, InMemoryTaskDataStore>();
builder.Services.AddSingleton<IMigrationJobStore, InMemoryMigrationJobStore>();

// Logging pipeline + background processing hosted services.
builder.Services.AddLoggingPipeline();
builder.Services.AddQueueProcessingWorker();
builder.Services
    .AddHealthChecks();

var app = builder.Build();

// API surface for submitting migrations + querying their status.
var migrationsGroup = app.MapGroup("/migrations");

migrationsGroup.MapPost(
    "/",
    async Task<IResult> (
        MigrationRequestDto request,
        IValidator<MigrationRequestDto> validator,
        IMessagePublisher publisher,
        IMigrationJobStore jobStore,
        CancellationToken cancellationToken) =>
    {
        var validation = await validator.ValidateAsync(request, cancellationToken).ConfigureAwait(false);
        if (!validation.IsValid)
        {
            return Results.ValidationProblem(validation.ToDictionary());
        }

        var command = MigrationRequestMapper.ToCommand(request);
        await jobStore.InitializeAsync(command.MigrationId, command.MigrationScope, cancellationToken).ConfigureAwait(false);
        await publisher.PublishAsync(command, cancellationToken).ConfigureAwait(false);
        var response = new MigrationAcceptedResponse(command.MigrationId, "queued");
        return Results.Accepted($"/migrations/{command.MigrationId}", response);
    });

migrationsGroup.MapGet(
    "/{migrationId:guid}",
    async Task<IResult> (
        Guid migrationId,
        IMigrationJobStore jobStore,
        CancellationToken cancellationToken) =>
    {
        var snapshot = await jobStore.GetAsync(migrationId, cancellationToken).ConfigureAwait(false);
        if (snapshot is null)
        {
            return Results.NotFound();
        }

        var response = new MigrationStatusResponse(
            snapshot.MigrationId,
            snapshot.Scope.ToString(),
            snapshot.Status.ToString(),
            snapshot.Message,
            snapshot.AgentsProcessed,
            snapshot.TasksProcessed,
            snapshot.LastUpdatedUtc);

        return Results.Ok(response);
    });

// Lightweight diagnostic endpoints.
app.MapGet(
    "/health",
    async Task<IResult> (
        HealthCheckService healthCheckService,
        CancellationToken cancellationToken) =>
    {
        var report = await healthCheckService.CheckHealthAsync(cancellationToken).ConfigureAwait(false);
        var payload = new
        {
            status = report.Status.ToString(),
            time = DateTimeOffset.UtcNow,
            results = report.Entries.ToDictionary(
                entry => entry.Key,
                entry => new
                {
                    status = entry.Value.Status.ToString(),
                    description = entry.Value.Description,
                    data = entry.Value.Data,
                }),
        };

        var statusCode = report.Status switch
        {
            HealthStatus.Healthy => StatusCodes.Status200OK,
            HealthStatus.Degraded => StatusCodes.Status200OK,
            _ => StatusCodes.Status503ServiceUnavailable,
        };

        return Results.Json(payload, statusCode: statusCode);
    });
app.MapGet(
    "/metrics",
    async Task<IResult> (
        IMigrationJobStore jobStore,
        CancellationToken cancellationToken) =>
    {
        var snapshots = await jobStore.ListAsync(cancellationToken).ConfigureAwait(false);
        var processedAgents = snapshots.Sum(s => s.AgentsProcessed);
        var processedTasks = snapshots.Sum(s => s.TasksProcessed);
        var activeJobs = snapshots.Count(s => s.Status is MigrationJobStatus.InProgress or MigrationJobStatus.Queued);
        return Results.Ok(new { processedAgents, processedTasks, activeJobs });
    });

app.MapTestDataEndpoints();

await app.RunAsync();
