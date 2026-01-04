using System.Diagnostics;
using System.Net.Http;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using WorkerHost.Messaging;
using WorkerHost.RabbitMq.Configuration;
using WorkerHost.RabbitMq.Messaging;

var options = FloodRunOptions.Parse(args);
var repoRoot = Directory.GetCurrentDirectory();
var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cts.Cancel();
};

var services = new ServiceCollection();
ConfigureServices(services, options, repoRoot);
await using var provider = services.BuildServiceProvider();

var logger = provider.GetRequiredService<ILoggerFactory>().CreateLogger("FloodPublisher");
var publisher = provider.GetRequiredService<MessagePublisher>();

if (options.SeedEndpoint is not null)
{
    await SeedTestDataAsync(options, repoRoot, logger, cts.Token).ConfigureAwait(false);
}

logger.LogInformation("Starting publisher flood: {Count} messages; scopes: {Scopes}; tasks/batch: {Tasks}",
    options.Count,
    string.Join(",", options.Mix),
    options.TasksPerBatch);

var sw = Stopwatch.StartNew();
var rnd = new Random();
var mix = options.Mix;
for (var i = 0; i < options.Count && !cts.IsCancellationRequested; i++)
{
    var scope = mix[rnd.Next(mix.Count)];
    var command = MigrationCommandFactory.Create(scope, options.TasksPerBatch, i);
    await publisher.PublishAsync(command, cts.Token).ConfigureAwait(false);

    if ((i + 1) % options.ProgressInterval == 0)
    {
        logger.LogInformation("Published {PublishedCount}/{Total} messages", i + 1, options.Count);
    }
}

sw.Stop();
logger.LogInformation("Publisher flood completed in {ElapsedMs:F0} ms", sw.Elapsed.TotalMilliseconds);

static void ConfigureServices(IServiceCollection services, FloodRunOptions options, string repoRoot)
{
    var configPath = options.ResolveConfigPath(repoRoot);
    var configuration = new ConfigurationBuilder()
        .SetBasePath(repoRoot)
        .AddJsonFile(configPath, optional: false)
        .AddEnvironmentVariables()
        .Build();

    services.AddLogging(builder =>
    {
        builder.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss ";
        });
        builder.SetMinimumLevel(LogLevel.Information);
    });

    services.AddOptions();
    services.Configure<BrokerOptions>(configuration.GetSection("BrokerOptions"));
    services.AddSingleton<WorkerHost.RabbitMq.Monitoring.PublisherMetrics>();
    services.AddSingleton<MessagePublisher>();
    services.AddSingleton<IMessagePublisher>(sp => sp.GetRequiredService<MessagePublisher>());
}

static async Task SeedTestDataAsync(FloodRunOptions options, string repoRoot, ILogger logger, CancellationToken cancellationToken)
{
    if (options.SeedEndpoint is null)
    {
        return;
    }

    var filePath = options.ResolveSeedFilePath(repoRoot);
    logger.LogInformation("Seeding test data from {File} to {Endpoint}", filePath, options.SeedEndpoint);
    using var httpClient = new HttpClient();
    using var content = new StringContent(await File.ReadAllTextAsync(filePath, cancellationToken).ConfigureAwait(false), Encoding.UTF8, "application/json");
    using var response = await httpClient.PostAsync(options.SeedEndpoint, content, cancellationToken).ConfigureAwait(false);
    var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
    if (!response.IsSuccessStatusCode)
    {
        throw new InvalidOperationException($"Failed to seed test data. Status {(int)response.StatusCode}: {body}");
    }

    logger.LogInformation("Seed endpoint response: {Response}", body);
}

internal sealed record FloodRunOptions(
    int Count,
    IReadOnlyList<MigrationScope> Mix,
    int TasksPerBatch,
    int ProgressInterval,
    string? ConfigOverride,
    Uri? SeedEndpoint,
    string? SeedFileOverride)
{
    private static readonly IReadOnlyList<MigrationScope> DefaultMix =
        Enum.GetValues<MigrationScope>();

    public static FloodRunOptions Parse(string[] args)
    {
        var count = 1000;
        var tasksPerBatch = 100;
        var progressInterval = 100;
        string? mixArg = null;
        string? configPath = null;
        string? seedEndpointRaw = null;
        string? seedFile = null;

        for (var i = 0; i < args.Length; i++)
        {
            static bool Matches(string input, string shortName, string longName)
                => string.Equals(input, shortName, StringComparison.OrdinalIgnoreCase)
                   || string.Equals(input, longName, StringComparison.OrdinalIgnoreCase);

            if (Matches(args[i], "-c", "--count") && i + 1 < args.Length && int.TryParse(args[i + 1], out var parsedCount))
            {
                count = parsedCount;
                i++;
            }
            else if (Matches(args[i], "-m", "--mix") && i + 1 < args.Length)
            {
                mixArg = args[i + 1];
                i++;
            }
            else if (Matches(args[i], "-t", "--tasks-per-batch") && i + 1 < args.Length && int.TryParse(args[i + 1], out var parsedTasks))
            {
                tasksPerBatch = parsedTasks;
                i++;
            }
            else if (Matches(args[i], "-p", "--progress") && i + 1 < args.Length && int.TryParse(args[i + 1], out var parsedProgress))
            {
                progressInterval = parsedProgress;
                i++;
            }
            else if (Matches(args[i], "-f", "--config") && i + 1 < args.Length)
            {
                configPath = args[i + 1];
                i++;
            }
            else if (Matches(args[i], "-s", "--seed-endpoint") && i + 1 < args.Length)
            {
                seedEndpointRaw = args[i + 1];
                i++;
            }
            else if (Matches(args[i], "-sf", "--seed-file") && i + 1 < args.Length)
            {
                seedFile = args[i + 1];
                i++;
            }
        }

        if (count <= 0)
        {
            throw new ArgumentException("Count must be positive.");
        }

        if (tasksPerBatch <= 0)
        {
            throw new ArgumentException("Tasks per batch must be positive.");
        }

        if (progressInterval <= 0)
        {
            progressInterval = 100;
        }

        var mix = ParseMix(mixArg);
        Uri? seedEndpoint = null;
        if (!string.IsNullOrWhiteSpace(seedEndpointRaw))
        {
            if (!Uri.TryCreate(seedEndpointRaw, UriKind.Absolute, out seedEndpoint)
                || (seedEndpoint.Scheme is not "http" and not "https"))
            {
                throw new ArgumentException("Seed endpoint must be an absolute HTTP/HTTPS URI.");
            }
        }

        return new FloodRunOptions(count, mix, tasksPerBatch, progressInterval, configPath, seedEndpoint, seedFile);
    }

    private static IReadOnlyList<MigrationScope> ParseMix(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return DefaultMix;
        }

        var scopes = new List<MigrationScope>();
        foreach (var token in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (Enum.TryParse<MigrationScope>(token, ignoreCase: true, out var scope))
            {
                scopes.Add(scope);
            }
        }

        return scopes.Count == 0 ? DefaultMix : scopes;
    }

    public string ResolveConfigPath(string repoRoot)
    {
        if (!string.IsNullOrWhiteSpace(ConfigOverride))
        {
            return EnsureExists(Path.IsPathRooted(ConfigOverride)
                ? ConfigOverride
                : Path.Combine(repoRoot, ConfigOverride));
        }

        var devConfig = Path.Combine(repoRoot, "src", "WorkerHost", "appsettings.Development.json");
        if (File.Exists(devConfig))
        {
            return devConfig;
        }

        var defaultConfig = Path.Combine(repoRoot, "src", "WorkerHost", "appsettings.json");
        return EnsureExists(defaultConfig);
    }

    public string ResolveSeedFilePath(string repoRoot)
    {
        if (!string.IsNullOrWhiteSpace(SeedFileOverride))
        {
            return EnsureExists(Path.IsPathRooted(SeedFileOverride)
                ? SeedFileOverride
                : Path.Combine(repoRoot, SeedFileOverride));
        }

        var defaultSeed = Path.Combine(repoRoot, "testdata", "sample_data.json");
        return EnsureExists(defaultSeed);
    }

    private static string EnsureExists(string path)
    {
        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"File not found: {path}");
        }

        return path;
    }
}

internal static class MigrationCommandFactory
{
    private const int KnownScenarioRatioNumerator = 3; // Target ≥30% known-good payloads.
    private const int KnownScenarioRatioDenominator = 10;

    private static readonly string[] ComputerIds = Enumerable.Range(1, 10).Select(i => $"comp-{i:D2}").ToArray();
    private static readonly string[] AgentIds = Enumerable.Range(1, 25).Select(i => $"agent-{i:D3}").ToArray();

    private static readonly ComputerScenario[] KnownComputerScenarios =
    [
        new("computer-1", "computer-2", new[] { "agent-a", "agent-b" }),
        new("computer-2", "computer-1", new[] { "agent-x" }),
    ];

    private static readonly AgentScenario[] KnownAgentScenarios =
    [
        new("computer-1", "agent-a", "computer-2", "agent-x"),
        new("computer-1", "agent-b", "computer-2", "agent-x"),
        new("computer-2", "agent-x", "computer-1", "agent-a"),
    ];

    private static readonly TaskBatchScenario[] KnownTaskBatchScenarios =
    [
        new("computer-1", "agent-a", new[] { "task-001", "task-002" }, "computer-2", "agent-x"),
        new("computer-1", "agent-b", new[] { "task-010" }, "computer-2", "agent-x"),
        new("computer-2", "agent-x", new[] { "task-900" }, "computer-1", "agent-a"),
    ];

    public static MigrationCommand Create(MigrationScope scope, int tasksPerBatch, int seed)
    {
        var now = DateTimeOffset.UtcNow;
        var preferKnownScenario = (seed % KnownScenarioRatioDenominator) < KnownScenarioRatioNumerator;
        var transfer = preferKnownScenario
                       && TryCreateKnownTransfer(scope, tasksPerBatch, seed, out var knownTransfer)
            ? knownTransfer
            : BuildRandomTransfer(scope, tasksPerBatch, seed);
        var command = new MigrationCommand
        {
            MigrationId = Guid.NewGuid(),
            MigrationScope = scope,
            RequestedBy = "flood-tester",
            RequestedAtUtc = now,
            Priority = MigrationPriority.High,
            RetryCount = 0,
            Metadata = new MigrationMetadata
            {
                ReasonCode = "LoadTest",
                Notes = $"Seed:{seed}"
            },
            ExpectedCounts = new ExpectedCounts
            {
                Agents = scope == MigrationScope.Computer ? 5 : null,
                Tasks = scope == MigrationScope.TaskBatch ? tasksPerBatch : null,
            },
            Transfer = transfer,
        };

        return command;
    }

    private static TransferPayload BuildRandomTransfer(MigrationScope scope, int tasksPerBatch, int seed)
    {
        var sourceComputer = ComputerIds[seed % ComputerIds.Length];
        var destinationComputer = ComputerIds[(seed + 3) % ComputerIds.Length];
        var sourceAgent = AgentIds[seed % AgentIds.Length];
        var destinationAgent = AgentIds[(seed + 7) % AgentIds.Length];

        return scope switch
        {
            MigrationScope.Computer => new TransferPayload
            {
                Source = new TransferEndpoint
                {
                    ComputerId = sourceComputer,
                    AgentIds = PickAgents(seed, 5).ToArray(),
                },
                Destination = new TransferEndpoint
                {
                    ComputerId = destinationComputer,
                },
            },
            MigrationScope.Agent => new TransferPayload
            {
                Source = new TransferEndpoint
                {
                    ComputerId = sourceComputer,
                    AgentId = sourceAgent,
                },
                Destination = new TransferEndpoint
                {
                    ComputerId = destinationComputer,
                    AgentId = destinationAgent,
                },
            },
            MigrationScope.TaskBatch => new TransferPayload
            {
                Source = new TransferEndpoint
                {
                    ComputerId = sourceComputer,
                    AgentId = sourceAgent,
                    TaskIds = GenerateTaskIds(seed, tasksPerBatch).ToArray(),
                },
                Destination = new TransferEndpoint
                {
                    ComputerId = destinationComputer,
                    AgentId = destinationAgent,
                },
            },
            _ => throw new ArgumentOutOfRangeException(nameof(scope), scope, null),
        };
    }

    private static bool TryCreateKnownTransfer(MigrationScope scope, int tasksPerBatch, int seed, out TransferPayload transfer)
    {
        switch (scope)
        {
            case MigrationScope.Computer:
                transfer = CreateComputerTransfer(KnownComputerScenarios[seed % KnownComputerScenarios.Length]);
                return true;
            case MigrationScope.Agent:
                transfer = CreateAgentTransfer(KnownAgentScenarios[seed % KnownAgentScenarios.Length]);
                return true;
            case MigrationScope.TaskBatch:
                transfer = CreateTaskBatchTransfer(KnownTaskBatchScenarios[seed % KnownTaskBatchScenarios.Length], tasksPerBatch);
                return true;
            default:
                transfer = null!;
                return false;
        }
    }

    private static TransferPayload CreateComputerTransfer(ComputerScenario scenario)
        => new()
        {
            Source = new TransferEndpoint
            {
                ComputerId = scenario.SourceComputer,
                AgentIds = scenario.AgentIds
            },
            Destination = new TransferEndpoint
            {
                ComputerId = scenario.DestinationComputer,
            },
        };

    private static TransferPayload CreateAgentTransfer(AgentScenario scenario)
        => new()
        {
            Source = new TransferEndpoint
            {
                ComputerId = scenario.SourceComputer,
                AgentId = scenario.SourceAgent,
            },
            Destination = new TransferEndpoint
            {
                ComputerId = scenario.DestinationComputer,
                AgentId = scenario.DestinationAgent,
            },
        };

    private static TransferPayload CreateTaskBatchTransfer(TaskBatchScenario scenario, int requestedTasksPerBatch)
    {
        var taskCount = Math.Min(requestedTasksPerBatch, scenario.TaskIds.Count);
        var taskSlice = scenario.TaskIds.Take(taskCount).ToArray();

        return new TransferPayload
        {
            Source = new TransferEndpoint
            {
                ComputerId = scenario.ComputerId,
                AgentId = scenario.AgentId,
                TaskIds = taskSlice,
            },
            Destination = new TransferEndpoint
            {
                ComputerId = scenario.TargetComputerId,
                AgentId = scenario.TargetAgentId,
            },
        };
    }

    private sealed record ComputerScenario(string SourceComputer, string DestinationComputer, IReadOnlyList<string> AgentIds);

    private sealed record AgentScenario(string SourceComputer, string SourceAgent, string DestinationComputer, string DestinationAgent);

    private sealed record TaskBatchScenario(string ComputerId, string AgentId, IReadOnlyList<string> TaskIds, string TargetComputerId, string TargetAgentId);

    private static IEnumerable<string> PickAgents(int seed, int count)
    {
        for (var i = 0; i < count; i++)
        {
            yield return AgentIds[(seed + i) % AgentIds.Length];
        }
    }

    private static IEnumerable<string> GenerateTaskIds(int seed, int count)
    {
        for (var i = 0; i < count; i++)
        {
            yield return $"task-{seed:D4}-{i:D3}";
        }
    }
}
