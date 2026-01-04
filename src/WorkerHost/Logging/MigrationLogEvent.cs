using Microsoft.Extensions.Logging;
using WorkerHost.Messaging;

namespace WorkerHost.Logging;

/// <summary>
/// Canonical representation of a migration log entry flowing through <see cref="ILogQueue"/>.
/// </summary>
public sealed record MigrationLogEvent(
    Guid MigrationId,
    MigrationScope Scope,
    string Category,
    LogLevel Level,
    string MessageTemplate,
    object?[] Args,
    Exception? Exception,
    IReadOnlyDictionary<string, object?> Properties);

/// <summary>
/// Helper to build <see cref="MigrationLogEvent"/> instances with consistent metadata.
/// </summary>
public sealed class MigrationLogEventBuilder
{
    private readonly Guid _migrationId;
    private readonly MigrationScope _scope;
    private readonly string _category;
    private LogLevel _level = LogLevel.Information;
    private string _messageTemplate = string.Empty;
    private object?[] _args = Array.Empty<object?>();
    private Exception? _exception;
    private readonly Dictionary<string, object?> _properties = new(StringComparer.OrdinalIgnoreCase);

    private MigrationLogEventBuilder(Guid migrationId, MigrationScope scope, string category)
    {
        _migrationId = migrationId;
        _scope = scope;
        _category = category;
        _properties["MigrationId"] = migrationId;
        _properties["Scope"] = scope.ToString();
    }

    public static MigrationLogEventBuilder ForCommand(MigrationCommand command, string category)
        => new(command.MigrationId, command.MigrationScope, category);

    public MigrationLogEventBuilder WithLevel(LogLevel level)
    {
        _level = level;
        return this;
    }

    public MigrationLogEventBuilder WithMessage(string template, params object?[] args)
    {
        _messageTemplate = template;
        _args = args ?? Array.Empty<object?>();
        return this;
    }

    public MigrationLogEventBuilder WithException(Exception? exception)
    {
        _exception = exception;
        return this;
    }

    public MigrationLogEventBuilder WithProperties(IReadOnlyDictionary<string, object?>? properties)
    {
        if (properties is null)
        {
            return this;
        }

        foreach (var kvp in properties)
        {
            _properties[kvp.Key] = kvp.Value;
        }

        return this;
    }

    public MigrationLogEventBuilder AddProperty(string key, object? value)
    {
        _properties[key] = value;
        return this;
    }

    public MigrationLogEvent Build()
    {
        if (string.IsNullOrWhiteSpace(_messageTemplate))
        {
            throw new InvalidOperationException("Message template must be specified before building a log event.");
        }

        return new MigrationLogEvent(
            _migrationId,
            _scope,
            _category,
            _level,
            _messageTemplate,
            _args,
            _exception,
            new Dictionary<string, object?>(_properties));
    }
}
