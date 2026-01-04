using System;
using System.Collections.Generic;

using Serilog.Context;

using WorkerHost.Messaging;

namespace WorkerHost.Logging;

/// <summary>
/// Provides a simple way to push migration metadata into Serilog's ambient log context.
/// </summary>
public static class MigrationLogContext
{
    /// <summary>
    /// Adds migration identifiers (and optional extra properties) to the Serilog <see cref="LogContext"/>.
    /// </summary>
    /// <param name="command">Migration command whose identifiers should flow with subsequent log lines.</param>
    /// <param name="additionalProperties">Optional extra key/value pairs to push into the context.</param>
    /// <returns>An <see cref="IDisposable"/> that must be disposed to remove the pushed properties.</returns>
    public static IDisposable Push(MigrationCommand command, IReadOnlyDictionary<string, object?>? additionalProperties = null)
    {
        if (command is null)
        {
            throw new ArgumentNullException(nameof(command));
        }

        var scopes = new List<IDisposable>
        {
            LogContext.PushProperty("MigrationId", command.MigrationId),
            LogContext.PushProperty("MigrationScope", command.MigrationScope.ToString()),
        };

        if (additionalProperties is not null)
        {
            foreach (var kvp in additionalProperties)
            {
                scopes.Add(LogContext.PushProperty(kvp.Key, kvp.Value));
            }
        }

        return new ScopeCollection(scopes);
    }

    /// <summary>
    /// Pushes arbitrary properties into the Serilog context without duplicating migration identifiers.
    /// </summary>
    public static IDisposable PushProperties(IReadOnlyDictionary<string, object?>? properties)
    {
        if (properties is null || properties.Count == 0)
        {
            return NullScope.Instance;
        }

        var scopes = new List<IDisposable>(properties.Count);
        foreach (var kvp in properties)
        {
            scopes.Add(LogContext.PushProperty(kvp.Key, kvp.Value));
        }

        return new ScopeCollection(scopes);
    }

    private sealed class ScopeCollection : IDisposable
    {
        private readonly IReadOnlyList<IDisposable> _scopes;
        private bool _disposed;

        public ScopeCollection(IReadOnlyList<IDisposable> scopes)
        {
            _scopes = scopes;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            for (var i = _scopes.Count - 1; i >= 0; i--)
            {
                _scopes[i].Dispose();
            }

            _disposed = true;
        }
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose()
        {
            // No-op
        }
    }
}
