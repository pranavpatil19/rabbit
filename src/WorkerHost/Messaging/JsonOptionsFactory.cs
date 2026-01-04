using System.Text.Json;

namespace WorkerHost.Messaging;

/// <summary>
/// Central place to create JSON serializer options used across messaging components.
/// </summary>
public static class JsonOptionsFactory
{
    /// <summary>
    /// Options for deserializing inbound worker messages (case-insensitive property names).
    /// </summary>
    public static JsonSerializerOptions CreateWorkerDeserializerOptions() =>
        new()
        {
            PropertyNameCaseInsensitive = true,
        };
}
