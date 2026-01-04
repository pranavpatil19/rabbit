using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;

using SharpFuzz;

using WorkerHost.Messaging;

using Xunit;

namespace WorkerHost.Tests.Fuzzing;

public class MigrationCommandFuzzTests
{
    private static readonly JsonSerializerOptions SerializerOptions = JsonOptionsFactory.CreateWorkerDeserializerOptions();

    private static readonly IReadOnlyList<byte[]> SeedCorpus =
    [
        Array.Empty<byte>(),
        Encoding.UTF8.GetBytes("{}"),
        Encoding.UTF8.GetBytes("{\"migrationId\":\"" + Guid.Empty + "\"}"),
        Encoding.UTF8.GetBytes(
            """
            {
              "migrationId": "00000000-0000-0000-0000-000000000001",
              "migrationScope": "Computer",
              "requestedBy": "qa-user",
              "requestedAtUtc": "2024-01-01T00:00:00Z",
              "priority": "High",
              "retryCount": 2,
              "transfer": {
                "computer": {
                  "sourceComputerId": "alpha",
                  "destinationComputerId": "beta",
                  "includeAgentIds": [ "a1", "a2" ]
                }
              }
            }
            """),
    ];

    [Fact]
    public void MigrationCommandDeserializer_FuzzHarness()
    {
        static void Exercise(ReadOnlySpan<byte> payload)
        {
            try
            {
                _ = JsonSerializer.Deserialize<MigrationCommand>(payload, SerializerOptions);
            }
            catch (JsonException)
            {
                // Expected for malformed JSON inputs.
            }
            catch (NotSupportedException)
            {
                // JSON payload used unsupported features (e.g., polymorphism).
            }
        }

        if (IsSharpFuzzEnabled())
        {
            Fuzzer.Run(stream =>
            {
                using var buffer = new MemoryStream(capacity: 1024);
                stream.CopyTo(buffer);
                Exercise(buffer.ToArray());
            });
            return;
        }

        foreach (var seed in SeedCorpus)
        {
            Exercise(seed);
        }
    }

    private static bool IsSharpFuzzEnabled() =>
        string.Equals(
            Environment.GetEnvironmentVariable("ENABLE_SHARPFUZZ"),
            "1",
            StringComparison.OrdinalIgnoreCase);
}
