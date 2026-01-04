using System;
using System.Collections.Generic;

namespace WorkerHost.Dal;

public sealed class TestDataPayload
{
    public IReadOnlyCollection<TestDataComputer> Computers { get; init; } = Array.Empty<TestDataComputer>();
}
