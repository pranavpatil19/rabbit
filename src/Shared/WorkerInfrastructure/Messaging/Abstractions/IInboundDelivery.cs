namespace WorkerHost.Messaging;

/// <summary>
/// Represents a broker delivery exposed to the worker, independent of transport details.
/// </summary>
public interface IInboundDelivery
{
    ReadOnlyMemory<byte> Body { get; }
}
