using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using WorkerHost.Messaging;

namespace WorkerHost.Background;

/// <summary>
/// Thin wrapper over <see cref="Channel{T}"/> so QueueWorker code reads in domain terms.
/// </summary>
public sealed class DeliveryQueue
{
    private readonly Channel<IInboundDelivery> _channel;

    public DeliveryQueue(int capacity)
    {
        _channel = Channel.CreateBounded<IInboundDelivery>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false,
            AllowSynchronousContinuations = false,
        });
    }

    public ValueTask EnqueueAsync(IInboundDelivery delivery, CancellationToken cancellationToken) =>
        _channel.Writer.WriteAsync(delivery, cancellationToken);

    public IAsyncEnumerable<IInboundDelivery> ReadAllAsync(CancellationToken cancellationToken) =>
        _channel.Reader.ReadAllAsync(cancellationToken);

    public void Complete(Exception? error = null) => _channel.Writer.TryComplete(error);
}
