using System.Threading;
using System.Threading.Tasks;

using RabbitMQ.Client;

using WorkerHost.Messaging;

namespace WorkerHost.RabbitMq.Channels;

/// <summary>
/// Coordinates exclusive access to RabbitMQ channel operations such as ACK/NACK to avoid concurrent writes.
/// </summary>
public interface IBrokerChannelAccess
{
    Task AcknowledgeAsync(IInboundDelivery delivery, CancellationToken cancellationToken = default);

    Task RejectAsync(IInboundDelivery delivery, bool requeue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Allows callers to reset a problematic channel (for example after exceptions) in a serialized manner.
    /// </summary>
    Task ResetAsync(IChannel channel, CancellationToken cancellationToken = default);
}
