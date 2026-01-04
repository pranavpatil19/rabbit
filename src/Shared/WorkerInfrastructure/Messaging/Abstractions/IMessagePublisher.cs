namespace WorkerHost.Messaging;

/// <summary>
/// Publishes migration messages to the broker.
/// </summary>
public interface IMessagePublisher
{
    Task PublishAsync(MigrationCommand command, CancellationToken cancellationToken = default);
}
