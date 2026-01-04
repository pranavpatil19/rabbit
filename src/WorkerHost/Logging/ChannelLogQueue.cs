using System.Threading.Channels;

namespace WorkerHost.Logging;

public sealed class ChannelLogQueue : ILogQueue
{
    private readonly Channel<MigrationLogEvent> _channel;

    public ChannelLogQueue(int capacity = 1024)
    {
        _channel = Channel.CreateBounded<MigrationLogEvent>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false,
        });
    }

    public ValueTask EnqueueAsync(MigrationLogEvent logEvent, CancellationToken cancellationToken = default)
        => _channel.Writer.WriteAsync(logEvent, cancellationToken);

    public async IAsyncEnumerable<MigrationLogEvent> ReadAllAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (_channel.Reader.TryRead(out var item))
            {
                yield return item;
            }
        }
    }

    public async ValueTask<MigrationLogEvent?> ReadNextAsync(CancellationToken cancellationToken)
    {
        while (await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (_channel.Reader.TryRead(out var item))
            {
                return item;
            }
        }

        return null;
    }
}
