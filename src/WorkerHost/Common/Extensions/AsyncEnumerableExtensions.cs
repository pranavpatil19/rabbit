using System.Runtime.CompilerServices;

namespace WorkerHost.Common.Extensions;

public static class AsyncEnumerableExtensions
{
    public static IAsyncEnumerable<IReadOnlyList<T>> ChunkAsync<T>(
        this IAsyncEnumerable<T> source,
        int chunkSize,
        CancellationToken cancellationToken = default)
    {
        if (chunkSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be positive.");
        }

        return ChunkAsyncCore(source, chunkSize, cancellationToken);
    }

    private static async IAsyncEnumerable<IReadOnlyList<T>> ChunkAsyncCore<T>(
        IAsyncEnumerable<T> source,
        int chunkSize,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var buffer = new List<T>(chunkSize);
        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            buffer.Add(item);
            if (buffer.Count >= chunkSize)
            {
                yield return buffer.ToArray();
                buffer.Clear();
            }
        }

        if (buffer.Count > 0)
        {
            yield return buffer.ToArray();
        }
    }
}
