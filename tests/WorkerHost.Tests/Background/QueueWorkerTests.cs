using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using WorkerHost.Background;
using WorkerHost.Bal;
using WorkerHost.Logging;
using WorkerHost.Messaging;
using WorkerHost.RabbitMq.Configuration;
using WorkerHost.RabbitMq.Messaging;

namespace WorkerHost.Tests.Background;

public sealed class QueueWorkerTests
{
    [Fact]
    public async Task ProcessesAgentMessage_AcksAndInvokesBal()
    {
        var bal = new Mock<IBalMigrationService>(MockBehavior.Strict);
        bal.Setup(s => s.TransferAgentAsync(It.IsAny<MigrationCommand>(), It.IsAny<AgentMigrationPayload>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable();

        var logQueue = new Mock<ILogQueue>();
        logQueue.Setup(l => l.EnqueueAsync(It.IsAny<MigrationLogEvent>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        var listener = new TestMessageListener();
        var channelAccess = new Mock<IBrokerChannelAccess>();
        var ackTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var command = CreateAgentCommand();
        var delivery = CreateDelivery(command);
        listener.Enqueue(delivery);

        channelAccess.Setup(a => a.AcknowledgeAsync(It.Is<IInboundDelivery>(d => ReferenceEquals(d, delivery)), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                ackTcs.TrySetResult();
                return Task.CompletedTask;
            })
            .Verifiable();
        channelAccess.Setup(a => a.RejectAsync(It.IsAny<IInboundDelivery>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        using var worker = CreateWorker(bal.Object, logQueue.Object, listener, channelAccess.Object);

        await worker.StartAsync(CancellationToken.None);
        await ackTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        await worker.StopAsync(CancellationToken.None);

        bal.Verify();
        channelAccess.Verify();
        channelAccess.Verify(a => a.RejectAsync(It.IsAny<IInboundDelivery>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task NacksOnBalFailure()
    {
        var bal = new Mock<IBalMigrationService>(MockBehavior.Strict);
        bal.Setup(s => s.TransferAgentAsync(It.IsAny<MigrationCommand>(), It.IsAny<AgentMigrationPayload>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("boom"));

        var logQueue = new Mock<ILogQueue>();
        logQueue.Setup(l => l.EnqueueAsync(It.IsAny<MigrationLogEvent>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        var listener = new TestMessageListener();
        var channelAccess = new Mock<IBrokerChannelAccess>();
        var nackTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var command = CreateAgentCommand();
        var delivery = CreateDelivery(command);
        listener.Enqueue(delivery);

        channelAccess.Setup(a => a.RejectAsync(It.Is<IInboundDelivery>(d => ReferenceEquals(d, delivery)), true, It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                nackTcs.TrySetResult();
                return Task.CompletedTask;
            })
            .Verifiable();
        channelAccess.Setup(a => a.AcknowledgeAsync(It.IsAny<IInboundDelivery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        using var worker = CreateWorker(bal.Object, logQueue.Object, listener, channelAccess.Object);

        await worker.StartAsync(CancellationToken.None);
        await nackTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        await worker.StopAsync(CancellationToken.None);

        channelAccess.Verify();
        channelAccess.Verify(a => a.AcknowledgeAsync(It.IsAny<IInboundDelivery>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private static QueueWorker CreateWorker(
        IBalMigrationService balService,
        ILogQueue logQueue,
        IMessageListener listener,
        IBrokerChannelAccess channelAccess)
    {
        var services = new ServiceCollection();
        services.AddScoped(_ => balService);
        var provider = services.BuildServiceProvider();
        var scopeFactory = provider.GetRequiredService<IServiceScopeFactory>();

        var config = new BrokerOptions();
        var logger = Mock.Of<ILogger<QueueWorker>>();

        return new QueueWorker(
            scopeFactory,
            Options.Create(config),
            logger,
            logQueue,
            listener,
            channelAccess);
    }

    private static IInboundDelivery CreateDelivery(MigrationCommand command)
    {
        var json = JsonSerializer.Serialize(command, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        });
        var body = Encoding.UTF8.GetBytes(json);
        return new TestInboundDelivery(body);
    }

    private static MigrationCommand CreateAgentCommand()
        => new()
        {
            MigrationId = Guid.NewGuid(),
            MigrationScope = MigrationScope.Agent,
            RequestedBy = "tester",
            RequestedAtUtc = DateTimeOffset.UtcNow,
            Transfer = new TransferPayload
            {
                Source = new TransferEndpoint
                {
                    ComputerId = "comp-1",
                    AgentId = "agent-a",
                },
                Destination = new TransferEndpoint
                {
                    ComputerId = "comp-2",
                    AgentId = "agent-b",
                },
            },
        };

    private sealed class TestMessageListener : IMessageListener
    {
        private readonly ConcurrentQueue<IInboundDelivery> _deliveries = new();

        public void Enqueue(IInboundDelivery delivery) => _deliveries.Enqueue(delivery);

        public async IAsyncEnumerable<IInboundDelivery> ReadAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (_deliveries.TryDequeue(out var delivery))
            {
                yield return delivery;
                await Task.Yield();
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class TestInboundDelivery : IInboundDelivery
    {
        public TestInboundDelivery(ReadOnlyMemory<byte> body) => Body = body;

        public ReadOnlyMemory<byte> Body { get; }
    }
}
