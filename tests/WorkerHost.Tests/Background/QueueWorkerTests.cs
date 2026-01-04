using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using WorkerHost.Background;
using WorkerHost.Bal;
using WorkerHost.Messaging;
using WorkerHost.RabbitMq.Channels;
using WorkerHost.RabbitMq.Configuration;

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

        var channelAccess = new Mock<IBrokerChannelAccess>();
        var ackTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var command = CreateAgentCommand();
        var delivery = CreateDelivery(command);

        channelAccess.Setup(a => a.AcknowledgeAsync(It.Is<IInboundDelivery>(d => ReferenceEquals(d, delivery)), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                ackTcs.TrySetResult();
                return Task.CompletedTask;
            })
            .Verifiable();
        channelAccess.Setup(a => a.RejectAsync(It.IsAny<IInboundDelivery>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var buffer = new DeliveryBuffer(8);
        using var worker = CreateWorker(bal.Object, channelAccess.Object, buffer);

        await worker.StartAsync(CancellationToken.None);
        await buffer.EnqueueAsync(delivery, CancellationToken.None);
        buffer.Complete();
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

        var channelAccess = new Mock<IBrokerChannelAccess>();
        var nackTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var command = CreateAgentCommand();
        var delivery = CreateDelivery(command);

        channelAccess.Setup(a => a.RejectAsync(It.Is<IInboundDelivery>(d => ReferenceEquals(d, delivery)), true, It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                nackTcs.TrySetResult();
                return Task.CompletedTask;
            })
            .Verifiable();
        channelAccess.Setup(a => a.AcknowledgeAsync(It.IsAny<IInboundDelivery>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var buffer = new DeliveryBuffer(8);
        using var worker = CreateWorker(bal.Object, channelAccess.Object, buffer);

        await worker.StartAsync(CancellationToken.None);
        await buffer.EnqueueAsync(delivery, CancellationToken.None);
        buffer.Complete();
        await nackTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
        await worker.StopAsync(CancellationToken.None);

        channelAccess.Verify();
        channelAccess.Verify(a => a.AcknowledgeAsync(It.IsAny<IInboundDelivery>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private static QueueWorker CreateWorker(
        IBalMigrationService balService,
        IBrokerChannelAccess channelAccess,
        DeliveryBuffer buffer)
    {
        var services = new ServiceCollection();
        services.AddScoped(_ => balService);
        var provider = services.BuildServiceProvider();
        var scopeFactory = provider.GetRequiredService<IServiceScopeFactory>();

        var config = new BrokerOptions();
        var logger = Mock.Of<ILogger<QueueWorker>>();
        var processorLogger = Mock.Of<ILogger<DeliveryProcessor>>();
        var deliveryProcessor = new DeliveryProcessor(scopeFactory, channelAccess, Options.Create(config), processorLogger);

        return new QueueWorker(
            Options.Create(config),
            logger,
            buffer,
            deliveryProcessor);
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
    private sealed class TestInboundDelivery : IInboundDelivery
    {
        public TestInboundDelivery(ReadOnlyMemory<byte> body) => Body = body;

        public ReadOnlyMemory<byte> Body { get; }
    }
}
