using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Moq;
using RabbitMQ.Client;
using WorkerHost.Common.Models;
using WorkerHost.Messaging;
using WorkerHost.RabbitMq.Messaging;

namespace WorkerHost.Tests.RabbitMq;

public sealed class BrokerChannelAccessTests
{
    [Fact]
    public async Task AcknowledgeAsync_InvokesBasicAck()
    {
        var channel = new Mock<IChannel>();
        channel.Setup(c => c.BasicAckAsync(5, false, It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask)
            .Verifiable();
        var delivery = CreateDelivery(channel.Object, deliveryTag: 5);
        var accessor = new BrokerChannelAccess(Mock.Of<ILogger<BrokerChannelAccess>>());

        await accessor.AcknowledgeAsync(delivery, CancellationToken.None);

        channel.Verify();
    }

    [Fact]
    public async Task RejectAsync_InvokesBasicNackWithRequeueFlag()
    {
        var channel = new Mock<IChannel>();
        channel.Setup(c => c.BasicNackAsync(8, false, true, It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask)
            .Verifiable();
        var delivery = CreateDelivery(channel.Object, deliveryTag: 8);
        var accessor = new BrokerChannelAccess(Mock.Of<ILogger<BrokerChannelAccess>>());

        await accessor.RejectAsync(delivery, requeue: true, CancellationToken.None);

        channel.Verify();
    }

    [Fact]
    public async Task AcknowledgeAsync_Throws_WhenDeliveryTypeUnsupported()
    {
        var accessor = new BrokerChannelAccess(Mock.Of<ILogger<BrokerChannelAccess>>());
        var bogusDelivery = Mock.Of<IInboundDelivery>(); // does not implement MessageDelivery

        await Assert.ThrowsAsync<InvalidOperationException>(() => accessor.AcknowledgeAsync(bogusDelivery, CancellationToken.None));
    }

    [Fact]
    public async Task RejectAsync_Throws_WhenDeliveryTypeUnsupported()
    {
        var accessor = new BrokerChannelAccess(Mock.Of<ILogger<BrokerChannelAccess>>());
        var bogusDelivery = Mock.Of<IInboundDelivery>();

        await Assert.ThrowsAsync<InvalidOperationException>(() => accessor.RejectAsync(bogusDelivery, requeue: false, CancellationToken.None));
    }

    [Fact]
    public async Task ResetAsync_ClosesAndDisposes_WhenChannelOpen()
    {
        var channel = new Mock<IChannel>();
        channel.SetupGet(c => c.IsOpen).Returns(true);
        channel.Setup(c => c.CloseAsync(
                It.IsAny<ushort>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable();
        channel.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask).Verifiable();
        var accessor = new BrokerChannelAccess(Mock.Of<ILogger<BrokerChannelAccess>>());

        await accessor.ResetAsync(channel.Object, CancellationToken.None);

        channel.Verify(c => c.CloseAsync(
            It.IsAny<ushort>(),
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Once);
        channel.Verify(c => c.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task ResetAsync_DisposesOnly_WhenChannelAlreadyClosed()
    {
        var channel = new Mock<IChannel>();
        channel.SetupGet(c => c.IsOpen).Returns(false);
        channel.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask).Verifiable();
        var accessor = new BrokerChannelAccess(Mock.Of<ILogger<BrokerChannelAccess>>());

        await accessor.ResetAsync(channel.Object, CancellationToken.None);

        channel.Verify(c => c.CloseAsync(
            It.IsAny<ushort>(),
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Never);
        channel.Verify(c => c.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task ResetAsync_Disposes_EvenIfCloseThrows()
    {
        var channel = new Mock<IChannel>();
        channel.SetupGet(c => c.IsOpen).Returns(true);
        channel.Setup(c => c.CloseAsync(
                It.IsAny<ushort>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.FromException(new InvalidOperationException("boom")));
        channel.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask).Verifiable();
        var accessor = new BrokerChannelAccess(Mock.Of<ILogger<BrokerChannelAccess>>());

        await accessor.ResetAsync(channel.Object, CancellationToken.None);

        channel.Verify(c => c.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task AcknowledgeAsync_ReleasesGate_WhenBrokerAckThrows()
    {
        var channel = new Mock<IChannel>();
        var invocationCount = 0;
        channel.Setup(c => c.BasicAckAsync(11, false, It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                invocationCount++;
                return invocationCount == 1
                    ? ValueTask.FromException(new InvalidOperationException("ack failure"))
                    : ValueTask.CompletedTask;
            });
        var delivery = CreateDelivery(channel.Object, deliveryTag: 11);
        var accessor = new BrokerChannelAccess(Mock.Of<ILogger<BrokerChannelAccess>>());

        await Assert.ThrowsAsync<InvalidOperationException>(() => accessor.AcknowledgeAsync(delivery, CancellationToken.None));

        await accessor.AcknowledgeAsync(delivery, CancellationToken.None);

        channel.Verify(c => c.BasicAckAsync(11, false, It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task RejectAsync_ReleasesGate_WhenBrokerNackThrows()
    {
        var channel = new Mock<IChannel>();
        var invocationCount = 0;
        channel.Setup(c => c.BasicNackAsync(22, false, false, It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                invocationCount++;
                return invocationCount == 1
                    ? ValueTask.FromException(new InvalidOperationException("nack failure"))
                    : ValueTask.CompletedTask;
            });
        var delivery = CreateDelivery(channel.Object, deliveryTag: 22);
        var accessor = new BrokerChannelAccess(Mock.Of<ILogger<BrokerChannelAccess>>());

        await Assert.ThrowsAsync<InvalidOperationException>(() => accessor.RejectAsync(delivery, requeue: false, CancellationToken.None));

        await accessor.RejectAsync(delivery, requeue: false, CancellationToken.None);

        channel.Verify(c => c.BasicNackAsync(22, false, false, It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task ResetAsync_LogsWarning_WhenCloseOrDisposeFails()
    {
        var channel = new Mock<IChannel>();
        channel.SetupGet(c => c.IsOpen).Returns(true);
        channel.Setup(c => c.CloseAsync(
                It.IsAny<ushort>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.FromException(new InvalidOperationException("close failure")));
        channel.Setup(c => c.DisposeAsync()).Returns(ValueTask.FromException(new InvalidOperationException("dispose failure")));
        var logger = new Mock<ILogger<BrokerChannelAccess>>();
        var accessor = new BrokerChannelAccess(logger.Object);

        await accessor.ResetAsync(channel.Object, CancellationToken.None);

        logger.Verify(l => l.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((state, _) => state.ToString() == "Failed to reset broker channel"),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    private static MessageDelivery CreateDelivery(IChannel channel, ulong deliveryTag)
    {
        var command = new MigrationCommand
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
        var json = JsonSerializer.Serialize(command, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        });
        var body = Encoding.UTF8.GetBytes(json);
        return new MessageDelivery(channel, deliveryTag, body);
    }
}
