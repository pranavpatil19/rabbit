using System.Collections.Generic;
using WorkerHost.RabbitMq.Configuration;
using WorkerHost.RabbitMq.Infrastructure;
using Xunit;

namespace WorkerHost.Tests.RabbitMq;

public sealed class QueueBindingsManagerTests
{
    [Fact]
    public void CreateQueueArguments_IncludesDeadLetterSettings()
    {
        var options = new BrokerOptions
        {
            WorkEndpoint = new WorkEndpointOptions
            {
                Exchange = new ExchangeOptions
                {
                    Name = "work-commands",
                    Type = "direct",
                },
                Queue = new QueueOptions
                {
                    Name = "work-requests",
                    Durable = true,
                },
                DeadLetter = new DeadLetterOptions
                {
                    Enabled = true,
                    ExchangeName = "work-requests.dlx",
                    ExchangeType = "direct",
                    QueueName = "work-requests.dlx.queue",
                    RoutingKey = "work-requests.dlx",
                    MessageTtlMilliseconds = 42_000,
                },
            },
        };

        var manager = new QueueBindingsManager(options);

        var args = manager.CreateQueueArguments();

        Assert.Equal("work-requests.dlx", args["x-dead-letter-exchange"]);
        Assert.Equal("work-requests.dlx", args["x-dead-letter-routing-key"]);
        Assert.Equal(42_000, args["x-message-ttl"]);
    }

    [Fact]
    public void CreateQueueArguments_PreservesCustomOverrides()
    {
        var options = new BrokerOptions
        {
            WorkEndpoint = new WorkEndpointOptions
            {
                Queue = new QueueOptions
                {
                    Name = "custom-queue",
                    Arguments = new Dictionary<string, object?>
                    {
                        ["x-dead-letter-exchange"] = "custom.dlx",
                        ["x-dead-letter-routing-key"] = "custom.route",
                        ["x-message-ttl"] = 123,
                    },
                },
                DeadLetter = new DeadLetterOptions
                {
                    Enabled = true,
                    ExchangeName = "ignored",
                    RoutingKey = "ignored",
                    MessageTtlMilliseconds = 456,
                },
            },
        };

        var manager = new QueueBindingsManager(options);

        var args = manager.CreateQueueArguments();

        Assert.Equal("custom.dlx", args["x-dead-letter-exchange"]);
        Assert.Equal("custom.route", args["x-dead-letter-routing-key"]);
        Assert.Equal(123, args["x-message-ttl"]);
    }
}
