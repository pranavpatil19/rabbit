using System.ComponentModel.DataAnnotations;
using System.Linq;
using Microsoft.Extensions.Configuration;
using WorkerHost.RabbitMq.Configuration;
using Xunit;

namespace WorkerHost.Tests.Configuration;

public class BrokerOptionsTests
{
    [Fact]
    public void Validation_Fails_For_Invalid_Concurrency()
    {
        var config = new BrokerOptions
        {
            Connection = new BrokerConnectionOptions
            {
                ConnectionString = "amqp://guest:guest@localhost/",
                Host = "localhost",
                Port = 5672,
                UserName = "guest",
                Password = "guest",
                ClientProvidedName = "test-worker",
            },
            WorkEndpoint = new WorkEndpointOptions
            {
                Exchange = new ExchangeOptions
                {
                    Name = "ex",
                    Type = "direct",
                },
                Queue = new QueueOptions
                {
                    Name = "queue",
                },
                RoutingKey = "key",
                DeclareInfrastructure = true,
            },
            DefaultBatchLimit = 10,
            MaxBatchLimit = 200,
            DefaultConcurrency = 0,
            MaxConcurrency = 32,
            PullTimeoutSeconds = 1,
            RequeueOnError = true,
            MaxRequeueAttempts = 5,
            LogSequenceChannelName = "log-channel",
        };

        var results = new List<ValidationResult>();
        var isValid = Validator.TryValidateObject(config, new ValidationContext(config), results, validateAllProperties: true);

        Assert.False(isValid);
        Assert.Contains(results, r => r.MemberNames.Contains(nameof(BrokerOptions.DefaultConcurrency)));
    }

    [Fact]
    public void Binding_Uses_Defaults_When_Config_Missing()
    {
        var data = new Dictionary<string, string?>
        {
            ["BrokerOptions:Connection:ClientProvidedName"] = "custom-worker",
            ["BrokerOptions:DefaultConcurrency"] = "10",
        };
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(data!)
            .Build();

        var options = new BrokerOptions();
        configuration.GetSection(nameof(BrokerOptions)).Bind(options);

        Assert.Equal(BrokerDefaults.Host, options.Connection.Host);
        Assert.Equal("custom-worker", options.Connection.ClientProvidedName);
        Assert.Equal(10, options.DefaultConcurrency);
        Assert.Equal(BrokerDefaults.WorkExchange, options.WorkEndpoint.Exchange.Name);
        Assert.Equal(BrokerDefaults.WorkQueue, options.WorkEndpoint.Queue.Name);
    }

    [Fact]
    public void Validation_Succeeds_For_Defaults()
    {
        var config = CreateValidConfig();

        var results = new List<ValidationResult>();
        var isValid = Validator.TryValidateObject(config, new ValidationContext(config), results, validateAllProperties: true);

        Assert.True(isValid);
    }
    private static BrokerOptions CreateValidConfig() => new()
    {
        Connection = new BrokerConnectionOptions
        {
            ConnectionString = "amqp://guest:guest@localhost/",
            Host = "localhost",
            Port = 5672,
            UserName = "guest",
            Password = "guest",
            ClientProvidedName = "test-worker",
        },
        WorkEndpoint = new WorkEndpointOptions
        {
            Exchange = new ExchangeOptions
            {
                Name = "ex",
                Type = "direct",
            },
            Queue = new QueueOptions
            {
                Name = "queue",
            },
            RoutingKey = "key",
            DeclareInfrastructure = true,
        },
        DefaultBatchLimit = 10,
        MaxBatchLimit = 200,
        DefaultConcurrency = 5,
        MaxConcurrency = 32,
        PullTimeoutSeconds = 1,
        RequeueOnError = true,
        MaxRequeueAttempts = 5,
        LogSequenceChannelName = "log-channel",
    };
}
