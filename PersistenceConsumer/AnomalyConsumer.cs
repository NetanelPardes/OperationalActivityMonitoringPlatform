using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PersistenceConsumer.Models;
using PersistenceConsumer.Services;

namespace PersistenceConsumer;

public class AnomalyConsumer : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<AnomalyConsumer> _logger;

    public AnomalyConsumer(
        IConfiguration configuration,
        IServiceScopeFactory scopeFactory,
        ILogger<AnomalyConsumer> logger
    )
    {
        _configuration = configuration;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken
    )
    {
        using (var scope = _scopeFactory.CreateScope())
        {
            var mysqlService = scope.ServiceProvider
                .GetRequiredService<IMySqlService>();

            var elasticsearchService = scope.ServiceProvider
                .GetRequiredService<IElasticsearchService>();

            await mysqlService.CreateTablesAsync(
                stoppingToken
            );

            await elasticsearchService.CreateIndexAsync(
                stoppingToken
            );
        }

        var kafkaServer = Required("KAFKA_BOOTSTRAP_SERVERS");
        var topic = Required("ANOMALIES_TOPIC");
        var groupId = Required("PERSISTENCE_GROUP_ID");

        var config = new ConsumerConfig
        {
            BootstrapServers = kafkaServer,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer =
            new ConsumerBuilder<string, string>(config).Build();

        consumer.Subscribe(topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var message = consumer.Consume(stoppingToken);

                var anomaly =
                    JsonSerializer.Deserialize<Anomaly>(
                        message.Message.Value
                    )
                    ?? throw new InvalidDataException(
                        "Empty anomaly message"
                    );

                using var scope = _scopeFactory.CreateScope();

                var mysqlService = scope.ServiceProvider
                    .GetRequiredService<IMySqlService>();

                var elasticsearchService = scope.ServiceProvider
                    .GetRequiredService<IElasticsearchService>();

                var rabbitMqService = scope.ServiceProvider
                    .GetRequiredService<IRabbitMqService>();

                var (savedAnomaly, sector) =
                    await mysqlService.SaveAnomalyAsync(
                        anomaly,
                        stoppingToken
                    );

                await elasticsearchService.SaveAnomalyAsync(
                    savedAnomaly,
                    sector,
                    stoppingToken
                );

                if (savedAnomaly.Severity == "Critical")
                {
                    await rabbitMqService.PublishAlertAsync(
                        savedAnomaly,
                        stoppingToken
                    );
                }

                consumer.Commit(message);

                _logger.LogInformation(
                    "Processed anomaly {EventId} with ID {Id}",
                    savedAnomaly.EventId,
                    savedAnomaly.Id
                );
            }
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            consumer.Close();
        }
    }

    private string Required(string name)
    {
        return _configuration[name]
            ?? throw new InvalidOperationException(
                $"Missing configuration: {name}"
            );
    }
}