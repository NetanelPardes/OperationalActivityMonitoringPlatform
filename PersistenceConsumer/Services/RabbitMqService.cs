using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using PersistenceConsumer.Models;
using RabbitMQ.Client;

namespace PersistenceConsumer.Services;

public class RabbitMqService : IRabbitMqService
{
    private readonly string _host;
    private readonly string _queue;

    public RabbitMqService(IConfiguration configuration)
    {
        _host = configuration["RABBITMQ_HOST"]
            ?? throw new InvalidOperationException(
                "Missing RABBITMQ_HOST"
            );

        _queue = configuration["ALERTS_QUEUE"]
            ?? throw new InvalidOperationException(
                "Missing ALERTS_QUEUE"
            );
    }

    public Task PublishAlertAsync(
        Anomaly anomaly,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();

        var factory = new ConnectionFactory
        {
            HostName = _host
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.QueueDeclare(
            queue: _queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null
        );

        channel.ConfirmSelect();

        var task = new
        {
            anomaly_id = anomaly.Id,
            source_id = anomaly.SourceId,
            severity = anomaly.Severity
        };

        var properties = channel.CreateBasicProperties();
        properties.Persistent = true;

        var body = Encoding.UTF8.GetBytes(
            JsonSerializer.Serialize(task)
        );

        channel.BasicPublish(
            exchange: "",
            routingKey: _queue,
            basicProperties: properties,
            body: body
        );

        channel.WaitForConfirmsOrDie(
            TimeSpan.FromSeconds(10)
        );

        return Task.CompletedTask;
    }
}