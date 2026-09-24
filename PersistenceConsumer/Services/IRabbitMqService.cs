using PersistenceConsumer.Models;

namespace PersistenceConsumer.Services;

public interface IRabbitMqService
{
    Task PublishAlertAsync(
        Anomaly anomaly,
        CancellationToken cancellationToken
    );
}