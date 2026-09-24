using PersistenceConsumer.Models;

namespace PersistenceConsumer.Services;

public interface IElasticsearchService
{
    Task CreateIndexAsync(CancellationToken cancellationToken);

    Task SaveAnomalyAsync(
        Anomaly anomaly,
        string sector,
        CancellationToken cancellationToken
    );
}