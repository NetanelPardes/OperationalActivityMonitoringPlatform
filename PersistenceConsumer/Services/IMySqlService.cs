using PersistenceConsumer.Models;

namespace PersistenceConsumer.Services;

public interface IMySqlService
{
    Task CreateTablesAsync(CancellationToken cancellationToken);

    Task<(Anomaly Anomaly, string Sector)> SaveAnomalyAsync(
        Anomaly anomaly,
        CancellationToken cancellationToken
    );
}