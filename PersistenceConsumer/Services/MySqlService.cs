using Microsoft.EntityFrameworkCore;
using PersistenceConsumer.Data;
using PersistenceConsumer.Models;

namespace PersistenceConsumer.Services;

public class MySqlService : IMySqlService
{
    private readonly AppDbContext _dbContext;

    public MySqlService(AppDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    public async Task CreateTablesAsync(
        CancellationToken cancellationToken
    )
    {
        await _dbContext.Database.ExecuteSqlRawAsync(
            """
            CREATE TABLE IF NOT EXISTS Anomalies (
                Id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                EventId VARCHAR(100) NOT NULL,
                SourceId VARCHAR(50) NOT NULL,
                Value DOUBLE NOT NULL,
                Mean DOUBLE NOT NULL,
                StandardDeviation DOUBLE NOT NULL,
                ZScore DOUBLE NULL,
                Severity VARCHAR(20) NOT NULL,
                Status VARCHAR(20) NOT NULL DEFAULT 'New',
                DetectedAt DATETIME NOT NULL,
                UNIQUE KEY UX_Anomalies_EventId (EventId),
                CONSTRAINT FK_Anomalies_Stations
                    FOREIGN KEY (SourceId) REFERENCES Stations(Id)
            )
            """,
            cancellationToken
        );

        await _dbContext.Database.ExecuteSqlRawAsync(
            """
            CREATE TABLE IF NOT EXISTS AlertLog (
                Id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                AnomalyId BIGINT NOT NULL,
                SentAt DATETIME NOT NULL,
                Attempts INT NOT NULL,
                CONSTRAINT FK_AlertLog_Anomalies
                    FOREIGN KEY (AnomalyId) REFERENCES Anomalies(Id)
            )
            """,
            cancellationToken
        );
    }

    public async Task<(Anomaly Anomaly, string Sector)> SaveAnomalyAsync(
        Anomaly anomaly,
        CancellationToken cancellationToken
    )
    {
        Validate(anomaly);

        var station = await _dbContext.Stations
            .AsNoTracking()
            .SingleOrDefaultAsync(
                x => x.Id == anomaly.SourceId,
                cancellationToken
            )
            ?? throw new InvalidOperationException(
                $"Station does not exist: {anomaly.SourceId}"
            );

        var existing = await _dbContext.Anomalies
            .AsNoTracking()
            .SingleOrDefaultAsync(
                x => x.EventId == anomaly.EventId,
                cancellationToken
            );

        if (existing is not null)
        {
            return (existing, station.Sector);
        }

        anomaly.Id = 0;
        anomaly.Status = "New";
        anomaly.DetectedAt = anomaly.DetectedAt.ToUniversalTime();

        _dbContext.Anomalies.Add(anomaly);
        await _dbContext.SaveChangesAsync(cancellationToken);

        return (anomaly, station.Sector);
    }

    private static void Validate(Anomaly anomaly)
    {
        if (string.IsNullOrWhiteSpace(anomaly.EventId) ||
            string.IsNullOrWhiteSpace(anomaly.SourceId))
        {
            throw new InvalidDataException(
                "Missing event_id or source_id"
            );
        }

        if (anomaly.Severity is not ("Warning" or "Critical"))
        {
            throw new InvalidDataException(
                $"Invalid severity: {anomaly.Severity}"
            );
        }

        if (anomaly.DetectedAt == default)
        {
            throw new InvalidDataException(
                "Missing or invalid detected_at"
            );
        }

        if (!double.IsFinite(anomaly.Value) ||
            !double.IsFinite(anomaly.Mean) ||
            !double.IsFinite(anomaly.StandardDeviation) ||
            (anomaly.ZScore.HasValue &&
             !double.IsFinite(anomaly.ZScore.Value)))
        {
            throw new InvalidDataException(
                "Invalid numeric value"
            );
        }
    }
}