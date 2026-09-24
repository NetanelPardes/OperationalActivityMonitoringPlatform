using System.Net;
using System.Net.Http.Json;
using System.Text;
using Microsoft.Extensions.Configuration;
using PersistenceConsumer.Models;

namespace PersistenceConsumer.Services;

public class ElasticsearchService : IElasticsearchService
{
    private readonly HttpClient _httpClient;
    private readonly string _indexName;

    public ElasticsearchService(
        HttpClient httpClient,
        IConfiguration configuration
    )
    {
        _httpClient = httpClient;

        _indexName = configuration["ELASTICSEARCH_INDEX"]
            ?? throw new InvalidOperationException(
                "Missing ELASTICSEARCH_INDEX"
            );
    }

    public async Task CreateIndexAsync(
        CancellationToken cancellationToken
    )
    {
        using var check = await _httpClient.GetAsync(
            _indexName,
            cancellationToken
        );

        if (check.IsSuccessStatusCode)
        {
            return;
        }

        if (check.StatusCode != HttpStatusCode.NotFound)
        {
            check.EnsureSuccessStatusCode();
        }

        const string mapping = """
            {
              "mappings": {
                "properties": {
                  "id": { "type": "long" },
                  "event_id": { "type": "keyword" },
                  "source_id": { "type": "keyword" },
                  "sector": { "type": "keyword" },
                  "value": { "type": "double" },
                  "mean": { "type": "double" },
                  "standard_deviation": { "type": "double" },
                  "z_score": { "type": "double" },
                  "severity": { "type": "keyword" },
                  "status": { "type": "keyword" },
                  "detected_at": { "type": "date" }
                }
              }
            }
            """;

        using var content = new StringContent(
            mapping,
            Encoding.UTF8,
            "application/json"
        );

        using var response = await _httpClient.PutAsync(
            _indexName,
            content,
            cancellationToken
        );

        response.EnsureSuccessStatusCode();
    }

    public async Task SaveAnomalyAsync(
        Anomaly anomaly,
        string sector,
        CancellationToken cancellationToken
    )
    {
        var document = new
        {
            id = anomaly.Id,
            event_id = anomaly.EventId,
            source_id = anomaly.SourceId,
            sector,
            value = anomaly.Value,
            mean = anomaly.Mean,
            standard_deviation = anomaly.StandardDeviation,
            z_score = anomaly.ZScore,
            severity = anomaly.Severity,
            status = anomaly.Status,
            detected_at = anomaly.DetectedAt
        };

        using var response = await _httpClient.PutAsJsonAsync(
            $"{_indexName}/_doc/{anomaly.Id}",
            document,
            cancellationToken
        );

        response.EnsureSuccessStatusCode();
    }
}