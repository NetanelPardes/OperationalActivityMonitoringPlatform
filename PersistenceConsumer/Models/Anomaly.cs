using System.Text.Json.Serialization;

namespace PersistenceConsumer.Models;

public class Anomaly
{
    public long Id { get; set; }

    [JsonPropertyName("event_id")]
    public string EventId { get; set; } = "";

    [JsonPropertyName("source_id")]
    public string SourceId { get; set; } = "";

    [JsonPropertyName("value")]
    public double Value { get; set; }

    [JsonPropertyName("mean")]
    public double Mean { get; set; }

    [JsonPropertyName("standard_deviation")]
    public double StandardDeviation { get; set; }

    [JsonPropertyName("z_score")]
    public double? ZScore { get; set; }

    [JsonPropertyName("severity")]
    public string Severity { get; set; } = "";

    public string Status { get; set; } = "New";

    [JsonPropertyName("detected_at")]
    public DateTime DetectedAt { get; set; }

    [JsonIgnore]
    public Station? Station { get; set; }
}