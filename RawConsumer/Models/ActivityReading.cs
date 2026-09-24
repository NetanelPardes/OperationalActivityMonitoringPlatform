using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace RawConsumer.Models;

public class ActivityReading
{
    [BsonId]
    public ObjectId Id { get; set; }

    [BsonElement("event_id")]
    public string EventId { get; set; } = string.Empty;

    [BsonElement("source_id")]
    public string SourceId { get; set; } = string.Empty;

    [BsonElement("timestamp")]
    public DateTime Timestamp { get; set; }

    [BsonElement("value")]
    public double Value { get; set; }
}