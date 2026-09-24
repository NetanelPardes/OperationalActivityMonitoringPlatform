using MongoDB.Driver;
using RawConsumer.Models;

namespace RawConsumer.Services;

public class MongoService : IMongoService
{
    private readonly IMongoCollection<ActivityReading> _collection;
    public MongoService(string connectionString,string databaseName,string collectionName)
    {
        MongoClient client =new MongoClient(connectionString);
        IMongoDatabase database =client.GetDatabase(databaseName);
        _collection =database.GetCollection<ActivityReading>(collectionName);
    }

    public async Task SaveAsync(ActivityReading reading)
    {
        var filter = Builders<ActivityReading>.Filter.Eq(
            x => x.EventId, reading.EventId
        );

        var update = Builders<ActivityReading>.Update
            .Set(x => x.SourceId, reading.SourceId)
            .Set(x => x.Timestamp, reading.Timestamp)
            .Set(x => x.Value, reading.Value);

        await _collection.UpdateOneAsync(
            filter,
            update,
            new UpdateOptions { IsUpsert = true }
        );

        Console.WriteLine($"Saved to MongoDB: {reading.EventId}");
    }
}