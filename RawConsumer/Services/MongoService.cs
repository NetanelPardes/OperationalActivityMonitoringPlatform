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
        await _collection.InsertOneAsync(reading);
        Console.WriteLine($"Saved to MongoDB: {reading.EventId}");
    }
}