using RawConsumer.Models;

namespace RawConsumer.Services;

public interface IMongoService
{
    Task SaveAsync(ActivityReading reading);
}