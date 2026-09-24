using RawConsumer.Models;

namespace RawConsumer.Services;

public interface IValidationService
{
    bool TryValidate(string message,out ActivityReading? reading);
}