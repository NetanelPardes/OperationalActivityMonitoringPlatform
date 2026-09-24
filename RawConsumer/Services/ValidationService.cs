using System.Text.Json;
using RawConsumer.Models;
using System.Globalization;

namespace RawConsumer.Services;

public class ValidationService : IValidationService
{
    public bool TryValidate(string message,out ActivityReading? reading)
    {
        reading = null;

        try
        {
            using JsonDocument document =JsonDocument.Parse(message);

            JsonElement root = document.RootElement;

            if (!root.TryGetProperty("event_id",out JsonElement eventIdElement))
            {
                Console.WriteLine("event_id is missing");
                return false;
            }

            string? eventId = eventIdElement.GetString();

            if (string.IsNullOrWhiteSpace(eventId))
            {
                Console.WriteLine("event_id is empty");
                return false;
            }

            if (!root.TryGetProperty("source_id",out JsonElement sourceIdElement))
            {
                Console.WriteLine("source_id is missing");
                return false;
            }

            string? sourceId = sourceIdElement.GetString();

            if (string.IsNullOrWhiteSpace(sourceId))
            {
                Console.WriteLine("source_id is empty");
                return false;
            }

            if (!root.TryGetProperty("timestamp",out JsonElement timestampElement))
            {
                Console.WriteLine("timestamp is missing");
                return false;
            }

            string? timestampText =
                timestampElement.GetString();

            if (!DateTime.TryParse(timestampText,out DateTime timestamp))
            {
                Console.WriteLine("timestamp is not valid");
                return false;
            }

            if (!root.TryGetProperty("value",out JsonElement valueElement))
            {
                Console.WriteLine("value is missing");
                return false;
            }

            string valueText = valueElement.ToString();

            bool isNumber = double.TryParse(valueText,NumberStyles.Any,CultureInfo.InvariantCulture,out double value);

            if (!isNumber)
            {
                Console.WriteLine("value is not a number");
                return false;
            }

            reading = new ActivityReading
            {
                EventId = eventId,
                SourceId = sourceId,
                Timestamp = timestamp,
                Value = value
            };

            return true;
        }
        catch (JsonException)
        {
            Console.WriteLine("JSON is not valid");
            return false;
        }
    }
}