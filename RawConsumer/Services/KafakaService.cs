using Confluent.Kafka;
using RawConsumer.Models;

namespace RawConsumer.Services;

public class KafkaService : IKafkaService
{
    private readonly ConsumerConfig _consumerConfig;
    private readonly string _topic;
    private readonly IValidationService _validationService;
    private readonly IMongoService _mongoService;

    public KafkaService(string bootstrapServers,string topic,string groupId,IValidationService validationService,IMongoService mongoService)
    {
        _topic = topic;
        _validationService = validationService;
        _mongoService = mongoService;

        _consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using IConsumer<Ignore, string> consumer =new ConsumerBuilder<Ignore, string>(_consumerConfig).Build();
        consumer.Subscribe(_topic);
        Console.WriteLine($"Listening to topic: {_topic}");
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<Ignore, string> result =consumer.Consume(cancellationToken);
                string message = result.Message.Value;
                Console.WriteLine($"Received: {message}");

                bool isValid =_validationService.TryValidate(message,out ActivityReading? reading);

                if (!isValid || reading is null)
                {
                    Console.WriteLine("Message rejected");
                    consumer.Commit(result);
                    continue;
                }

                await _mongoService.SaveAsync(reading);

                consumer.Commit(result);

                Console.WriteLine($"Message completed: {reading.EventId}");
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Consumer stopped");
        }
        finally
        {
            consumer.Close();
        }
    }
}