namespace RawConsumer.Services;

public interface IKafkaService
{
    Task StartAsync(CancellationToken cancellationToken);
}