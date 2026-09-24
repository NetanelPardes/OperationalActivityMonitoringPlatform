using Microsoft.Extensions.Configuration;
using RawConsumer.Services;

IConfiguration configuration =
    new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json")
        .Build();

string kafkaServer = configuration["Kafka:BootstrapServers"]!;

string kafkaTopic = configuration["Kafka:Topic"]!;

string kafkaGroupId = configuration["Kafka:GroupId"]!;

string mongoConnection = configuration["MongoDb:ConnectionString"]!;

string mongoDatabase = configuration["MongoDb:DatabaseName"]!;

string mongoCollection = configuration["MongoDb:CollectionName"]!;

IValidationService validationService = new ValidationService();

IMongoService mongoService = new MongoService(mongoConnection,mongoDatabase,mongoCollection);

IKafkaService kafkaService =new KafkaService(kafkaServer,kafkaTopic,kafkaGroupId,validationService,mongoService);

using CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellationTokenSource.Cancel();
};

Console.WriteLine("Raw Consumer started");
Console.WriteLine("Press Ctrl+C to stop");

await kafkaService.StartAsync(cancellationTokenSource.Token);