using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PersistenceConsumer;
using PersistenceConsumer.Data;
using PersistenceConsumer.Services;

var builder = Host.CreateApplicationBuilder(args);

var mysqlConnectionString =
    builder.Configuration["MYSQL_CONNECTION_STRING"]
    ?? throw new InvalidOperationException(
        "Missing MYSQL_CONNECTION_STRING"
    );

var elasticsearchUrl =
    builder.Configuration["ELASTICSEARCH_URL"]
    ?? throw new InvalidOperationException(
        "Missing ELASTICSEARCH_URL"
    );

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseMySql(
        mysqlConnectionString,
        new MySqlServerVersion(new Version(8, 0, 0))
    )
);

builder.Services.AddScoped<IMySqlService, MySqlService>();
builder.Services.AddScoped<IRabbitMqService, RabbitMqService>();

builder.Services.AddHttpClient<
    IElasticsearchService,
    ElasticsearchService
>(client =>
{
    client.BaseAddress = new Uri(
        elasticsearchUrl.TrimEnd('/') + "/"
    );
});

builder.Services.AddHostedService<AnomalyConsumer>();

await builder.Build().RunAsync();