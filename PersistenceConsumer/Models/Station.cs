namespace PersistenceConsumer.Models;

public class Station
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string Sector { get; set; } = "";
    public string Status { get; set; } = "";
    public DateTime CreatedAt { get; set; }
}