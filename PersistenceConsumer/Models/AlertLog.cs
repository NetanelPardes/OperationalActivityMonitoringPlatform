namespace PersistenceConsumer.Models;

public class AlertLog
{
    public long Id { get; set; }
    public long AnomalyId { get; set; }
    public DateTime SentAt { get; set; }
    public int Attempts { get; set; }

    public Anomaly? Anomaly { get; set; }
}