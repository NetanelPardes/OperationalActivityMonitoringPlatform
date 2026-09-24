using Microsoft.EntityFrameworkCore;
using PersistenceConsumer.Models;

namespace PersistenceConsumer.Data;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options)
        : base(options)
    {
    }

    public DbSet<Station> Stations => Set<Station>();
    public DbSet<Anomaly> Anomalies => Set<Anomaly>();
    public DbSet<AlertLog> AlertLogs => Set<AlertLog>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Station>(entity =>
        {
            entity.ToTable("Stations");
            entity.HasKey(x => x.Id);

            entity.Property(x => x.Id).HasMaxLength(50);
            entity.Property(x => x.Name).HasMaxLength(100);
            entity.Property(x => x.Sector).HasMaxLength(100);
            entity.Property(x => x.Status).HasMaxLength(20);
            entity.Property(x => x.CreatedAt).HasColumnType("datetime");
        });

        modelBuilder.Entity<Anomaly>(entity =>
        {
            entity.ToTable("Anomalies");
            entity.HasKey(x => x.Id);

            entity.Property(x => x.Id).ValueGeneratedOnAdd();
            entity.Property(x => x.EventId).HasMaxLength(100);
            entity.Property(x => x.SourceId).HasMaxLength(50);
            entity.Property(x => x.Severity).HasMaxLength(20);
            entity.Property(x => x.Status).HasMaxLength(20);
            entity.Property(x => x.DetectedAt).HasColumnType("datetime");

            entity.HasIndex(x => x.EventId).IsUnique();

            entity.HasOne(x => x.Station)
                .WithMany()
                .HasForeignKey(x => x.SourceId);
        });

        modelBuilder.Entity<AlertLog>(entity =>
        {
            entity.ToTable("AlertLog");
            entity.HasKey(x => x.Id);

            entity.Property(x => x.Id).ValueGeneratedOnAdd();
            entity.Property(x => x.SentAt).HasColumnType("datetime");

            entity.HasOne(x => x.Anomaly)
                .WithMany()
                .HasForeignKey(x => x.AnomalyId);
        });
    }
}