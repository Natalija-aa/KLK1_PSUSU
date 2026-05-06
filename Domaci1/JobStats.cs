namespace Domaci1;

// podaci o zavrsenom poslu
public class JobStats
{
    public Guid JobId { get; set; }
    public JobType Type { get; set; }
    public double ExecutionTimeMs { get; set; }
    public bool Success { get; set; }
    public DateTime CompletedAt { get; set; }
}
