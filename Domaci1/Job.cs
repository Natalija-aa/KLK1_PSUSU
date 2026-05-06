namespace Domaci1;

public enum JobType
{
    Prime,
    IO
}

public class Job
{
    public Guid Id { get; set; }    // guide globalno jedinstveni indentifikator
    // jer radi vise niti da svaki job ima jedinstven ID
    public JobType Type { get; set; }
    public string Payload { get; set; } = string.Empty;
    public int Priority { get; set; }
}
