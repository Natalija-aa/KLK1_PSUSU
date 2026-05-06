namespace Domaci1;

public class JobHandle
{
    public Guid Id { get; set; }    // isti ID kao u Job.Id
    public Task<int> Result { get; set; } = null!;
}
