namespace Domaci1;

public delegate void JobCompletedHandler(Guid jobId, int result, double executionTimeMs, JobType type);
public delegate void JobFailedHandler(Guid jobId, string reason, int attempt, JobType type);
