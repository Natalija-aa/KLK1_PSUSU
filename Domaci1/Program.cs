using System.Xml.Linq;
using Domaci1;

try
{
    XElement config = XElement.Load("SystemConfig.xml");
    int workerCount = int.Parse(config.Element("WorkerCount")!.Value);
    int maxQueueSize = int.Parse(config.Element("MaxQueueSize")!.Value);

    ProcessingSystem system = new ProcessingSystem(workerCount, maxQueueSize);

    //  lambda fja — multicast delegates
    JobCompletedHandler consoleCompleted = (jobId, result, timeMs, type) =>
    {
        Console.WriteLine($"[COMPLETED] {type,-5} | {jobId} | Result: {result,3} | Time: {timeMs:F0}ms");
    };
    JobCompletedHandler logCompleted = (jobId, result, timeMs, type) =>
    {
        Task.Run(() => ProcessingSystem.WriteLog("COMPLETED", jobId, result.ToString()));
    };
    JobCompletedHandler combinedCompleted = consoleCompleted + logCompleted;
    system.JobCompleted += combinedCompleted;   // pretplati na event


    JobFailedHandler consoleFailed = (jobId, reason, attempt, type) =>
    {
        Console.WriteLine($"[FAILED]    {type,-5} | {jobId} | Attempt: {attempt} | {reason}");
    };
    JobFailedHandler logFailed = (jobId, reason, attempt, type) =>
    {
        Task.Run(() => ProcessingSystem.WriteLog("FAILED", jobId, $"Attempt {attempt}: {reason}"));
    };
    JobFailedHandler combinedFailed = consoleFailed + logFailed;
    system.JobFailed += combinedFailed;


    // podaci iz XML-a -> LINQ
    var initialJobs = from jobEl in config.Descendants("Job")   // trazi Job el u XML fajlu
                      select new Job
                      {
                          Id = Guid.NewGuid(),
                          Type = Enum.Parse<JobType>(jobEl.Attribute("Type")!.Value),
                          Payload = jobEl.Attribute("Payload")!.Value,
                          Priority = int.Parse(jobEl.Attribute("Priority")!.Value)
                      };

    Console.WriteLine("=== Loading initial jobs from XML ===");
    // submit job
    foreach (Job job in initialJobs)
    {
        JobHandle? handle = system.Submit(job, out _);  // razlog odbijanja nije vazan
        if (handle != null)
            Console.WriteLine($"[INIT] Submitted {job.Type,-5} | Priority: {job.Priority} | Id: {job.Id}");
    }

    // producer niti koje nasumicno dodaju poslove u sistem
    Thread[] producers = new Thread[workerCount];
    for (int i = 0; i < workerCount; i++)
    {
        int threadIndex = i;
        producers[threadIndex] = new Thread(() =>
        {
            Random localRng = new Random(threadIndex * 17 + Environment.TickCount); // svaka nit ima razlicit seed
            while (true)
            {
                try
                {
                    Job job = new Job
                    {
                        Id = Guid.NewGuid(),
                        Priority = localRng.Next(1, 6)
                    };

                    if (localRng.Next(2) == 0)  // prime = 0 ili io = 1
                    {
                        job.Type = JobType.Prime;
                        int limit = localRng.Next(1, 6) * 5000; // random limit
                        int threads = localRng.Next(1, 9);
                        job.Payload = $"numbers:{limit},threads:{threads}";
                    }
                    else
                    {
                        job.Type = JobType.IO;
                        int delay = localRng.Next(100, 4000);
                        job.Payload = $"delay:{delay}";
                    }

                    JobHandle? handle = system.Submit(job, out string? reason);
                    if (handle == null) // posao je odbijen
                        Console.WriteLine($"[PRODUCER {threadIndex}] Job rejected — {reason}");

                    Thread.Sleep(localRng.Next(300, 1500));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[PRODUCER {threadIndex}] Error: {ex.Message}");
                }
            }
        });
        producers[i].IsBackground = true;   //i ako radi moze se prekinuti
        producers[i].Start();
    }

    Console.WriteLine($"\n=== System running: {workerCount} workers, max queue {maxQueueSize} ===");
    Console.WriteLine("Press ENTER to exit...\n");
    Console.ReadLine();

    system.GenerateReport();
    system.Stop();
}
catch (Exception ex)
{
    Console.WriteLine($"[FATAL] {ex.Message}");
}
