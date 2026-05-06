using System.Diagnostics;
using System.Xml.Linq;

namespace Domaci1;

public class ProcessingSystem
{
    private readonly List<Job> _queue;
    private readonly object _queueLock;

    private readonly HashSet<Guid> _submittedIds;
    private readonly Dictionary<Guid, Job> _allJobs;    // kljuc, vrijednost
    private readonly Dictionary<Guid, TaskCompletionSource<int>> _tcsMap;

    private readonly List<JobStats> _completedStats;
    private readonly object _statsLock;

    private readonly Thread[] _workerThreads;
    private volatile bool _isRunning;   // sve niti uvijek citaju stvarnu vrijednost iz memeorije

    private readonly int _maxQueueSize;
    private readonly int _timeoutMs;

    private int _reportCounter;
    private readonly object _reportLock;

    private static readonly object _logLock = new object(); // da vise niti ne bi pisalo istovremeno u log fajl
    public static readonly string OutputDir = Path.Combine(AppContext.BaseDirectory, "output"); // mjesto za cuvanje
    public static readonly string LogFile = Path.Combine(OutputDir, "jobs.log");    // do log fajla

    private readonly IJobExecutor _executor;

    public event JobCompletedHandler? JobCompleted; // ? moze biti null
    public event JobFailedHandler? JobFailed;

    public ProcessingSystem(int workerCount, int maxQueueSize, IJobExecutor? executor = null, int timeoutMs = 2000)
    {
        _queue = new List<Job>();
        _queueLock = new object();
        _submittedIds = new HashSet<Guid>();
        _allJobs = new Dictionary<Guid, Job>();
        _tcsMap = new Dictionary<Guid, TaskCompletionSource<int>>();
        _completedStats = new List<JobStats>();
        _statsLock = new object();
        _isRunning = true;
        _maxQueueSize = maxQueueSize;
        _timeoutMs = timeoutMs;
        _reportCounter = 0;
        _reportLock = new object();
        _executor = executor ?? new DefaultJobExecutor();

        Directory.CreateDirectory(OutputDir);   // output folder

        // da bi uvijek kretala od 0
        foreach (string file in Directory.GetFiles(OutputDir, "report_*.xml"))
            File.Delete(file);
        if (File.Exists(LogFile))
            File.Delete(LogFile);

        _workerThreads = new Thread[workerCount];
        for (int i = 0; i < workerCount; i++)
        {
            // IsBackground moze da se zavrsi program cak i ako nit jos radi
            _workerThreads[i] = new Thread(WorkerLoop) { IsBackground = true };
            _workerThreads[i].Start();
        }

        // nit za izvjestaj
        Thread reportThread = new Thread(ReportLoop) { IsBackground = true };
        reportThread.Start();
    }


    public JobHandle? Submit(Job job, out string? rejectReason)
    {
        lock (_queueLock)
        {
            if (_submittedIds.Contains(job.Id)) // da li sam vec imala ovaj Id
            {
                rejectReason = "duplicate ID";
                return null;
            }

            if (_queue.Count >= _maxQueueSize)  // previse
            {
                rejectReason = "queue full";
                return null;
            }

            rejectReason = null;    // posao nije odbijen

            // rucno da postavim kada je task gotov i sa kojim rezultatom, kada se zavrsi daje rezultat
            var tcs = new TaskCompletionSource<int>();
            _tcsMap[job.Id] = tcs;  // da worker moze da ga nadje
            _submittedIds.Add(job.Id);
            _allJobs[job.Id] = job;
            InsertSorted(job);  // dodaje na slobodno mjesto
            Monitor.Pulse(_queueLock);  // budi worker

            return new JobHandle { Id = job.Id, Result = tcs.Task };
        }
    }

    public IEnumerable<Job> GetTopJobs(int n)
    {
        // da se nit ne mjenja dok uzimamo vrijednost
        lock (_queueLock)
        {
            return _queue.Take(n).ToList(); // pravim kopiju liste za klijenta da ne bi mjenjao orginal
        }
    }

    public Job? GetJob(Guid id)
    {
        // kako nebi citali dok se mjenja
        lock (_queueLock)
        {
            _allJobs.TryGetValue(id, out Job? job); // da nadje vrijednost po kljucu
            // [id] bi dao gresku ako id ne postoji
            return job;
        }
    }

    public void Stop()
    {
        _isRunning = false; // da sve niti prestanu sa radom
        lock (_queueLock)
        {
            Monitor.PulseAll(_queueLock);   // probudi sve niti da bi vidjele false i stale
        }
    }

    // ddaoj posao po prioritetu
    private void InsertSorted(Job job)
    {
        int i = 0;
        while (i < _queue.Count && _queue[i].Priority <= job.Priority)
        {
            i++;
        }
        _queue.Insert(i, job);
    }

    private void WorkerLoop()
    {
        while (_isRunning)
        {
            Job? job = null;
            // da ne bi bespotrebno vise niti radilo jedan posao
            lock (_queueLock)
            {
                while (_queue.Count == 0 && _isRunning)
                {
                    Monitor.Wait(_queueLock, 500);  // otpusta lock
                }
                if (_queue.Count > 0)
                {
                    job = _queue[0];    // zbog prioriteta
                    _queue.RemoveAt(0);
                }
            }

            if (job != null)
            {
                try
                {
                    ProcessJobWithRetry(job);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Unhandled exception for job {job.Id}: {ex.Message}");
                }
            }
        }
    }

    private void ProcessJobWithRetry(Job job)
    {
        const int maxAttempts = 3;  // 1 org + 2 nova pokusaja

        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            Stopwatch sw = Stopwatch.StartNew();
            bool success = false;
            int result = 0;
            Exception? error = null;

            try
            {
                Task<int> workTask = Task.Run(() => _executor.Execute(job));
                bool completed = false;

                try
                {
                    completed = workTask.Wait(_timeoutMs);  // koliko se ceka
                }
                catch (AggregateException ae)
                {
                    error = ae.InnerException ?? ae;
                    completed = true;
                }

                sw.Stop();  // prekini mjerenje

                if (error == null && !completed)    // nije se desilo na vrijeme
                {
                    error = new TimeoutException($"Job exceeded {_timeoutMs}ms limit");
                }
                else if (error == null)
                {
                    result = workTask.Result;
                    success = true;
                }
            }
            catch (Exception ex)
            {
                sw.Stop();
                error = ex;
            }

            // uspjelo je
            if (success)
            {
                // dodajem u listu
                lock (_statsLock)
                {
                    _completedStats.Add(new JobStats
                    {
                        JobId = job.Id,
                        Type = job.Type,
                        ExecutionTimeMs = sw.Elapsed.TotalMilliseconds, // koliko je trajao posao
                        Success = true,
                        CompletedAt = DateTime.Now  // vrijeme izvrsavanja
                    });
                }

                JobCompleted?.Invoke(job.Id, result, sw.Elapsed.TotalMilliseconds, job.Type);   // ? da li se iko pretplatio na event

                lock (_queueLock)
                {
                    if (_tcsMap.TryGetValue(job.Id, out var tcs))
                        tcs.TrySetResult(result);  // excetude result
                }
                return; // izadji
            }

            JobFailed?.Invoke(job.Id, error?.Message ?? "Unknown error", attempt, job.Type);    

            if (attempt == maxAttempts)
            {
                Task.Run(() => WriteLog("ABORT", job.Id, "-"));

                lock (_statsLock)
                {
                    // statistika
                    _completedStats.Add(new JobStats
                    {
                        JobId = job.Id,
                        Type = job.Type,
                        ExecutionTimeMs = 0,
                        Success = false,
                        CompletedAt = DateTime.Now
                    });
                }

                lock (_queueLock)
                {
                    if (_tcsMap.TryGetValue(job.Id, out var tcs))
                        tcs.TrySetException(error ?? new Exception("Job aborted after max retries"));
                }
            }
        }
    }

    private void ReportLoop()
    {
        while (_isRunning)  // radi dok je sistem aktivan
        {
            Thread.Sleep(60000);
            if (_isRunning)
            {
                try
                {
                    GenerateReport();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Report generation failed: {ex.Message}");
                }
            }
        }
    }

    public void GenerateReport()
    {
        List<JobStats> stats;
        lock (_statsLock)
        {
            stats = new List<JobStats>(_completedStats);    // kopija liste statistike
        }

        var reportData = from stat in stats
                         group stat by stat.Type into g // grupisanje po tipu
                         orderby g.Key
                         select new
                         {
                             Type = g.Key,
                             CompletedCount = (from s in g where s.Success select s).Count(),
                             AvgTimeMs = (from s in g where s.Success select s.ExecutionTimeMs)
                                          .DefaultIfEmpty(0).Average(),
                             FailedCount = (from s in g where !s.Success select s).Count()
                         };
        // xml dokument
        XElement reportElement = new XElement("Report",
            new XAttribute("GeneratedAt", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")));

        foreach (var r in reportData)
        {
            reportElement.Add(new XElement("JobType",
                new XAttribute("Type", r.Type.ToString()),
                new XElement("CompletedCount", r.CompletedCount),
                new XElement("AvgTimeMs", r.AvgTimeMs.ToString("F2")),
                new XElement("FailedCount", r.FailedCount)));
        }

        int index;
        lock (_reportLock)
        {
            index = _reportCounter % 10;
            _reportCounter++;
        }

        string fileName = Path.Combine(OutputDir, $"report_{index}.xml");
        reportElement.Save(fileName);   // xml save
        Console.WriteLine($"[REPORT] Generated: {fileName}");
    }

    public static void WriteLog(string status, Guid jobId, string result, string? logFile = null)
    {
        try
        {
            string targetFile = logFile ?? LogFile;
            string line = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] [{status}] {jobId}, {result}";
            lock (_logLock)
            {
                File.AppendAllText(targetFile, line + Environment.NewLine);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] Failed to write log: {ex.Message}");
        }
    }
}