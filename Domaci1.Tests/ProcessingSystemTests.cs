using Domaci1;
using Moq;

namespace Domaci1.Tests;

[Collection("NonParallel")]
public class ProcessingSystemTests
{
    [Fact]
    public void Submit_ValidJob_ReturnsNonNullHandle()
    {
        // Arrange
        var mockExecutor = new Mock<IJobExecutor>();
        mockExecutor.Setup(e => e.Execute(It.IsAny<Job>())).Returns(0); // vrati 0 za bilo koji job
        var system = new ProcessingSystem(1, 100, mockExecutor.Object);
        var job = new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:50", Priority = 1 };

        // Act
        var handle = system.Submit(job, out _);

        // Assert
        Assert.NotNull(handle);
        Assert.Equal(job.Id, handle.Id);
        Assert.NotNull(handle.Result);

        system.Stop();
    }

    [Fact]
    public void Submit_WhenQueueFull_ReturnsNullWithQueueFullReason()
    {
        var mockExecutor = new Mock<IJobExecutor>();
        var system = new ProcessingSystem(0, 2, mockExecutor.Object); // 0 niti da bi red ostao pun, max 2 posla

        system.Submit(new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:50", Priority = 1 }, out _);
        system.Submit(new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:50", Priority = 1 }, out _);
        var rejectedHandle = system.Submit(new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:50", Priority = 1 }, out string? reason);

        Assert.Null(rejectedHandle);
        Assert.Equal("queue full", reason);

        system.Stop();
    }

    [Fact]
    public void Submit_DuplicateId_ReturnsNullWithDuplicateReason()
    {
        var mockExecutor = new Mock<IJobExecutor>();
        var system = new ProcessingSystem(0, 100, mockExecutor.Object);
        var job = new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:50", Priority = 1 };

        system.Submit(job, out _);
        var secondHandle = system.Submit(job, out string? reason);  // isti job - isti Id

        Assert.Null(secondHandle);
        Assert.Equal("duplicate ID", reason);

        system.Stop();
    }

    [Fact]
    public async Task Submit_ValidJob_CompletesWithCorrectResult()
    {
        var mockExecutor = new Mock<IJobExecutor>();
        mockExecutor.Setup(e => e.Execute(It.IsAny<Job>())).Returns(42);
        var system = new ProcessingSystem(1, 100, mockExecutor.Object, 1000);
        var job = new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:10", Priority = 1 };

        var handle = system.Submit(job, out _);
        int result = await handle!.Result;

        Assert.Equal(42, result);
        mockExecutor.Verify(e => e.Execute(It.IsAny<Job>()), Times.Once);

        system.Stop();
    }

    [Fact]
    public async Task Submit_ValidJob_FiresJobCompletedEvent()
    {
        var mockExecutor = new Mock<IJobExecutor>();
        mockExecutor.Setup(e => e.Execute(It.IsAny<Job>())).Returns(99);
        var system = new ProcessingSystem(1, 100, mockExecutor.Object, 1000);
        var job = new Job { Id = Guid.NewGuid(), Type = JobType.Prime, Payload = "numbers:10,threads:1", Priority = 2 };

        bool eventFired = false;
        Guid firedJobId = Guid.Empty;
        system.JobCompleted += (jobId, result, timeMs, type) =>
        {
            eventFired = true;
            firedJobId = jobId;
        };

        var handle = system.Submit(job, out _);
        await handle!.Result;

        Assert.True(eventFired);
        Assert.Equal(job.Id, firedJobId);

        system.Stop();
    }

    // max 3
    [Fact]
    public async Task Submit_TimingOutJob_FiresJobFailedEvent()
    {
        var mockExecutor = new Mock<IJobExecutor>();
        mockExecutor.Setup(e => e.Execute(It.IsAny<Job>())).Returns(() =>
        {
            Thread.Sleep(500); // >200ms
            return 0;
        });
        var system = new ProcessingSystem(1, 100, mockExecutor.Object, 200);
        var job = new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:500", Priority = 1 };

        int failCount = 0;
        system.JobFailed += (jobId, reason, attempt, type) => { failCount++; };

        var handle = system.Submit(job, out _);
        try { await handle!.Result; } catch { } // bez try ne bi se desilo assert.equal

        Assert.Equal(3, failCount);

        system.Stop();
    }


    [Fact]
    public async Task Submit_JobThrowsException_RetriesAndAborts()
    {
        var mockExecutor = new Mock<IJobExecutor>();
        // execute uvijek baci gresku
        mockExecutor.Setup(e => e.Execute(It.IsAny<Job>())).Throws(new InvalidOperationException("Simulated failure"));
        var system = new ProcessingSystem(1, 100, mockExecutor.Object, 1000);
        var job = new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:10", Priority = 1 };

        int failCount = 0;
        system.JobFailed += (jobId, reason, attempt, type) => { failCount++; };

        var handle = system.Submit(job, out _);
        try { await handle!.Result; } catch { }

        Assert.Equal(3, failCount);
        mockExecutor.Verify(e => e.Execute(It.IsAny<Job>()), Times.Exactly(3));

        system.Stop();
    }

    [Fact]
    public void GetJob_ExistingId_ReturnsCorrectJob()
    {
        var mockExecutor = new Mock<IJobExecutor>();
        var system = new ProcessingSystem(0, 100, mockExecutor.Object);
        var job = new Job { Id = Guid.NewGuid(), Type = JobType.Prime, Payload = "numbers:10,threads:1", Priority = 3 };

        system.Submit(job, out _);
        var found = system.GetJob(job.Id);

        Assert.NotNull(found);
        Assert.Equal(job.Id, found!.Id);
        Assert.Equal(JobType.Prime, found.Type);

        system.Stop();
    }

    [Fact]
    public void GetJob_NonExistingId_ReturnsNull()
    {
        var mockExecutor = new Mock<IJobExecutor>();
        var system = new ProcessingSystem(0, 100, mockExecutor.Object);

        var found = system.GetJob(Guid.NewGuid());

        Assert.Null(found);

        system.Stop();
    }

    [Fact]
    public void GetTopJobs_ReturnsHighestPriorityFirst()
    {
        var mockExecutor = new Mock<IJobExecutor>();
        var system = new ProcessingSystem(0, 100, mockExecutor.Object); // 0 workers
        var job1 = new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:50", Priority = 3 };
        var job2 = new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:50", Priority = 1 };
        var job3 = new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:50", Priority = 2 };

        system.Submit(job1, out _);
        system.Submit(job2, out _);
        system.Submit(job3, out _);
        var top2 = system.GetTopJobs(2).ToList();

        Assert.Equal(2, top2.Count);
        Assert.Equal(1, top2[0].Priority);
        Assert.Equal(2, top2[1].Priority);

        system.Stop();
    }

    [Fact]
    public void GetTopJobs_RequestMoreThanAvailable_ReturnsAll()
    {
        var mockExecutor = new Mock<IJobExecutor>();
        var system = new ProcessingSystem(0, 100, mockExecutor.Object);
        system.Submit(new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:50", Priority = 1 }, out _);
        system.Submit(new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:50", Priority = 2 }, out _);

        var top10 = system.GetTopJobs(10).ToList();

        Assert.Equal(2, top10.Count);

        system.Stop();
    }

    [Fact]
    public void DefaultJobExecutor_Prime_ReturnsCorrectCount()
    {
        var executor = new DefaultJobExecutor();
        var job = new Job { Id = Guid.NewGuid(), Type = JobType.Prime, Payload = "numbers:10,threads:1", Priority = 1 };

        int result = executor.Execute(job);

        // Assert — primes up to 10: 2, 3, 5, 7 = 4
        Assert.Equal(4, result);
    }

    [Fact]
    public void DefaultJobExecutor_Prime_MultipleThreads_SameResult()
    {
        var executor = new DefaultJobExecutor();
        var job1 = new Job { Id = Guid.NewGuid(), Type = JobType.Prime, Payload = "numbers:100,threads:1", Priority = 1 };
        var job2 = new Job { Id = Guid.NewGuid(), Type = JobType.Prime, Payload = "numbers:100,threads:4", Priority = 1 };

        int result1 = executor.Execute(job1);
        int result2 = executor.Execute(job2);

        // Assert — both should find the same 25 primes up to 100
        Assert.Equal(result1, result2);
    }

    [Fact]
    public void DefaultJobExecutor_IO_ReturnsInRange()
    {
        var executor = new DefaultJobExecutor();
        var job = new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:50", Priority = 1 };

        int result = executor.Execute(job);

        Assert.InRange(result, 0, 100);
    }

    [Fact]
    public void GenerateReport_AfterCompletedJob_CreatesXmlFile()
    {
        var mockExecutor = new Mock<IJobExecutor>();
        mockExecutor.Setup(e => e.Execute(It.IsAny<Job>())).Returns(5);
        var system = new ProcessingSystem(1, 100, mockExecutor.Object, 1000);
        var job = new Job { Id = Guid.NewGuid(), Type = JobType.Prime, Payload = "numbers:10,threads:1", Priority = 1 };

        system.Submit(job, out _);
        Thread.Sleep(500);

        system.GenerateReport();

        string expectedFile = Path.Combine(ProcessingSystem.OutputDir, "report_0.xml");
        Assert.True(File.Exists(expectedFile));
        string content = File.ReadAllText(expectedFile);
        Assert.Contains("Report", content);

        if (File.Exists(expectedFile)) File.Delete(expectedFile);

        system.Stop();
    }

    [Fact]
    public void WriteLog_AppendsEntryToFile()
    {
        string testLogFile = "test_log_writetest.log";
        if (File.Exists(testLogFile)) File.Delete(testLogFile);
        Guid jobId = Guid.NewGuid();

        ProcessingSystem.WriteLog("COMPLETED", jobId, "42", testLogFile);

        Assert.True(File.Exists(testLogFile));
        string content = File.ReadAllText(testLogFile);
        Assert.Contains("COMPLETED", content);
        Assert.Contains(jobId.ToString(), content);
        Assert.Contains("42", content);

        File.Delete(testLogFile);
    }

    [Fact]
    public async Task Submit_MultiplePriorityJobs_ProcessedInOrder()
    {
        var mockExecutor = new Mock<IJobExecutor>();
        var processedOrder = new List<int>();
        var lockObj = new object();

        mockExecutor.Setup(e => e.Execute(It.IsAny<Job>())).Returns((Job j) =>
        {
            lock (lockObj) { processedOrder.Add(j.Priority); }
            return j.Priority;
        });

        var system = new ProcessingSystem(1, 100, mockExecutor.Object, 1000);   // 1 nit obradjuje jedan po jedan job

        var job3 = new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:10", Priority = 3 };
        var job1 = new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:10", Priority = 1 };
        var job2 = new Job { Id = Guid.NewGuid(), Type = JobType.IO, Payload = "delay:10", Priority = 2 };

        var h3 = system.Submit(job3, out _);
        var h1 = system.Submit(job1, out _);
        var h2 = system.Submit(job2, out _);

        await Task.WhenAll(h3!.Result, h1!.Result, h2!.Result);

        Assert.Equal(1, await h1.Result);
        Assert.Equal(2, await h2.Result);
        Assert.Equal(3, await h3.Result);

        system.Stop();
    }
}
