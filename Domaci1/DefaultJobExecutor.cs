namespace Domaci1;

public class DefaultJobExecutor : IJobExecutor
{
    private static readonly Random _random = new Random();
    // kako bi samo jedna nit u jednom trenutku mmogla koristiti random
    private static readonly object _randomLock = new object();

    // vrsta posla
    public int Execute(Job job)
    {
        if (job.Type == JobType.Prime)
            return CountPrimes(job.Payload);
        return SimulateIO(job.Payload);
    }

    private int CountPrimes(string payload)
    {
        string[] parts = payload.Split(',');
        int limit = int.Parse(parts[0].Split(':')[1].Replace("_", "")); // pretvoriti str u int - granica do kog broja
        int threadCount = Math.Clamp(int.Parse(parts[1].Split(':')[1].Replace("_", "")), 1, 8); // br niti od 1 do 8

        if (limit < 2)  // br manji od 2, nema prostih brojeva manjih od 2
            return 0;

        int count = 0;  // rezultati niti
        object countLock = new object();    // lock za rad count
        int chunkSize = Math.Max(1, (limit - 1) / threadCount); // koliko br svaka nit provjerava
        Thread[] threads = new Thread[threadCount];

        for (int t = 0; t < threadCount; t++)
        {
            int localT = t;
            int start = localT * chunkSize + 2; // od kog prostog br koja nit provjerava
            int end = (localT == threadCount - 1) ? limit : Math.Min(limit, (localT + 1) * chunkSize + 1);

            threads[localT] = new Thread(() =>
            {
                // lokalni brojilac za svaku nit
                int localCount = 0;
                for (int n = start; n <= end; n++)
                {
                    if (IsPrime(n))
                        localCount++;
                }

                // dodaje localC u count, lock da samo edna nit moze da mu pristupi
                lock (countLock)
                {
                    count += localCount;
                }
            });
            threads[localT].Start();    // pokrece nit
        }

        foreach (Thread t in threads)
            t.Join();   // join - cekaj da niti zavrse zbog rezultata

        return count;
    }

    private bool IsPrime(int n)
    {
        if (n < 2) return false;
        if (n == 2) return true;
        if (n % 2 == 0) return false;
        for (int i = 3; i * i <= n; i += 2)
        {
            if (n % i == 0) return false;
        }
        return true;
    }

    private int SimulateIO(string payload)
    {
        int delay = int.Parse(payload.Split(':')[1].Replace("_", ""));
        Thread.Sleep(delay);    // zamrznuti nit koliko se trazi
        lock (_randomLock)
        {
            return _random.Next(0, 101);
        }
    }
}
