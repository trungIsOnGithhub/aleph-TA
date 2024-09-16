using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;

/* Tested With .NET 6.0 */
/* I Considered: Consumer - Producer Problem with Multiple Producers At First
    Concurrency Control On Shared Resources */

class Program
{
    // Static Variable Allocated When Class Is Loaded, May Suitable To Avoid Memory Mistake Before Thread Running
    // Readonly - Ensure Behavior When Passing Resource Between Many Threads
    static readonly StreamWriter pw = new StreamWriter("primes.txt", true);
    static readonly StreamWriter sw = new StreamWriter("sorted.txt", true);
    // Stream Writer Is Good For Writing Continously Generate Data Stream(I think)

    static readonly int SORTER_BATCH_SIZE = 20;

    /* A Binary Semaphore somehow Equal to A Lock, But with Signaling Schema
        Make It Easier To Reason and Our Code Cleaner(I think) */
    // SemaphoreSlim is more Lightweight, Application-Separate than Normal(OS) Semaphore
    static readonly SemaphoreSlim sem0 = new SemaphoreSlim(0,1);
    static readonly SemaphoreSlim sem1 = new SemaphoreSlim(0,1);
    static readonly SemaphoreSlim sem2 = new SemaphoreSlim(0,1);
    static readonly SemaphoreSlim sem3 = new SemaphoreSlim(0,1);

    static readonly string[] generatorName = {"RNG1", "RNG2", "RNG3"};
    // Multiple Threads Reading from 1 Array Is Safe If Data Not Modified, and Array Is Filled After Thread Run

    // Thread-Safe version of Queue but Lock-Free, Need Concurrency Control
    static readonly ConcurrentQueue<int> messageQueue = new ConcurrentQueue<int>();
    static readonly ConcurrentQueue<int> messageQueue2 = new ConcurrentQueue<int>();
    static readonly ConcurrentQueue<int> messageQueue3 = new ConcurrentQueue<int>();
    static readonly ConcurrentQueue<int> messageQueue4 = new ConcurrentQueue<int>();

    // Different seed applied May Ensure There No Same Number Between 3 Generators in 1 Batch of 1000 number
    static readonly ConcurrentDictionary<int,int> generatorDict = new ConcurrentDictionary<int,int>();

    // Performace Calculator
    static readonly Stopwatch timer = new Stopwatch();

    static void Main(string[] args)
    {
        timer.Start();

        var nowTime = DateTime.Now;

        // Task.Run Consider The Ligtweight Version of StartNew()
        var numGenerator1 = Task.Run( () => taskNumberGenerator(0, nowTime.Hour) );
        var numGenerator2 = Task.Run( () => taskNumberGenerator(1, nowTime.Minute) );
        var numGenerator3 = Task.Run( () => taskNumberGenerator(2, nowTime.Second) );

        var primeFilter = Task.Run( () => taskFilterPrimeNumber()) ;
        var first1000Sorter = Task.Run( () => taskSortBatchNumber() );
        
        var writerB = Task.Run( () => taskWriteLineToSortedFile() );
        var writerA = Task.Run( () => taskWriteLineToPrimeFile() );
        // Async and Await May Good For I/O Bound Operation, Not CPU-Bound So
        // I Decided Not To Use ITiti

        // Start Allow 1 Thread to enter Semaphore
        // which was previously 0 mean no thread is able to enter
        sem0.Release(1);
        sem1.Release(1);
        sem2.Release(1);
        sem3.Release(1);


            Task.WaitAll(
                numGenerator1,
                numGenerator2,
                numGenerator3,
                first1000Sorter,
                primeFilter,
                writerA,
                writerB
            );

            timer.Stop();

            sem0.Dispose();
            sem1.Dispose();
            sem2.Dispose();
            sem3.Dispose();

            sw.Close();
            pw.Close();
    }

    // Static Method Do Not Rely On Class Object State
    private static void taskNumberGenerator(int generatorIndex, int seed) {
        var randomizer = new Random(seed);

        for (int i=0; i<100; ++i) {
            int randomInt = randomizer.Next();

            // using SemaphoreSlim must ensure wait time is short
            sem0.Wait();
            messageQueue.Enqueue(randomInt);
            sem0.Release();

            if (!generatorDict.ContainsKey(randomInt))
            {
                generatorDict.TryAdd(randomInt, generatorIndex);
            }

            Console.WriteLine($"Index-{generatorIndex} gen: {randomInt}");
            // sem0.Release();
        }
    }

    private static void taskSortBatchNumber() {
        int cursor1Idx = 0;
        int cursor2Idx = SORTER_BATCH_SIZE - 1;
        int[] sortBuffer = new int[SORTER_BATCH_SIZE]; // allocate one time and for the whole program
        // I chose Array as I thought It should be more Efficent compare to List

        /* The Batch Size Here is Even Number, So I Take And Fill From Queue
            2 Numbers At A Time to Make the Consumer Speedy */
        int nextInt = 0;
        int nextNextInt = 0;
        while (true)
        {
            var takeOutSuccess = false;
            sem0.Wait();
            if (messageQueue.Count > 1) {
                takeOutSuccess = messageQueue.TryDequeue(out nextInt);
                takeOutSuccess = messageQueue.TryDequeue(out nextNextInt);
                Console.WriteLine("Taked ((++++++++++++++++++++{0}---------{1}", nextInt, nextNextInt);
                Console.WriteLine($"Queue Count: {messageQueue.Count}");
            }
            sem0.Release();

            if (takeOutSuccess)
            {
                if (nextInt < nextNextInt)
                {
                    sortBuffer[cursor1Idx] = nextInt;
                    sortBuffer[cursor2Idx] = nextNextInt;
                }
                else
                {
                    sortBuffer[cursor1Idx] = nextNextInt;
                    sortBuffer[cursor2Idx] = nextInt; 
                }
                cursor1Idx++;
                cursor2Idx--;
                /* I Placed The Smaller Number To First Half, Larger Number To Later Half
                    to Help The Inner Algorithm of Array.Sort(): IntroSort become Slightly Faster */

                if (cursor1Idx >= cursor2Idx) {
                    // A Heap-Based or Balanced-Tree Based Data Structure, I considered it to be Inefficent in terms of Memory tradeoff
                    Array.Sort(sortBuffer);
                    cursor1Idx = 0;
                    cursor2Idx = SORTER_BATCH_SIZE - 1;

                    foreach (int num in sortBuffer)
                    {
                        sem1.Wait();
                        messageQueue2.Enqueue(num);
                        sem1.Release();
                        
                        sem2.Wait();
                        messageQueue3.Enqueue(num);
                        sem2.Release();
                    }
                }
            }
        }
        //}
    }

    private static void taskWriteLineToSortedFile()
    {
        // Use String Builder For Appending Efficency
        var sbuilder = new StringBuilder();
        int nextInt = 0;
        bool takeOutSuccess = false;
        int countWritten = 0;

        while (true)
        {
            try
            {
                takeOutSuccess = false;
                sem1.Wait();
                if (messageQueue2.Count > 0) {
                    takeOutSuccess = messageQueue2.TryDequeue(out nextInt);
                    // Console.WriteLine("Queue 2 Taked ((++++++++++++++++++++{0}", nextInt);
                    // Console.WriteLine($"Queue 2 Count: {messageQueue2.Count}");
                }
                sem1.Release();

                if (takeOutSuccess)
                {
                    int generatorIndex = -1;
                    if (generatorDict.TryGetValue(nextInt, out generatorIndex))
                    {
                        sbuilder.Length = 0;

                        sbuilder.Append(nextInt)
                            .Append(',')
                            .Append(generatorName[generatorIndex]);

                        sw.WriteLine(sbuilder.ToString());
                        sw.Flush();

                        Console.WriteLine("00000000000000000000000000 {0}", nextInt);
                        ++countWritten;
                    }

                    // Measured Memory Used(Virtual and Mapped) After a Pipeline Done, The Same Between Threads in Process
                    using(Process currentProc = Process.GetCurrentProcess())
                    {
                        Console.WriteLine("Physical Memory Used: {0} bytes", currentProc.WorkingSet64);
                    }

                    Console.WriteLine("Speed Write To sorted.txt: ~{0}", countWritten/timer.Elapsed.TotalSeconds);
                }
                
            }
            catch (Exception)
            {
                Console.WriteLine("Error Occured Taking From Queue Write Sorted File Task!!");
                break;
            }
        }
    }

    private static void taskFilterPrimeNumber()
    {
        // lock(cl4) {
        // var sbuilder = new StringBuilder();
        int nextInt = 0;
        while (true)
        {
            try
            {
                var takeOutSuccess = false;
                sem2.Wait();
                if (messageQueue3.Count > 0) {
                    takeOutSuccess = messageQueue3.TryDequeue(out nextInt);
                    // Console.WriteLine("Taked ((++++++++++++++++++++{0}", nextInt);
                    // Console.WriteLine($"Queue 3 Count: {messageQueue3.Count}");
                }
                sem2.Release();

                if (takeOutSuccess)
                {
                    if (isPrimePreChecked(nextInt)) {
                        sem3.Wait();
                        messageQueue4.Enqueue(nextInt);
                        sem3.Release();
                        Console.WriteLine("888888888888888888: {0}", nextInt);
                    }
                }
            }
            catch (Exception)
            {
                Console.WriteLine("Error Occured Taking From Queue Prime Filter Task!!");
                break;
            }
        }
    }

    private static void taskWriteLineToPrimeFile()
    {
        var sbuilder = new StringBuilder();
        int nextInt = 0;
        int countWritten = 0;

        while (true)
        {
            try
            {
                var takeOutSuccess = false;
                sem3.Wait();

                if (messageQueue4.Count > 0) {
                    takeOutSuccess = messageQueue4.TryDequeue(out nextInt);
                    Console.WriteLine("Queue 4 Taked ((++++++++++++++++++++{0}", nextInt);
                    // Console.WriteLine($"Queue 4 Count: {messageQueue4.Count}");
                }
                sem3.Release();

                if (takeOutSuccess)
                {                            
                    int generatorIndex = -1;
                    if (generatorDict.TryGetValue(nextInt, out generatorIndex))
                    {
                        sbuilder.Length = 0;

                        sbuilder.Append(nextInt)
                            .Append(',')
                            .Append(generatorName[generatorIndex]);

                        pw.WriteLine(sbuilder.ToString());
                        pw.Flush();

                        Console.WriteLine("JJJJJJJJJJJJJJJJJJJJ {0}", nextInt);
                        ++countWritten;
                    }

                    // Measured Memory Used(Virtual and Mapped) After a Pipeline Done, The Same Between Threads in Process
                    using(Process currentProc = Process.GetCurrentProcess())
                    {
                        Console.WriteLine("11111Physical Memory Used: {0} bytes", currentProc.WorkingSet64);
                    }

                    Console.WriteLine("Speed Write To primes.txt: ~{0}", countWritten/timer.Elapsed.TotalSeconds);
                }
            }
            catch (Exception)
            {
                Console.WriteLine("Error Occured Taking From Queue Write Prime Task!!");
                break;
            }
        }
    }

    // Consider Create A Cache For Prime Checker
    public static bool isPrimePreChecked(int number)
    {
        // no assign to intermmediate variable for speed
        if (number < 2) return false;
        if (number % 2 == 0) return (number == 2);

        // I used square root to void i*i multiply overflow
        for (int i=3; i <= (int)Math.Sqrt((double)number); i+=2)
        {
            if (number%i == 0) return false;
        }
        return true;
    }
}

// https://learn.microsoft.com/en-us/dotnet/api/system.threading.semaphoreslim?view=net-8.0
// https://stackoverflow.com/questions/62814/difference-between-binary-semaphore-and-mutex
// https://stackoverflow.com/questions/10010748/what-are-the-differences-between-concurrentqueue-and-ConcurrentQueue-in-net
// https://jeremyshanks.com/fastest-way-to-write-text-files-to-disk-in-c/
// https://stackoverflow.com/questions/6664538/is-stopwatch-elapsedticks-threadsafe