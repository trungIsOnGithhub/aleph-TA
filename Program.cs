using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;

// https://learn.microsoft.com/en-us/dotnet/api/system.threading.semaphoreslim?view=net-8.0
// https://stackoverflow.com/questions/62814/difference-between-binary-semaphore-and-mutex
// https://stackoverflow.com/questions/10010748/what-are-the-differences-between-concurrentqueue-and-blockingcollection-in-net
// https://jeremyshanks.com/fastest-way-to-write-text-files-to-disk-in-c/

class Program
{
    // Readonly - Ensure Behavior When Passing Resource Between Many Threads
    static readonly StreamWriter pw = new StreamWriter("primes.txt", true);
    static readonly StreamWriter sw = new StreamWriter("sorted.txt", true);

    static readonly int SORTER_BATCH_SIZE = 21;

    static readonly SemaphoreSlim _pool = new SemaphoreSlim(0,1);

    static readonly string[] generatorName = {"RNG1", "RNG2", "RNG3"};
    // static readonly int WRITE_FILE_SIGNAL = 1;

    // static readonly Stopwatch timer = new Stopwatch();
    
    // static readonly System.Threading.Lock cl = new System.Threading.Lock();
    // static readonly System.Threading.Lock cl2 = new System.Threading.Lock();
    // static readonly System.Threading.Lock cl3 = new System.Threading.Lock();

    // static readonly System.Threading.Lock cl4 = new System.Threading.Lock();

    // Thread-Safe version of Queue but Lock-Free, Need Concurrency Control
    static readonly ConcurrentQueue<int> messageQueue = new ConcurrentQueue<int>();

    static readonly BlockingCollection<int> messageQueue2 = new BlockingCollection<int>();
    static readonly BlockingCollection<int> messageQueue3 = new BlockingCollection<int>();
    static readonly BlockingCollection<int> messageQueue4 = new BlockingCollection<int>();

    static readonly ConcurrentDictionary<int,int> generatorDict = new ConcurrentDictionary<int,int>();

    static void Main(string[] args)
    {
        // timer.Start();
        var nowTime = DateTime.Now;

        var numGenerator1 = Task.Run( () => taskNumberGenerator(0, nowTime.Hour) );
        var numGenerator2 = Task.Run( () => taskNumberGenerator(1, nowTime.Minute) );
        var numGenerator3 = Task.Run( () => taskNumberGenerator(2, nowTime.Second) );

        var primeFilter = Task.Run( () => taskFilterPrimeNumber()) ;
        var first1000Sorter = Task.Run( () => taskSortBatchNumber() );
        
        var writerB = Task.Run( () => taskWriteLineToSortedFile() );
        var writerA = Task.Run( () => taskWriteLineToPrimeFile() );

        // Start Allow 1 Thread to enter Semaphore
        // which was previously 0 mean no thread is able to enter
        _pool.Release(1);
        Task.WaitAll(
            numGenerator1,
            numGenerator2,
            numGenerator3,
            first1000Sorter,
            primeFilter,
            writerA,
            writerB
        );

        _pool.Dispose();
        sw.Dispose();
        pw.Dispose();

        // Console.WriteLine($"Execution Time: {timer.Elapsed.ToString()}");
        // timer.Stop();
    }

    // private static void decideNumberGenerator
    private static void taskNumberGenerator(int generatorIndex, int seed) {
        var randomizer = new Random(seed);

        for (int i=0; i<7; ++i) {
            int randomInt = randomizer.Next(1, 10000);

            // using SemaphoreSlim must ensure wait time is short
            _pool.Wait();
            messageQueue.Enqueue(randomInt);
            _pool.Release();

            if (!generatorDict.ContainsKey(randomInt))
            {
                generatorDict.TryAdd(randomInt, generatorIndex);
            }

            Console.WriteLine($"Index-{generatorIndex} gen: {randomInt}");
            // _pool.Release();
        }
    }

    private static void taskSortBatchNumber() {
        int cursorIndex = 0;
        int[] sortBuffer = new int[SORTER_BATCH_SIZE]; // allocate one time and for the whole program
        // I chose Array as I thought It should be more Efficent compare to List

        int nextInt = 0;
        while (true)
        {
            // if (!messageQueue.IsCompleted)
            // {
                // try
                // {
                    // lock(cl3) {
                    // try
                    // {
                        var takeOutSuccess = false;
                        _pool.Wait();
                        // takeOutSuccess = messageQueue.Count > 0;
                        if (messageQueue.Count > 0) {
                            takeOutSuccess = messageQueue.TryDequeue(out nextInt);
                            Console.WriteLine("Taked ((++++++++++++++++++++{0}", nextInt);
                            Console.WriteLine($"QueueCount: {messageQueue.Count}");
                        }
                        _pool.Release();

                        if (takeOutSuccess)
                        {
                            sortBuffer[cursorIndex++] = nextInt;

                            if (cursorIndex == SORTER_BATCH_SIZE) {
                                // A Heap-Based or Balanced-Tree Based Data Structure, I considered it to be Inefficent in terms of Memory tradeoff

                                Array.Sort(sortBuffer);
                                cursorIndex = 0;
  
                                foreach (int num in sortBuffer)
                                {
                                    messageQueue2.Add(num);
                                    messageQueue3.Add(num);
                                }
                            }
                        }
                    // }
                    // catch (OperationCanceledException)
                    // {
                    //     // Console.WriteLine("Taking canceled.");
                    //     // break;
                    // }
                    // }
                // }
                // catch (InvalidOperationException) {
                //     Console.WriteLine("Error Adding To Message Queue!");
                // }
            // }
            // if (messageQueue.Count > 0)
            // {
            //     if (cursorIndex == SORTER_BATCH_SIZE) {
            //         Array.Sort(sortBuffer);
            //         foreach (int num in sortBuffer) {
            //             Console.Write($"{num},");
            //         }
            //         cursorIndex = 0;
            //         // queueSorter100ToWriterB.Enqueue(WRITE_FILE_SIGNAL);
            //         // Console.WriteLine(sortBuffer.Join(","));
            //     }
            //     messageQueue.TryTake(out sortBuffer[cursorIndex]);
            //     ++cursorIndex;
            // }
        }
        //}
    }

    private static void taskWriteLineToSortedFile()
    {
        // lock(cl4) {
        var sbuilder = new StringBuilder();
        int nextInt = 0;
        while (true)
        {
            // lock(cl4) {
            if (!messageQueue2.IsCompleted)
            {
                // try
                // {
                    // lock(cl3) {
                    try
                    {
                        if (messageQueue2.TryTake(out nextInt))
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
                            }
                        }
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("Taking canceled.");
                        break;
                    }

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
            // lock(cl4) {
            if (!messageQueue3.IsCompleted)
            {
                // try
                // {
                    // lock(cl3) {
                    try
                    {
                        if (messageQueue3.TryTake(out nextInt))
                        {
                            // foreach (int num in nextIntList)
                            // {
                                if (isPrimePreChecked(nextInt)) {
                                    messageQueue4.Add(nextInt);
                                    Console.WriteLine("888888888888888888: {0}", nextInt);
                                }
                            // }
                            // Console.WriteLine(String.Join(Environment.NewLine, nextInt));
                            // Console.WriteLine("-----------------Write To Prime Files------------------");
                            // sbuilder.Length = 0;
                        }
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("Taking canceled.");
                        break;
                    }
                    // }
                // }
                // catch (InvalidOperationException) {
                //     Console.WriteLine("Error Adding To Message Queue!");
                // }
            }
            // }
        }
    }

    private static void taskWriteLineToPrimeFile()
    {
        // lock(cl4) {
        var sbuilder = new StringBuilder();
        int nextInt = 0;
        while (true)
        {
            // Console.WriteLine("111111111111111 {0}");
            if (!messageQueue4.IsCompleted)
            {
                // try
                // {
                    // lock(cl3) {
                    // Console.WriteLine("22222222222 {0}", nextInt);
                    try
                    {
                        if (messageQueue4.TryTake(out nextInt))
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
                            }
                        }
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("Taking canceled");
                        break;
                    }
            }
            // }

        }
    }

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