﻿using System;
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
    // static readonly string primeFilePath = ;
    static readonly StreamWriter sw = new StreamWriter("primes.txt", true);
    static readonly string sortedFilePath = "sorted.txt";

    static readonly int SORTER_BATCH_SIZE = 1000;
    // static readonly int WRITE_FILE_SIGNAL = 1;

    static readonly Stopwatch timer = new Stopwatch();
    
    // static readonly System.Threading.Lock cl = new System.Threading.Lock();
    // static readonly System.Threading.Lock cl2 = new System.Threading.Lock();
    // static readonly System.Threading.Lock cl3 = new System.Threading.Lock();

    // static readonly System.Threading.Lock cl4 = new System.Threading.Lock();

    static readonly ConcurrentQueue<int> messageQueue = new ConcurrentQueue<int>();
    static readonly BlockingCollection<List<int>> messageQueue2 = new BlockingCollection<List<int>>();
    static readonly BlockingCollection<List<int>> messageQueue3 = new BlockingCollection<List<int>>();
    static readonly BlockingCollection<int> messageQueue4 = new BlockingCollection<int>();

    static readonly Semaphore _pool = new Semaphore(0,1);
    // static readonly Semaphore _pool2 = new Semaphore(0,1);

    // static readonly Queue<int> queueSorter100ToWriterB = new Queue<int>();
    
    // static readonly BufferBlock<int> messageQueue = new BufferBlock<int>();

    static void Main(string[] args)
    {
        // timer.Start();

        // for (Int32 i=0; i<10; ++i) {
            // var cts = new CancellationTokenSource();

            var numGenerator1 = Task.Run( () => taskNumberGenerator("RNG1", 1, 1000));
            var numGenerator2 = Task.Run( () => taskNumberGenerator("RNG2", 1001, 2000));
            var numGenerator3 = Task.Run( () => taskNumberGenerator("RNG3", 2001, 3000));

            var primeFilter = Task.Run( () => taskFilterPrimeNumber());
            var first100Sorter = Task.Run( () => taskSortBatchNumber(SORTER_BATCH_SIZE));
            
            var writerB = Task.Run( () => taskWriteLineToSortedFile() );
            var writerA = Task.Run( () => taskWriteLineToPrimeFile() );

            // numberGeneratorWorker.Start();
            // showMessageQueueInfoWorker.Start();
            Console.WriteLine("Main thread calls Release Semaphore");
            _pool.Release(1);

            Task.WaitAll(
                numGenerator1,
                numGenerator2,
                numGenerator3,
                first100Sorter,
                primeFilter,
                writerA,
                writerB
            );

            _pool.Dispose();
            sw.Dispose();
            // _pool2.Dispose();
        // }

        Console.WriteLine($"Execution Time: {timer.Elapsed.ToString()}");
        // timer.Stop();
    }

    private static void taskNumberGenerator(String name, Int32 minInt, Int32 maxInt) {
        // lock(lck) {
            for (int i=0; ; ++i) {
                Int32 randomInt = (new Random()).Next(minInt, maxInt);

                _pool.WaitOne();
                messageQueue.Enqueue(randomInt);
                _pool.Release();

                Console.WriteLine($"{name}: {randomInt}");
                // _pool.Release();
            }
        // }
        // Thread.Sleep(5000);
        // messageQueue.CompleteAdding();
    }

    private static void taskSortBatchNumber(int SORTER_BATCH_SIZE) {
        int cursorIndex = 0;
        int[] sortBuffer = new int[SORTER_BATCH_SIZE]; // only allocate one time and for the whole program
        // lock(cl4) {
        while (true)
        {
            // if (!messageQueue.IsCompleted)
            // {
                // try
                // {
                    // lock(cl3) {
                    int nextInt = 0;
                    // try
                    // {
                        var takeOutSuccess = false;
                        _pool.WaitOne();
                        // takeOutSuccess = messageQueue.Count > 0;
                        if (messageQueue.Count > 0) {
                            takeOutSuccess = messageQueue.TryDequeue(out nextInt);

                            Console.WriteLine("Taked +++++++++++++++++{0}", nextInt);
                        }
                        Console.WriteLine($"QueueCount: {messageQueue.Count}");
                        _pool.Release();

                        if (takeOutSuccess)
                        {
                            sortBuffer[cursorIndex++] = nextInt;

                            if (cursorIndex == SORTER_BATCH_SIZE) {
                                Array.Sort(sortBuffer);
                                cursorIndex = 0;
                                    // lock(cl4) {
                                    // foreach (int num in sortBuffer) {
                                        // Console.Write($"{num},");
                                            messageQueue2.Add(new List<int>(sortBuffer));
                                            messageQueue3.Add(new List<int>(sortBuffer));
                                            // Console.WriteLine("-----{0}",messageQueue2.Count);
                                        // }
                                    // }}
                                // }
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
        while (true)
        {
            // lock(cl4) {
            if (!messageQueue2.IsCompleted)
            {
                // try
                // {
                    // lock(cl3) {
                    List<int> nextInt;
                    try
                    {
                        if (messageQueue2.TryTake(out nextInt))
                        {
                            // foreach (int num in nextList)
                            // {
                            //     sbuilder.Append(num).Append(Environment.NewLine);
                                
                            //     Console.WriteLine("++++++++++{0}", sbuilder.Length);
                                
                                // if (sbuilder.Length > SORTER_BATCH_SIZE)
                                // {
                                    foreach (int num in nextInt)
                                    {
                                        sbuilder.Append(num)
                                                .Append(Environment.NewLine);
                                    }
                                    sbuilder.Append(Environment.NewLine).Append(Environment.NewLine);
        
                                    File.AppendAllText(
                                        sortedFilePath,
                                        sbuilder.ToString()
                                    );
                                    // Console.WriteLine("-----------------Write To Sortedddddddddddddddd Files------------------");
                                    sbuilder.Length = 0;
                                // }
                            // }
                        }
                    }
                    catch (OperationCanceledException)
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
            // var sbuilder = new StringBuilder();
            // lock(cl4) {
            // while (true)
            // {
            //     // lock(cl4) {
            //     if (queueSorter100ToWriterB.Count > 0)
            //     {
            //         // if (cursorIndex == SORTER_BATCH_SIZE) {
            //         //     Array.Sort(sortBuffer);
            //         //     foreach (int num of sortBuffer) {
            //         //         Console.Write($"{num},");
            //         //     }
            //         //     cursorIndex = 0;
            //         // }
            //         // sortBuffer[cursorIndex] = messageQueue.Dequeue();
            //         // ++cursorIndex;
                    
            //         // form a StringBuilder before write to file to avoid access file to many times
            //         sbuilder.Append(queueSorter100ToWriterB.Dequeue()).Append(Environment.NewLine);
                    
            //         Console.WriteLine("++++++++++{0}", sbuilder.Length);
                    
            //         if (sbuilder.Length > SORTER_BATCH_SIZE)
            //         {
            //             Console.WriteLine(sbuilder.ToString());
            //             Console.WriteLine("-----------------Write To Files------------------");
            //             sbuilder.Length = 0;
            //         }
            //     }
            // }
            // }
        // }
    }

    private static void taskFilterPrimeNumber()
    {
        // lock(cl4) {
        // var sbuilder = new StringBuilder();
        while (true)
        {
            // lock(cl4) {
            if (!messageQueue3.IsCompleted)
            {
                // try
                // {
                    // lock(cl3) {
                    List<int> nextIntList;
                    try
                    {
                        if (messageQueue3.TryTake(out nextIntList))
                        {
                            foreach (int num in nextIntList)
                            {
                                if (isPrimePreChecked(num)) {
                                    messageQueue4.Add(num);
                                    Console.WriteLine("888888888888888888: {0}", num);
                                }
                            }
                            // Console.WriteLine(String.Join(Environment.NewLine, nextInt));
                            // Console.WriteLine("-----------------Write To Prime Files------------------");
                            // sbuilder.Length = 0;
                        }
                    }
                    catch (OperationCanceledException)
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
        while (true)
        {
            // lock(cl4) {
            // Console.WriteLine("111111111111111 {0}");
            if (!messageQueue4.IsCompleted)
            {
                // try
                // {
                    // lock(cl3) {
                    int nextInt = 0;
                    // Console.WriteLine("22222222222 {0}", nextInt);
                    try
                    {
                        if (messageQueue4.TryTake(out nextInt))
                        {
                            // foreach (int num in nextList)
                            // {
                                // append char or int each time give more atomicity than whole string
                            Console.WriteLine("00000000000000000000 {0}", nextInt);
                            sw.WriteLine(nextInt);
                            //     sbuilder.Append(nextInt)
                            //             .Append(Environment.NewLine);
                                
                            // //     Console.WriteLine("++++++++++{0}", sbuilder.Length);
                                
                            //     if (sbuilder.Length > SORTER_BATCH_SIZE)
                            //     {
                            //         sbuilder.Append(nextInt)
                            //             .Append(Environment.NewLine)
                            //             .Append(Environment.NewLine);

                            //         // Console.WriteLine(String.Join(Environment.NewLine, nextInt));using System;
                            //         File.AppendAllText(primeFilePath, sbuilder.ToString());
                            //         Console.WriteLine("-----------------Write To Primeeeeeeeeeeeeeeeeeeeeeeeeee Files------------------");
                            //         sbuilder.Length = 0;
                            //     }
                            // }
                        }
                    }
                    catch (OperationCanceledException)
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