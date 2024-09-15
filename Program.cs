using System;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
// using System.Threading.Tasks.Dataflow;

// public class CustomMinHeapOperation
// {
//     public static int getTop(int arr, int n)
//     {
//         int result = arr[0];
//         arr[0] = arr[n];
//         Heapify(arr, n-1, 0);
//         return result;
//     }
//     public static void Heapify(int[] arr, int n, int i)
//     {
//         int smallest = i;
//         int leftChild = 2*i + 1, rightChild = 2*i + 2;

//         if (leftChild < n && arr[leftChild] < arr[smallest])
//             smallest = leftChild;

//         if (rightChild < n && arr[rightChild] < arr[smallest])
//             smallest = rightChild;

//         if (smallest != i)
//         {
//             int temp = arr[i];
//             arr[i] = arr[smallest];
//             arr[smallest] = temp;

//             Heapify(arr, n, smallest);
//         }
//     }
// }

class Program
{
    static readonly int SORTER_BATCH_SIZE = 20;
    // static readonly int WRITE_FILE_SIGNAL = 1;

    static readonly Stopwatch timer = new Stopwatch();
    
    static readonly System.Threading.Lock cl = new System.Threading.Lock();
    static readonly System.Threading.Lock cl2 = new System.Threading.Lock();
    static readonly System.Threading.Lock cl3 = new System.Threading.Lock();

    static readonly System.Threading.Lock cl4 = new System.Threading.Lock();

    static readonly BlockingCollection<int> messageQueue = new BlockingCollection<int>();
    static readonly BlockingCollection<List<int>> messageQueue2 = new BlockingCollection<List<int>>();
    static readonly BlockingCollection<List<int>> messageQueue3 = new BlockingCollection<List<int>>();
    static readonly BlockingCollection<int> messageQueue4 = new BlockingCollection<int>();

    static readonly Semaphore _pool = new Semaphore(initialCount: 0, maximumCount: 4);

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

            // var primeFilter = Task.Run( () => taskFilterPrimeNumber());
            var first100Sorter = Task.Run( () => taskSortBatchNumber(SORTER_BATCH_SIZE));
            
            // var writerB = Task.Run( () => taskWriteLineToSortedFile() );
            // var writerA = Task.Run( () => taskWriteLineToPrimeFile() );

            // first100Sorter.Start();

            // numGenerator1.Start();
            // numGenerator2.Start();
            // numGenerator3.Start();

            // writerB.Wait();

            // numberGeneratorWorker.Start();
            // showMessageQueueInfoWorker.Start();
            Console.WriteLine("Main thread calls Release All");
            _pool.Release(releaseCount: 4);

            Task.WaitAll(
                numGenerator1,
                numGenerator2,
                numGenerator3,
                first100Sorter
                // primeFilter,
                // writerA,
                // writerB
            );

            // cts.Dispose();
        // }

        Console.WriteLine($"Execution Time: {timer.Elapsed.ToString()}");
        // timer.Stop();
    }

    private static void taskNumberGenerator(String name, Int32 minInt, Int32 maxInt) {
        // lock(lck) {
            for (int i=0; ; ++i) {
                Int32 randomInt = (new Random()).Next(minInt, maxInt);
                _pool.WaitOne();
                messageQueue.Add(randomInt);
                Console.WriteLine($"{name}: {randomInt} --> QueueCount:{messageQueue.Count}");
                _pool.Release();
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
            _pool.WaitOne();
            // if (!messageQueue.IsCompleted)
            // {
                // try
                // {
                    // lock(cl3) {
                    int nextInt;
                    try
                    {
                        if (messageQueue.TryTake(out nextInt))
                        {
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
                            sortBuffer[cursorIndex] = nextInt;
                            ++cursorIndex;
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
            _pool.Release();
        }
        //}
    }

    private static void taskWriteLineToSortedFile()
    {
        // lock(cl4) {
        var sbuilder = new StringBuilder();
        while (true)
        {
            lock(cl4) {
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
                                    Console.WriteLine(String.Join(Environment.NewLine, nextInt));
                                    Console.WriteLine("-----------------Write To Sortedddddddddddddddd Files------------------");
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
            }
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
            lock(cl4) {
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
                                if (num > 2 && num%2 == 1 && isPrimePreChecked(num)) {
                                    messageQueue4.Add(num);
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
            }
        }
    }

    private static void taskWriteLineToPrimeFile()
    {
        // lock(cl4) {
        var sbuilder = new StringBuilder();
        while (true)
        {
            lock(cl4) {
            if (!messageQueue4.IsCompleted)
            {
                // try
                // {
                    // lock(cl3) {
                    int nextInt;
                    try
                    {
                        if (messageQueue4.TryTake(out nextInt))
                        {
                            // foreach (int num in nextList)
                            // {
                                sbuilder.Append(nextInt).Append(Environment.NewLine);
                                
                            //     Console.WriteLine("++++++++++{0}", sbuilder.Length);
                                
                                if (sbuilder.Length > SORTER_BATCH_SIZE)
                                {
                                    // sbuilder.Append(nextInt).Append(Environment.NewLine);
                                    Console.WriteLine(String.Join(Environment.NewLine, nextInt));
                                    Console.WriteLine("-----------------Write To Primeeeeeeeeeeeeeeeeeeeeeeeeee Files------------------");
                                    sbuilder.Length = 0;
                                }
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
            }

        }
    }

    public static bool isPrimePreChecked(int number)
    {
        int sqrtNumber = (int)Math.Sqrt(number);
        for (int i=3; i<sqrtNumber; ++i)
        {
            if (number%i == 0)
            {
                return false;
            }
        }
        return true;
    }
}