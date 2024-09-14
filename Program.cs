namespace AlephTA_2024;

using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Concurrent;

class Program
{
    static readonly Stopwatch timer = new Stopwatch();
    static readonly ConcurrentQueue<int> messageQueue = new ConcurrentQueue<int>();

    static void Main(string[] args)
    {
        timer.Start();

        for (Int32 i=0; i<100; ++i) {
            var numberGeneratorWorker = TaskNumberGenerator("RNG1", 1, 1000);
            var showMessageQueueInfoWorker = ShowMessageQueueInfo();

            // numberGeneratorWorker.Start();
            // showMessageQueueInfoWorker.Start();
            Task.WaitAll(new[] {numberGeneratorWorker, showMessageQueueInfoWorker});
        }
        // TaskNumberGenerator("RNG2", 1001, 2000);
        // TaskNumberGenerator("RNG3", 2001, 3000);

        Console.WriteLine("Execution Time: {0:hh\\:mm\\:ss}",timer.Elapsed);
        timer.Stop();
    }

    private static async Task TaskNumberGenerator(String name, Int32 minInt, Int32 maxInt) {
        Int32 randomInt = (new Random()).Next(minInt, maxInt);
        Console.WriteLine($"{name} generated: {randomInt}");

        messageQueue.Enqueue(randomInt);
    }

    private static async Task ShowMessageQueueInfo() {
        var randomInt = (new Random()).Next(minInt, maxInt);
        Console.WriteLine($"Current Queue Length: {messageQueue.Count}");
    }
}
