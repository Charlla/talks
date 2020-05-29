using System;
using System.Threading;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;

namespace consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello Again Kafkateers!");

            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                ClientId = "Consumer 1",
                GroupId = "Group 1",
                IsolationLevel = IsolationLevel.ReadCommitted,
                EnableAutoCommit = false,
                StatisticsIntervalMs = 10000
            };

            var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler(Handle_Error)
            .SetLogHandler(Handle_Log)
            .SetStatisticsHandler(Handle_Stats)
            .Build();

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            consumer.Subscribe("TESTS");

            try
            {
                var commitCounter = 0;
                while (!cts.Token.IsCancellationRequested)
                {
                    commitCounter += 1;
                    var consumeResult = consumer.Consume(cts.Token);
                    var message = JObject.Parse(consumeResult.Message.Value);
                    Console.WriteLine($"Message Consumed: TPO={consumeResult.Topic}-{consumeResult.Partition}-{consumeResult.Offset}, Value={message.ToString()}");
                    if (commitCounter >= 10)
                    {
                        consumer.Commit(consumeResult);
                        commitCounter = 0;
                    }

                }
            }
            catch (OperationCanceledException)
            {
                //Time to exit swiftly
            }
            finally
            {
                consumer.Commit();
                consumer.Close();
            }
        }

        private static void Handle_Stats(IConsumer<string, string> consumer, string stats)
        {
            Console.WriteLine("====================");
            Console.WriteLine($"STATS: {stats}");
        }

        private static void Handle_Log(IConsumer<string, string> consumer, LogMessage logMessage)
        {
            Console.WriteLine("====================");
            Console.WriteLine($"LOG: {logMessage.Message}");
        }

        private static void Handle_Error(IConsumer<string, string> consumer, Error error)
        {
            Console.WriteLine("====================");
            Console.WriteLine($"ERROR: {error.Reason}");
        }
    }
}
