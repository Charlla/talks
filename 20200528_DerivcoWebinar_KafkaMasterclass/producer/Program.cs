using System;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;

namespace producer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            // Config 
            var config = new ProducerConfig(){
                BootstrapServers = "localhost:9092",
                ClientId = "Producer 1",
                Acks = Acks.All,
                Partitioner = Partitioner.Murmur2Random,
                CompressionType = CompressionType.Lz4,
                LingerMs = 50,
                BatchNumMessages = 100,
                EnableIdempotence = true,
                StatisticsIntervalMs = 10000

            };

            // Producer
            var producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler(HandleError)
            .SetLogHandler(HandleLog)
            .SetStatisticsHandler(HandleStats)
            .Build();

            // Send message
            for (int i = 0; i < 100; i++)
            {
                var messageObject = new {intVal = i, stringVal = i.ToString()};

                var message = new Message<string, string>();
                message.Key = $"Key-{i.ToString()}";
                message.Value = JObject.FromObject(messageObject).ToString();

                producer.Produce("TESTS", message, dr => {
                    if(!dr.Error.IsError){
                        Console.WriteLine($"P[{dr.Partition}]O[{dr.Offset}]");
                    }
                });
            }

            //Flush
	    producer.Flush(TimeSpan.FromSeconds(10));

            while (true){
	        System.Threading.Thread.Sleep(100);
            }
        }

        private static void HandleStats(IProducer<string, string> arg1, string stats)
        {
            Console.WriteLine(stats);
        }

        private static void HandleLog(IProducer<string, string> arg1, LogMessage arg2)
        {
            throw new NotImplementedException();
        }

        private static void HandleError(IProducer<string, string> arg1, Error arg2)
        {
            throw new NotImplementedException();
        }
    }
}
