using Confluent.Kafka;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace SuperDuper_Gameplay_UI.Classes
{
    public sealed class KafkaConsumer
    {
        private static readonly KafkaConsumer instance = new KafkaConsumer();

        static KafkaConsumer()
        {
        }

        private KafkaConsumer()
        {
        }

        public static KafkaConsumer Instance
        {
            get
            {
                return instance;
            }
        }

        public async void StartConsumer(CancellationTokenSource cts, FormMain formMain)
        {
            var consumerConfiguration = new ConsumerConfig
            {
                GroupId = "DashboardGroup",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<string, string>(consumerConfiguration)
                .SetErrorHandler(Handle_Error)
                .Build();
            List<string> topics = new List<string>();
            topics.Add("T_STATS_DEPOSITS");
            topics.Add("T_STATS_DEPOSITS_WIN_10S");
            topics.Add("T_STATS_WAGERS");
            topics.Add("T_STATS_WAGERS_WIN_10S");
            consumer.Subscribe(topics);

            await Task.Run(() =>
            {
                try
                {
                    var commitCounter = 0;
                    while (!cts.Token.IsCancellationRequested)
                    {
                        commitCounter += 1;
                        var consumeResult = consumer.Consume(cts.Token);
                        var message = JObject.Parse(consumeResult.Message.Value);

                        var stats = new Dictionary<string, string>();

                        if (consumeResult.Topic.Equals("T_STATS_DEPOSITS"))
                        {
                            stats.Add("LABEL_COUNT_DEPOSITS", message["COUNT_DEPOSITS"].ToString());
                            stats.Add("LABEL_SUM_DEPOSIT_AMOUNT", message["SUM_DEPOSIT_AMOUNT"].ToString());
                        }
                        else if (consumeResult.Topic.Equals("T_STATS_DEPOSITS_WIN_10S"))
                        {
                            stats.Add("LABEL_WIN_10S_COUNT_DEPOSITS", message["COUNT_DEPOSITS"].ToString());
                            stats.Add("LABEL_WIN_10S_SUM_DEPOSIT_AMOUNT", message["SUM_DEPOSIT_AMOUNT"].ToString());
                        }
                        else if (consumeResult.Topic.Equals("T_STATS_WAGERS"))
                        {
                            stats.Add("LABEL_COUNT_WAGERS", message["COUNT_WAGERS"].ToString());
                            stats.Add("LABEL_SUM_BET_AMOUNT", message["SUM_BET_AMOUNT"].ToString());
                            //stats.Add("LABEL_SUM_WIN_AMOUNT", message["SUM_WIN_AMOUNT"].ToString());
                            stats.Add("LABEL_AVG_BET_AMOUNT", message["AVG_BET_AMOUNT"].ToString());
                            stats.Add("LABEL_AVG_WIN_AMOUNT", message["AVG_WIN_AMOUNT"].ToString());
                            //stats.Add("LABEL_MIN_BET_AMOUNT", message["MIN_BET_AMOUNT"].ToString());
                            //stats.Add("LABEL_MIN_WIN_AMOUNT", message["MIN_WIN_AMOUNT"].ToString());
                            //stats.Add("LABEL_MAX_BET_AMOUNT", message["MAX_BET_AMOUNT"].ToString());
                            //stats.Add("LABEL_MAX_WIN_AMOUNT", message["MAX_WIN_AMOUNT"].ToString());
                            stats.Add("LABEL_MIN_START_BALANCE_AMOUNT", message["MIN_START_BALANCE_AMOUNT"].ToString());
                            //stats.Add("LABEL_MIN_END_BALANCE_AMOUNT", message["MIN_END_BALANCE_AMOUNT"].ToString());
                            //stats.Add("LABEL_MAX_START_BALANCE_AMOUNT", message["MAX_START_BALANCE_AMOUNT"].ToString());
                            stats.Add("LABEL_MAX_END_BALANCE_AMOUNT", message["MAX_END_BALANCE_AMOUNT"].ToString());
                        }
                        else if (consumeResult.Topic.Equals("T_STATS_WAGERS_WIN_10S"))
                        {
                            stats.Add("LABEL_WIN_10S_COUNT_WAGERS", message["COUNT_WAGERS"].ToString());
                            stats.Add("LABEL_WIN_10S_SUM_BET_AMOUNT", message["SUM_BET_AMOUNT"].ToString());
                            //stats.Add("LABEL_WIN_10S_SUM_WIN_AMOUNT", message["SUM_WIN_AMOUNT"].ToString());
                            stats.Add("LABEL_WIN_10S_AVG_BET_AMOUNT", message["AVG_BET_AMOUNT"].ToString());
                            stats.Add("LABEL_WIN_10S_AVG_WIN_AMOUNT", message["AVG_WIN_AMOUNT"].ToString());
                            //stats.Add("LABEL_WIN_10S_MIN_BET_AMOUNT", message["MIN_BET_AMOUNT"].ToString());
                            //stats.Add("LABEL_WIN_10S_MIN_WIN_AMOUNT", message["MIN_WIN_AMOUNT"].ToString());
                            //stats.Add("LABEL_WIN_10S_MAX_BET_AMOUNT", message["MAX_BET_AMOUNT"].ToString());
                            //stats.Add("LABEL_WIN_10S_MAX_WIN_AMOUNT", message["MAX_WIN_AMOUNT"].ToString());
                            stats.Add("LABEL_WIN_10S_MIN_START_BALANCE_AMOUNT", message["MIN_START_BALANCE_AMOUNT"].ToString());
                            //stats.Add("LABEL_WIN_10S_MIN_END_BALANCE_AMOUNT", message["MIN_END_BALANCE_AMOUNT"].ToString());
                            //stats.Add("LABEL_WIN_10S_MAX_START_BALANCE_AMOUNT", message["MAX_START_BALANCE_AMOUNT"].ToString());
                            stats.Add("LABEL_WIN_10S_MAX_END_BALANCE_AMOUNT", message["MAX_END_BALANCE_AMOUNT"].ToString());
                        }

                        formMain.UpdateLabels(stats);

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
                    consumer.Close();
                }
            });
        }

        private void Handle_Error(IConsumer<string, string> arg1, Error arg2)
        {
            throw new NotImplementedException();
        }
    }
}
