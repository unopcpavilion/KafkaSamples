using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TempAnalyzer
{
    internal class MessageConsumer: BackgroundService
    {
        private readonly ILogger<MessageConsumer> _log;
        private readonly ConsumerConfig _consumerConfig;

        public MessageConsumer(ILogger<MessageConsumer> log, ConsumerConfig consumerConfig)
        {
            _log = log;
            _consumerConfig = consumerConfig;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            //await DoOneConsumer(stoppingToken); //~20 threads

            await DoManyConsumers(stoppingToken); //~50 threads
        }

        private  Task DoManyConsumers(CancellationToken stoppingToken)
        {
            var tasks = new List<Task>();


            for (var i = 0; i < 5; i++)
            {
                var i1 = i;
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    var consumer = new ConsumerBuilder<string, int>(_consumerConfig).Build();
                    
                    consumer.Subscribe("WeatherTopic"+i1);

                    while (!stoppingToken.IsCancellationRequested)
                    {
                        var consumeResult = consumer.Consume(stoppingToken);

                        if (consumeResult.Message != null)
                            _log.LogInformation(consumer.Assignment[0].Topic + " - " + consumeResult.Topic);

                    }
                }, stoppingToken));
            }
            
            return  Task.WhenAll(tasks);
        }


        private async Task DoOneConsumer(CancellationToken stoppingToken)
        {
            await Task.Yield();
            
            var consumer = new ConsumerBuilder<string, int>(_consumerConfig).Build();
            var topics = new string[5];
            for (var i = 0; i < 5; i++)
            {
                topics[i] = "WeatherTopic" + i;
            }
            
            consumer.Subscribe(topics);

            while (!stoppingToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(stoppingToken);

                if (consumeResult.Message != null)
                    _log.LogInformation(consumer.Assignment[0].Partition + " - " + consumeResult.Topic);

            }
        }
    }
}