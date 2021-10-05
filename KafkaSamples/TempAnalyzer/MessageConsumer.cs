using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
// ReSharper disable TemplateIsNotCompileTimeConstantProblem

namespace TempAnalyzer
{
    public class MessageConsumer
    {
        private readonly ILogger<MessageConsumer> _log;
        private readonly ConsumerConfig _consumerConfig;

        public MessageConsumer(ILogger<MessageConsumer> log, ConsumerConfig consumerConfig)
        {
            _log = log;
            _consumerConfig = consumerConfig;
        }
        public  async Task StartListeners(CancellationToken stoppingToken)
        {
            await DoManyConsumers(stoppingToken);
        }
        private async  Task DoManyConsumers(CancellationToken stoppingToken)
        {
            var consumerBuilder = new ConsumerBuilder<string, int>(_consumerConfig);
            
            async Task ConsumeQwert(CancellationToken cancellationToken, int i1)
            {
                var consumer = consumerBuilder.Build();
                consumer.Subscribe("WeatherTopic" + i1);
                
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = await DoConsume(consumer, cancellationToken);
                
                    if (consumeResult.Message != null)
                    {
                        _log.LogInformation(consumer.Assignment[0].Topic + " - " + consumeResult.Topic);
                    }
                }
            }

            var tasks = new List<Task>();
            
            for (var i = 0; i < 50; i++)
            {
                tasks.Add(ConsumeQwert(stoppingToken, i));
            }
            
            await Task.WhenAll(tasks);
        }

        private async Task<ConsumeResult<string, int>> DoConsume(IConsumer<string, int> consumer,
            CancellationToken stoppingToken)
        {
            while (true)
            {
                stoppingToken.ThrowIfCancellationRequested();
                var consumeResult = consumer.Consume(0);
                if (consumeResult != null) return consumeResult;
                await Task.Yield();
            }
        }
    }
}