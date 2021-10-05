using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TempAnalyzer
{
    public class ServiceBusHostedService : IHostedService
    {
        private MessageConsumer consumer;
        private readonly ILogger<MessageConsumer> _log;
        private readonly ConsumerConfig _consumerConfig;

        public ServiceBusHostedService(ILogger<MessageConsumer> log, ConsumerConfig consumerConfig)
        {
            _log = log;
            _consumerConfig = consumerConfig;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            consumer = new MessageConsumer(_log,_consumerConfig);

            await  consumer.StartListeners(cancellationToken);
               
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return  Task.CompletedTask;
        }
    }
}