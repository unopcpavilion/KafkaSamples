using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using static System.Threading.Tasks.Task;

namespace WeatherLoader
{
    internal class WeatherPullService : BackgroundService
    {
        private readonly IWeatherProvider _provider;
        private readonly IWeatherPublisher _publisher;
        private readonly ILogger<WeatherPullService> _log;

        public WeatherPullService(IWeatherProvider provider, IWeatherPublisher publisher, ILogger<WeatherPullService> log)
        {
            _provider = provider;
            _publisher = publisher;
            _log = log;
        }
        
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var weather = await _provider.GetCurrent(stoppingToken);
                var tasks = new List<Task>();
                for (var i = 0; i < 50; i++)
                {
                    tasks.Add( _publisher.Publish(weather, stoppingToken, "WeatherTopic"+i));
                }

                await WhenAll(tasks);
                
                _log.LogInformation("Published {WeatherCount} weather items", weather.Count);
                await Delay(2000, stoppingToken);
            }
        }
    }
}