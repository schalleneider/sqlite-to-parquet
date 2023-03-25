using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace sqlite_to_parquet
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Setup Host
            var host = CreateDefaultBuilder().Build();

            // Invoke Worker
            using IServiceScope serviceScope = host.Services.CreateScope();
            IServiceProvider provider = serviceScope.ServiceProvider;
            var workerInstance = provider.GetRequiredService<Worker>();
            await workerInstance.DoWorkAsync();

            host.Run();
        }

        static IHostBuilder CreateDefaultBuilder()
        {
            return Host.CreateDefaultBuilder()
                .ConfigureAppConfiguration(app =>
                {
                    app.AddJsonFile("appsettings.json");
                })
                .ConfigureServices(services =>
                {
                    services.AddSingleton<Worker>();
                });
        }
    }
}
