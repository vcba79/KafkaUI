using KafkaUI.Services;
using KafkaUI.ViewModels;
using KafkaUI.Views;
using Microsoft.Extensions.DependencyInjection;
using System.Windows;

namespace KafkaUI
{
    public partial class App : Application
    {
        private ServiceProvider? _serviceProvider;

        protected override void OnStartup(StartupEventArgs e)
        {
            base.OnStartup(e);

            var services = new ServiceCollection();
            services.AddSingleton<IKafkaService, KafkaService>();
            services.AddSingleton<ClusterStore>();
            services.AddTransient<MainViewModel>();
            services.AddTransient<MainWindow>();

            _serviceProvider = services.BuildServiceProvider();

            var window = _serviceProvider.GetRequiredService<MainWindow>();
            window.DataContext = _serviceProvider.GetRequiredService<MainViewModel>();
            window.Show();
        }

        protected override void OnExit(ExitEventArgs e)
        {
            _serviceProvider?.Dispose();
            base.OnExit(e);
        }
    }
}
