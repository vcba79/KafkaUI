using System;
using System.Windows;
using KafkaUI.ViewModels;

namespace KafkaUI.Views
{
    public partial class AddClusterDialog : Window
    {
        public AddClusterDialog() { InitializeComponent(); }
    }

    public partial class ViewClustersDialog : Window
    {
        public ViewClustersDialog() { InitializeComponent(); }

        protected override void OnContentRendered(EventArgs e)
        {
            base.OnContentRendered(e);
            if (DataContext is ViewClustersViewModel vm)
                vm.ClusterConnected += _ => Dispatcher.Invoke(Close);
        }
    }
}
