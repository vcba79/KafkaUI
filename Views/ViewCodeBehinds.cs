using System.Windows.Controls;
using System.Windows;
using KafkaUI.Models;
using KafkaUI.ViewModels;

namespace KafkaUI.Views
{
    public partial class DashboardView : UserControl { public DashboardView() { InitializeComponent(); } }
    public partial class BrokersView : UserControl { public BrokersView() { InitializeComponent(); } }

    public partial class TopicsView : UserControl
    {
        public TopicsView() { InitializeComponent(); }

        // Called when DataGrid loads — attach PreviewMouseLeftButtonDown to each checkbox cell
        private void TopicsGrid_LoadingRow(object? sender, DataGridRowEventArgs e)
        {
            e.Row.PreviewMouseLeftButtonDown += Row_PreviewMouseLeftButtonDown;
        }

        private void Row_PreviewMouseLeftButtonDown(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            if (sender is not DataGridRow row) return;

            // Walk up from the clicked element to find which DataGridCell was hit
            var dep = e.OriginalSource as System.Windows.DependencyObject;
            while (dep != null && dep is not DataGridCell)
                dep = System.Windows.Media.VisualTreeHelper.GetParent(dep);

            if (dep is not DataGridCell cell) return;
            if (cell.Column?.DisplayIndex != 0) return; // only intercept checkbox column

            // Toggle checked state and notify VM, then stop the event from selecting the row
            if (row.DataContext is KafkaTopic topic && !topic.IsInternal)
            {
                topic.IsChecked = !topic.IsChecked;
                if (DataContext is TopicsViewModel vm)
                    vm.ToggleTopicCheckedCommand.Execute(topic);
            }
            e.Handled = true; // prevent row selection / navigation
        }
    }

    public partial class ConsumersView : UserControl { public ConsumersView() { InitializeComponent(); } }
    public partial class SchemaRegistryView : UserControl { public SchemaRegistryView() { InitializeComponent(); } }
    public partial class KafkaConnectView : UserControl { public KafkaConnectView() { InitializeComponent(); } }
    public partial class BrokerDetailView : UserControl { public BrokerDetailView() { InitializeComponent(); } }
    public partial class CreateTopicView : UserControl { public CreateTopicView() { InitializeComponent(); } }
    public partial class ProduceMessageView : UserControl { public ProduceMessageView() { InitializeComponent(); } }
}
