using System.Windows;
using System.Windows.Controls;
using KafkaUI.Models;

namespace KafkaUI.Views
{
    public partial class TopicDetailView : UserControl
    {
        public TopicDetailView() { InitializeComponent(); }

        private void MenuButton_Click(object sender, RoutedEventArgs e)
        {
            ActionsPopup.IsOpen = !ActionsPopup.IsOpen;
        }

        private void ActionItem_Click(object sender, RoutedEventArgs e)
        {
            ActionsPopup.IsOpen = false;
        }

        private void PartitionMenuButton_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button btn && btn.ContextMenu != null)
            {
                btn.ContextMenu.PlacementTarget = btn;
                btn.ContextMenu.IsOpen = true;
                e.Handled = true;
            }
        }

        private void ClearPartitionMenuItem_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not MenuItem item) return;
            // ContextMenu.Tag was bound to PlacementTarget.Tag which is the KafkaPartition
            var partition = (item.Parent as ContextMenu)?.Tag as KafkaPartition;
            if (partition == null) return;
            if (DataContext is KafkaUI.ViewModels.TopicDetailViewModel vm)
                vm.ClearPartitionCommand.Execute(partition);
        }
    }
}
