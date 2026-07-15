using KafkaUI.Models;
using KafkaUI.Services;
using System.Collections.ObjectModel;
using System.Linq;
using System.Windows.Input;

namespace KafkaUI.ViewModels
{
    public class MainViewModel : ViewModelBase
    {
        private readonly IKafkaService _kafkaService;
        private readonly ClusterStore _clusterStore;
        private KafkaCluster? _selectedCluster;
        private object? _currentContent;
        private string _statusMessage = "Ready";
        private bool _isBusy;

        // Sidebar only ever shows the active (default) cluster
        public ObservableCollection<KafkaCluster> Clusters { get; } = new();

        public KafkaCluster? SelectedCluster
        {
            get => _selectedCluster;
            set
            {
                SetField(ref _selectedCluster, value);
                if (value != null) _ = LoadClusterOverview(value);
            }
        }

        public object? CurrentContent
        {
            get => _currentContent;
            set => SetField(ref _currentContent, value);
        }

        public string StatusMessage
        {
            get => _statusMessage;
            set => SetField(ref _statusMessage, value);
        }

        public bool IsBusy
        {
            get => _isBusy;
            set => SetField(ref _isBusy, value);
        }

        public ICommand ViewClustersCommand { get; }
        public ICommand RemoveClusterCommand { get; }
        public ICommand RefreshCommand { get; }
        public ICommand ShowDashboardCommand { get; }
        public ICommand ShowBrokersCommand { get; }
        public ICommand ShowTopicsCommand { get; }
        public ICommand ShowConsumersCommand { get; }
        public ICommand ShowSchemaRegistryCommand { get; }
        public ICommand ShowKafkaConnectCommand { get; }

        public MainViewModel(IKafkaService kafkaService, ClusterStore clusterStore)
        {
            _kafkaService = kafkaService;
            _clusterStore = clusterStore;

            ViewClustersCommand = new RelayCommand(ShowViewClustersDialog);
            RemoveClusterCommand = new RelayCommand(RemoveCluster, () => SelectedCluster != null);
            RefreshCommand = new AsyncRelayCommand(RefreshAsync, () => SelectedCluster != null);
            ShowDashboardCommand = new AsyncRelayCommand(async _ => await ShowDashboard());
            ShowBrokersCommand = new AsyncRelayCommand(async _ => await ShowBrokers());
            ShowTopicsCommand = new AsyncRelayCommand(async _ => await ShowTopics());
            ShowConsumersCommand = new AsyncRelayCommand(async _ => await ShowConsumers());
            ShowSchemaRegistryCommand = new RelayCommand(_ => ShowSchemaRegistry());
            ShowKafkaConnectCommand = new RelayCommand(_ => ShowKafkaConnect());

            LoadClusters();
        }

        private void LoadClusters()
        {
            var allClusters = _clusterStore.LoadClusters();
            var defaultCluster = allClusters.FirstOrDefault(c => c.IsDefault);
            if (defaultCluster != null)
            {
                Clusters.Add(defaultCluster);
                SelectedCluster = defaultCluster;
            }
        }

        /// <summary>
        /// Switches the sidebar to show only the new default cluster,
        /// disconnects the old one, and connects to the new one.
        /// </summary>
        private void SwitchToDefaultCluster(KafkaCluster newDefault)
        {
            // Disconnect and remove everything currently in the sidebar
            SelectedCluster = null;
            CurrentContent = null;
            Clusters.Clear();

            // Show only the new default
            Clusters.Add(newDefault);
            SelectedCluster = newDefault;
        }

        private void ShowViewClustersDialog()
        {
            var allClusters = _clusterStore.LoadClusters();
            var vm = new ViewClustersViewModel(_kafkaService, _clusterStore, allClusters);

            var dlg = new Views.ViewClustersDialog
            {
                DataContext = vm,
                Owner = System.Windows.Application.Current.MainWindow
            };

            vm.AddNewRequested += () =>
            {
                var addVm = new AddClusterViewModel(_kafkaService);
                var addDlg = new Views.AddClusterDialog { DataContext = addVm, Owner = dlg };
                addVm.ClusterAdded += cluster =>
                {
                    if (cluster.IsDefault)
                    {
                        _clusterStore.ClearDefaultFlag();
                    }
                    _clusterStore.UpsertCluster(cluster);
                    vm.AddCluster(cluster);

                    if (cluster.IsDefault)
                    {
                        addDlg.Close();
                        dlg.Close();
                        SwitchToDefaultCluster(cluster);
                    }
                    else
                    {
                        addDlg.Close();
                    }
                };
                addDlg.ShowDialog();
            };

            vm.EditRequested += cluster =>
            {
                var editVm = new AddClusterViewModel(_kafkaService)
                {
                    Name             = cluster.Name,
                    BootstrapServers = cluster.BootstrapServers,
                    SchemaRegistry   = cluster.SchemaRegistryUrl ?? string.Empty,
                    KafkaConnect     = cluster.KafkaConnectUrl   ?? string.Empty,
                    IsDefault        = cluster.IsDefault
                };
                var editDlg = new Views.AddClusterDialog { DataContext = editVm, Owner = dlg, Title = "Edit Cluster" };

                editVm.ClusterAdded += updated =>
                {
                    updated.Id = cluster.Id;
                    bool becomingDefault = updated.IsDefault && !cluster.IsDefault;

                    if (updated.IsDefault)
                    {
                        _clusterStore.ClearDefaultFlag();
                    }
                    _clusterStore.UpsertCluster(updated);
                    vm.UpdateCluster(updated);

                    editDlg.Close();

                    if (becomingDefault)
                    {
                        // Close the clusters dialog and switch sidebar to new default
                        dlg.Close();
                        SwitchToDefaultCluster(updated);
                    }
                    else if (SelectedCluster?.Id == updated.Id)
                    {
                        // Refresh sidebar entry if it's the currently connected cluster
                        Clusters[0] = updated;
                        SelectedCluster = updated;
                    }
                };
                editDlg.ShowDialog();
            };

            dlg.ShowDialog();
        }

        private void RemoveCluster()
        {
            if (SelectedCluster == null) return;
            _clusterStore.DeleteCluster(SelectedCluster.Id);
            Clusters.Remove(SelectedCluster);
            SelectedCluster = null;
            CurrentContent = null;
        }

        private async Task RefreshAsync()
        {
            if (SelectedCluster == null) return;
            await LoadClusterOverview(SelectedCluster);
        }

        private async Task LoadClusterOverview(KafkaCluster cluster)
        {
            await ShowDashboard();
        }

        private async Task ShowDashboard()
        {
            IsBusy = true;
            StatusMessage = "Loading dashboard...";
            try
            {
                var vm = new DashboardViewModel(_kafkaService, Clusters);
                await vm.LoadAsync();
                CurrentContent = vm;
                StatusMessage = "Ready";
            }
            catch (Exception ex)
            {
                StatusMessage = $"Error: {ex.Message}";
            }
            finally { IsBusy = false; }
        }

        private async Task ShowBrokers()
        {
            if (SelectedCluster == null) return;
            IsBusy = true;
            StatusMessage = "Loading brokers...";
            try
            {
                var vm = new BrokersViewModel(_kafkaService, SelectedCluster);
                await vm.LoadAsync();
                CurrentContent = vm;
                StatusMessage = "Ready";
            }
            catch (Exception ex) { StatusMessage = $"Error: {ex.Message}"; }
            finally { IsBusy = false; }
        }

        private async Task ShowTopics()
        {
            if (SelectedCluster == null) return;
            IsBusy = true;
            StatusMessage = "Loading topics...";
            try
            {
                var vm = new TopicsViewModel(_kafkaService, SelectedCluster);
                await vm.LoadAsync();
                CurrentContent = vm;
                StatusMessage = "Ready";
            }
            catch (Exception ex) { StatusMessage = $"Error: {ex.Message}"; }
            finally { IsBusy = false; }
        }

        private async Task ShowConsumers()
        {
            if (SelectedCluster == null) return;
            IsBusy = true;
            StatusMessage = "Loading consumer groups...";
            try
            {
                var vm = new ConsumersViewModel(_kafkaService, SelectedCluster);
                await vm.LoadAsync();
                CurrentContent = vm;
                StatusMessage = "Ready";
            }
            catch (Exception ex) { StatusMessage = $"Error: {ex.Message}"; }
            finally { IsBusy = false; }
        }

        private void ShowSchemaRegistry()
        {
            if (SelectedCluster == null) return;
            var vm = new SchemaRegistryViewModel();
            vm.Initialise(SelectedCluster);
            CurrentContent = vm;
        }

        private void ShowKafkaConnect()
        {
            if (SelectedCluster == null) return;
            var vm = new KafkaConnectViewModel(SelectedCluster);
            CurrentContent = vm;
        }
    }
}
