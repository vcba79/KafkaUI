using KafkaUI.Models;
using KafkaUI.Services;
using System.Collections.ObjectModel;
using System.Linq;
using System.Windows.Input;

namespace KafkaUI.ViewModels
{
    // ─── Dashboard ──────────────────────────────────────────────────────────────
    public class DashboardClusterRow : ViewModelBase
    {
        public string Name { get; set; } = string.Empty;
        public string Version { get; set; } = "1.0-UNKNOWN";
        public int BrokersCount { get; set; }
        public int Partitions { get; set; }
        public int Topics { get; set; }
        public long ProductionBytes { get; set; }
        public long ConsumptionBytes { get; set; }
        public bool IsOnline { get; set; }

        public string ProductionDisplay => FormatBytes(ProductionBytes);
        public string ConsumptionDisplay => FormatBytes(ConsumptionBytes);

        private static string FormatBytes(long b) => b switch
        {
            < 1024 => $"{b} Bytes",
            < 1024 * 1024 => $"{b / 1024.0:F1} KB",
            _ => $"{b / 1024.0 / 1024:F1} MB"
        };
    }

    public class DashboardViewModel : ViewModelBase
    {
        private readonly IKafkaService _kafka;
        private readonly IEnumerable<KafkaCluster> _clusters;
        private bool _onlyOfflineClusters;

        public ObservableCollection<DashboardClusterRow> ClusterRows { get; } = new();
        public ObservableCollection<DashboardClusterRow> FilteredClusterRows { get; } = new();

        public int OnlineCount  { get; private set; }
        public int OfflineCount { get; private set; }

        public bool OnlyOfflineClusters
        {
            get => _onlyOfflineClusters;
            set { SetField(ref _onlyOfflineClusters, value); ApplyFilter(); }
        }

        public DashboardViewModel(IKafkaService kafka, IEnumerable<KafkaCluster> clusters)
        {
            _kafka = kafka;
            _clusters = clusters;
        }

        public async Task LoadAsync()
        {
            ClusterRows.Clear();
            int online = 0, offline = 0;

            foreach (var cluster in _clusters)
            {
                var row = new DashboardClusterRow { Name = cluster.Name };
                try
                {
                    var brokers = await _kafka.GetBrokersAsync(cluster.BootstrapServers);
                    var topics  = await _kafka.GetTopicsAsync(cluster.BootstrapServers);

                    row.BrokersCount = brokers.Count;
                    row.Partitions   = topics.Sum(t => t.Partitions);
                    row.Topics       = topics.Count;
                    row.IsOnline     = true;

                    cluster.IsConnected = true;
                    cluster.BrokerCount = brokers.Count;
                    cluster.TopicCount  = topics.Count;
                    online++;
                }
                catch
                {
                    row.IsOnline = false;
                    cluster.IsConnected = false;
                    offline++;
                }
                ClusterRows.Add(row);
            }

            OnlineCount  = online;
            OfflineCount = offline;
            OnPropertyChanged(nameof(OnlineCount));
            OnPropertyChanged(nameof(OfflineCount));
            ApplyFilter();
        }

        private void ApplyFilter()
        {
            FilteredClusterRows.Clear();
            foreach (var row in ClusterRows)
                if (!OnlyOfflineClusters || !row.IsOnline)
                    FilteredClusterRows.Add(row);
        }
    }

    // ─── Broker Detail ───────────────────────────────────────────────────────
    public class BrokerDetailViewModel : ViewModelBase
    {
        private readonly IKafkaService _kafka;
        private readonly KafkaCluster _cluster;
        private readonly int _brokerId;

        public KafkaBrokerDetail? Detail { get; private set; }
        public ICommand BackCommand { get; }
        public ICommand EditConfigCommand { get; }
        public ICommand SaveConfigCommand { get; }
        public ICommand CancelEditCommand { get; }
        public ICommand NextPageCommand { get; }
        public ICommand PreviousPageCommand { get; }
        public event Action? BackRequested;

        // ── Configs pagination ──────────────────────────────────────────────
        private const int PageSize = 15;
        private int _currentPage = 1;

        public ObservableCollection<BrokerConfigEntry> PagedConfigEntries { get; } = new();

        public int CurrentPage
        {
            get => _currentPage;
            private set { SetField(ref _currentPage, value); ApplyPage(); }
        }

        public int TotalPages => Detail == null || Detail.ConfigEntries.Count == 0
            ? 1
            : (int)Math.Ceiling(Detail.ConfigEntries.Count / (double)PageSize);

        public bool HasPreviousPage => CurrentPage > 1;
        public bool HasNextPage => CurrentPage < TotalPages;
        public string PageDisplay => $"Page {CurrentPage} of {TotalPages}";

        public BrokerDetailViewModel(IKafkaService kafka, KafkaCluster cluster, int brokerId)
        {
            _kafka = kafka;
            _cluster = cluster;
            _brokerId = brokerId;
            BackCommand = new RelayCommand(_ => BackRequested?.Invoke());
            EditConfigCommand = new RelayCommand(entry =>
            {
                if (entry is not BrokerConfigEntry e) return;
                e.EditValue = e.Value;
                e.IsEditing = true;
            });
            SaveConfigCommand = new AsyncRelayCommand(async obj => await SaveConfigAsync(obj as BrokerConfigEntry));
            CancelEditCommand = new RelayCommand(entry =>
            {
                if (entry is not BrokerConfigEntry e) return;
                e.IsEditing = false;
            });
            NextPageCommand = new RelayCommand(_ => CurrentPage++, _ => HasNextPage);
            PreviousPageCommand = new RelayCommand(_ => CurrentPage--, _ => HasPreviousPage);
        }

        public async Task LoadAsync()
        {
            Detail = await _kafka.GetBrokerDetailAsync(_cluster.BootstrapServers, _brokerId);
            OnPropertyChanged(nameof(Detail));
            _currentPage = 1;
            OnPropertyChanged(nameof(CurrentPage));
            OnPropertyChanged(nameof(TotalPages));
            ApplyPage();
        }

        private void ApplyPage()
        {
            PagedConfigEntries.Clear();
            if (Detail == null) return;

            var page = Detail.ConfigEntries
                .Skip((CurrentPage - 1) * PageSize)
                .Take(PageSize);
            foreach (var e in page) PagedConfigEntries.Add(e);

            OnPropertyChanged(nameof(HasPreviousPage));
            OnPropertyChanged(nameof(HasNextPage));
            OnPropertyChanged(nameof(PageDisplay));
            OnPropertyChanged(nameof(TotalPages));
            System.Windows.Input.CommandManager.InvalidateRequerySuggested();
        }

        private async Task SaveConfigAsync(BrokerConfigEntry? entry)
        {
            if (entry == null) return;
            try
            {
                await _kafka.UpdateBrokerConfigAsync(_cluster.BootstrapServers, _brokerId, entry.Key, entry.EditValue);
                entry.Value = entry.EditValue;
                entry.IsEditing = false;
            }
            catch (Exception ex)
            {
                System.Windows.MessageBox.Show($"Failed to update config: {ex.Message}", "Error",
                    System.Windows.MessageBoxButton.OK, System.Windows.MessageBoxImage.Error);
            }
        }
    }

    // ─── Brokers ──────────────────────────────────────────────────────────────
    // ─── Brokers ─────────────────────────────────────────────────────────────
    public class BrokersViewModel : ViewModelBase
    {
        private readonly IKafkaService _kafka;
        private readonly KafkaCluster _cluster;
        private KafkaBroker? _selectedBroker;

        public ObservableCollection<KafkaBroker> Brokers { get; } = new();
        public KafkaClusterStats Stats { get; private set; } = new();
        public BrokerDetailViewModel? BrokerDetail { get; private set; }

        public KafkaBroker? SelectedBroker
        {
            get => _selectedBroker;
            set { SetField(ref _selectedBroker, value); if (value != null) LoadBrokerDetail(value.Id); }
        }

        // Uptime card
        public int BrokerCount => Stats.BrokerCount;
        public int ActiveController => Stats.ActiveController;
        public string ActiveControllerDisplay => Stats.ActiveController >= 0 ? Stats.ActiveController.ToString() : "-";
        public string Version => Stats.Version;

        // Partitions card
        public string OnlinePartitionsDisplay => $"{Stats.OnlinePartitions} of {Stats.TotalPartitions}";
        public int UnderReplicatedPartitions => Stats.UnderReplicatedPartitions;
        public string InSyncReplicasDisplay => $"{Stats.InSyncReplicas} of {Stats.TotalReplicas}";
        public int OutOfSyncReplicas => Stats.OutOfSyncReplicas;

        public BrokersViewModel(IKafkaService kafka, KafkaCluster cluster)
        {
            _kafka = kafka;
            _cluster = cluster;
        }

        private async void LoadBrokerDetail(int brokerId)
        {
            var vm = new BrokerDetailViewModel(_kafka, _cluster, brokerId);
            vm.BackRequested += () =>
            {
                BrokerDetail = null;
                _selectedBroker = null;
                OnPropertyChanged(nameof(BrokerDetail));
                OnPropertyChanged(nameof(SelectedBroker));
            };
            await vm.LoadAsync();
            BrokerDetail = vm;
            OnPropertyChanged(nameof(BrokerDetail));
        }

        public async Task LoadAsync()
        {
            var brokers = await _kafka.GetBrokersAsync(_cluster.BootstrapServers);
            Brokers.Clear();
            foreach (var b in brokers) Brokers.Add(b);

            var topics = await _kafka.GetTopicsAsync(_cluster.BootstrapServers, true);
            int totalPartitions = topics.Sum(t => t.Partitions);
            int underReplicated = topics.SelectMany(t => t.PartitionList).Count(p => p.IsUnderReplicated);
            // Online partitions = partitions with a live leader assigned (Leader >= 0).
            int onlinePartitions = topics.SelectMany(t => t.PartitionList).Count(p => p.Leader >= 0);
            int totalReplicas = topics.SelectMany(t => t.PartitionList).Sum(p => p.Replicas.Count);
            int inSyncReplicas = topics.SelectMany(t => t.PartitionList).Sum(p => p.InSyncReplicas.Count);

            Stats = new KafkaClusterStats
            {
                BrokerCount = brokers.Count,
                ActiveController = brokers.FirstOrDefault(b => b.IsController)?.Id ?? -1,
                Version = "1.0-UNKNOWN",
                TotalPartitions = totalPartitions,
                OnlinePartitions = onlinePartitions,
                TotalReplicas = totalReplicas,
                InSyncReplicas = inSyncReplicas,
                UnderReplicatedPartitions = underReplicated,
                OutOfSyncReplicas = totalReplicas - inSyncReplicas,
            };
            OnPropertyChanged(nameof(Stats));
            OnPropertyChanged(nameof(BrokerCount));
            OnPropertyChanged(nameof(ActiveController));
            OnPropertyChanged(nameof(ActiveControllerDisplay));
            OnPropertyChanged(nameof(Version));
            OnPropertyChanged(nameof(OnlinePartitionsDisplay));
            OnPropertyChanged(nameof(UnderReplicatedPartitions));
            OnPropertyChanged(nameof(InSyncReplicasDisplay));
            OnPropertyChanged(nameof(OutOfSyncReplicas));
        }
    }

    // ─── Topics ──────────────────────────────────────────────────────────────
    public class TopicsViewModel : ViewModelBase
    {
        private readonly IKafkaService _kafka;
        private readonly KafkaCluster _cluster;
        private string _searchText = string.Empty;
        private bool _showInternal;
        private KafkaTopic? _selectedTopic;

        public ObservableCollection<KafkaTopic> Topics { get; } = new();
        public ObservableCollection<KafkaTopic> FilteredTopics { get; } = new();

        // Tracks non-internal topics the user has checked
        public ObservableCollection<KafkaTopic> CheckedTopics { get; } = new();
        public bool HasCheckedTopics => CheckedTopics.Count > 0;

        public string SearchText
        {
            get => _searchText;
            set { SetField(ref _searchText, value); ApplyFilter(); }
        }

        public bool ShowInternal
        {
            get => _showInternal;
            set { SetField(ref _showInternal, value); ApplyFilter(); }
        }

        public KafkaTopic? SelectedTopic
        {
            get => _selectedTopic;
            set { SetField(ref _selectedTopic, value); if (value != null) LoadTopicDetails(value); }
        }

        public TopicDetailViewModel? TopicDetail { get; private set; }
        public CreateTopicViewModel? CreateTopicVM { get; private set; }

        public ICommand CreateTopicCommand { get; }
        public ICommand DeleteTopicCommand { get; }
        public ICommand DeleteSelectedCommand { get; }
        public ICommand CopySelectedCommand { get; }
        public ICommand PurgeSelectedCommand { get; }
        public ICommand RefreshCommand { get; }
        public ICommand ToggleTopicCheckedCommand { get; }

        public TopicsViewModel(IKafkaService kafka, KafkaCluster cluster)
        {
            _kafka = kafka;
            _cluster = cluster;
            CreateTopicCommand = new RelayCommand(ShowCreateTopicDialog);
            DeleteTopicCommand = new AsyncRelayCommand(DeleteTopicAsync, () => SelectedTopic != null);
            DeleteSelectedCommand = new AsyncRelayCommand(DeleteSelectedAsync, () => HasCheckedTopics);
            CopySelectedCommand   = new RelayCommand(_ => { /* TODO */ }, _ => HasCheckedTopics);
            PurgeSelectedCommand  = new AsyncRelayCommand(PurgeSelectedAsync,  () => HasCheckedTopics);
            RefreshCommand = new AsyncRelayCommand(LoadAsync);
            ToggleTopicCheckedCommand = new RelayCommand(ToggleTopicChecked);
            CheckedTopics.CollectionChanged += (_, _) =>
            {
                OnPropertyChanged(nameof(HasCheckedTopics));
                System.Windows.Input.CommandManager.InvalidateRequerySuggested();
            };
        }

        public async Task LoadAsync()
        {
            var topics = await _kafka.GetTopicsAsync(_cluster.BootstrapServers, true); // always fetch all; ShowInternal filters client-side
            Topics.Clear();
            foreach (var t in topics) Topics.Add(t);
            ApplyFilter();
        }

        private void ApplyFilter()
        {
            FilteredTopics.Clear();
            var filtered = Topics.Where(t =>
                (ShowInternal || !t.IsInternal) &&
                (string.IsNullOrWhiteSpace(SearchText) || t.Name.Contains(SearchText, StringComparison.OrdinalIgnoreCase)));
            foreach (var t in filtered) FilteredTopics.Add(t);
        }

        private void ShowCreateTopicDialog()
        {
            var vm = new CreateTopicViewModel();
            vm.Cancelled += () => { CreateTopicVM = null; OnPropertyChanged(nameof(CreateTopicVM)); };
            vm.TopicCreated += async spec =>
            {
                CreateTopicVM = null;
                OnPropertyChanged(nameof(CreateTopicVM));
                await _kafka.CreateTopicAsync(_cluster.BootstrapServers, spec.Name, spec.Partitions, spec.ReplicationFactor, spec.Config);
                await LoadAsync();
            };
            CreateTopicVM = vm;
            OnPropertyChanged(nameof(CreateTopicVM));
        }

        private void ToggleTopicChecked(object? param)
        {
            if (param is not KafkaTopic topic || topic.IsInternal) return;
            if (CheckedTopics.Contains(topic))
                CheckedTopics.Remove(topic);
            else
                CheckedTopics.Add(topic);
        }

        private async Task DeleteTopicAsync()
        {
            if (SelectedTopic == null) return;
            var result = System.Windows.MessageBox.Show(
                $"Delete topic '{SelectedTopic.Name}'? This cannot be undone.",
                "Confirm Delete", System.Windows.MessageBoxButton.YesNo, System.Windows.MessageBoxImage.Warning);
            if (result != System.Windows.MessageBoxResult.Yes) return;
            await _kafka.DeleteTopicAsync(_cluster.BootstrapServers, SelectedTopic.Name);
            await LoadAsync();
        }

        private async Task DeleteSelectedAsync()
        {
            if (CheckedTopics.Count == 0) return;
            var names = string.Join(", ", CheckedTopics.Select(t => t.Name));
            var result = System.Windows.MessageBox.Show(
                $"Delete {CheckedTopics.Count} topic(s)?\n{names}\n\nThis cannot be undone.",
                "Confirm Delete", System.Windows.MessageBoxButton.YesNo, System.Windows.MessageBoxImage.Warning);
            if (result != System.Windows.MessageBoxResult.Yes) return;
            foreach (var t in CheckedTopics.ToList())
                await _kafka.DeleteTopicAsync(_cluster.BootstrapServers, t.Name);
            CheckedTopics.Clear();
            await LoadAsync();
        }

        private async Task PurgeSelectedAsync()
        {
            if (CheckedTopics.Count == 0) return;
            var result = System.Windows.MessageBox.Show(
                $"Purge all messages from {CheckedTopics.Count} topic(s)?",
                "Confirm Purge", System.Windows.MessageBoxButton.YesNo, System.Windows.MessageBoxImage.Warning);
            if (result != System.Windows.MessageBoxResult.Yes) return;
            // Purge by setting retention to 1ms then restoring — standard Kafka approach
            foreach (var t in CheckedTopics.ToList())
            {
                var purgeConfig = new Dictionary<string, string> { ["retention.ms"] = "1" };
                await _kafka.CreateTopicAsync(_cluster.BootstrapServers, t.Name, t.Partitions, (short)t.ReplicationFactor, purgeConfig);
            }
        }

        private async void LoadTopicDetails(KafkaTopic topic)
        {
            var vm = new TopicDetailViewModel(_kafka, _cluster, topic.Name);
            vm.BackRequested += () =>
            {
                TopicDetail = null;
                _selectedTopic = null;
                OnPropertyChanged(nameof(TopicDetail));
                OnPropertyChanged(nameof(SelectedTopic));
            };
            vm.TopicRemoved += async () =>
            {
                TopicDetail = null;
                _selectedTopic = null;
                OnPropertyChanged(nameof(TopicDetail));
                OnPropertyChanged(nameof(SelectedTopic));
                await LoadAsync();
            };
            await vm.LoadAsync();
            TopicDetail = vm;
            OnPropertyChanged(nameof(TopicDetail));
        }
    }

    public record TopicCreationSpec(string Name, int Partitions, short ReplicationFactor, Dictionary<string, string> Config);

    public class TopicDetailViewModel : ViewModelBase
    {
        private readonly IKafkaService _kafka;
        private readonly KafkaCluster _cluster;
        private readonly string _topicName;

        // Filter state
        private string _seekType = "Offset";
        private string _offsetValue = "0";
        private DateTime? _timestampValue;
        private string _keySerde = "String";
        private string _valueSerde = "String";
        private string _searchText = string.Empty;
        private string _sortOrder = "Oldest First";
        private bool _isLoadingMessages;

        // Stats
        private long _elapsedMs;
        private long _totalBytes;
        private int  _consumedCount;

        public KafkaTopic? Topic { get; private set; }
        public ObservableCollection<KafkaMessage> Messages       { get; } = new();
        public ObservableCollection<KafkaMessage> FilteredMessages { get; } = new();
        public ObservableCollection<string>       PartitionItems  { get; } = new() { "All items are selected." };

        private KafkaMessage? _selectedMessage;
        public KafkaMessage? SelectedMessage
        {
            get => _selectedMessage;
            set { SetField(ref _selectedMessage, value); }
        }

        private string _selectedPartitionItem = "All items are selected.";
        private int    _selectedPartition = -1;

        public string SelectedPartitionItem
        {
            get => _selectedPartitionItem;
            set
            {
                SetField(ref _selectedPartitionItem, value);
                _selectedPartition = value == "All items are selected." ? -1
                    : int.TryParse(value?.Replace("Partition ", ""), out var p) ? p : -1;
            }
        }

        public string SeekType
        {
            get => _seekType;
            set
            {
                SetField(ref _seekType, value);
                OnPropertyChanged(nameof(IsOffsetSeek));
                OnPropertyChanged(nameof(IsTimestampSeek));
            }
        }
        public string OffsetValue { get => _offsetValue; set => SetField(ref _offsetValue, value); }
        public DateTime? TimestampValue { get => _timestampValue; set => SetField(ref _timestampValue, value); }
        public string KeySerde   { get => _keySerde;   set => SetField(ref _keySerde, value); }
        public string ValueSerde { get => _valueSerde; set => SetField(ref _valueSerde, value); }
        public string SortOrder
        {
            get => _sortOrder;
            set
            {
                if (SetField(ref _sortOrder, value))
                    ApplyFilter();
            }
        }
        public bool   IsLoadingMessages { get => _isLoadingMessages; set => SetField(ref _isLoadingMessages, value); }

        public bool IsOffsetSeek    => _seekType == "Offset";
        public bool IsTimestampSeek => _seekType == "Timestamp";

        public string SearchText
        {
            get => _searchText;
            set { SetField(ref _searchText, value); ApplyFilter(); }
        }

        // Matches the original kafka-ui Messages filter bar: only Offset and Timestamp are
        // valid seek types. "Offset" shows a text input, "Timestamp" shows a date picker.
        public IEnumerable<string> SeekTypes  => new[] { "Offset", "Timestamp" };
        public IEnumerable<string> SerdeTypes => new[] { "String", "JSON", "Avro", "Protobuf", "Long", "Int" };
        public IEnumerable<string> SortOrders => new[] { "Oldest First", "Newest First" };

        // Stats display
        public string ElapsedDisplay  => $"{_elapsedMs} ms";
        public string BytesDisplay    => FormatBytes(_totalBytes);
        public string ConsumedDisplay => $"{_consumedCount} messages consumed";
        public bool   HasStats        => _consumedCount > 0;

        // Inline produce page
        private ProduceMessageViewModel? _produceVM;
        public ProduceMessageViewModel? ProduceVM
        {
            get => _produceVM;
            private set { SetField(ref _produceVM, value); }
        }

        // Consumers tab
        public ObservableCollection<KafkaConsumerGroup> ConsumerGroups { get; } = new();
        public ObservableCollection<KafkaConsumerGroup> FilteredConsumerGroups { get; } = new();
        public bool HasConsumerGroups => FilteredConsumerGroups.Count > 0;

        private string _consumerSearchText = string.Empty;
        public string ConsumerSearchText
        {
            get => _consumerSearchText;
            set { SetField(ref _consumerSearchText, value); ApplyConsumerFilter(); }
        }

        public ICommand LoadMessagesCommand   { get; }
        public ICommand ClearMessagesCommand  { get; }
        public ICommand ProduceMessageCommand { get; }
        public ICommand BackCommand           { get; }
        public ICommand RestartAnalysisCommand { get; }

        // ⋮ menu actions
        public ICommand PurgeTopicCommand   { get; }
        public ICommand RecreateTopicCommand { get; }
        public ICommand RemoveTopicCommand  { get; }
        public ICommand ClearPartitionCommand { get; }

        public bool CanPurge => string.Equals(Topic?.CleanupPolicy, "delete", StringComparison.OrdinalIgnoreCase);

        private TopicStatistics? _statistics;
        public TopicStatistics? Statistics
        {
            get => _statistics;
            private set => SetField(ref _statistics, value);
        }

        private bool _isAnalyzing;
        public bool IsAnalyzing
        {
            get => _isAnalyzing;
            private set => SetField(ref _isAnalyzing, value);
        }

        public event Action? BackRequested;
        public event Action? TopicRemoved;

        public TopicDetailViewModel(IKafkaService kafka, KafkaCluster cluster, string topicName)
        {
            _kafka = kafka;
            _cluster = cluster;
            _topicName = topicName;
            LoadMessagesCommand   = new AsyncRelayCommand(LoadMessagesAsync);
            ClearMessagesCommand  = new RelayCommand(_ =>
            {
                Messages.Clear(); FilteredMessages.Clear();
                _consumedCount = 0; NotifyStats();
            });
            ProduceMessageCommand = new RelayCommand(_ => OpenProducePage());
            BackCommand           = new RelayCommand(_ => BackRequested?.Invoke());
            RestartAnalysisCommand = new AsyncRelayCommand(AnalyzeAsync, () => !IsAnalyzing);

            PurgeTopicCommand    = new AsyncRelayCommand(PurgeTopicAsync, () => CanPurge);
            RecreateTopicCommand = new AsyncRelayCommand(RecreateTopicAsync);
            RemoveTopicCommand   = new AsyncRelayCommand(RemoveTopicAsync);
            ClearPartitionCommand = new AsyncRelayCommand(async obj => await ClearPartitionAsync(obj as KafkaPartition));
        }

        private async Task AnalyzeAsync()
        {
            IsAnalyzing = true;
            System.Windows.Input.CommandManager.InvalidateRequerySuggested();
            try
            {
                Statistics = await _kafka.AnalyzeTopicAsync(_cluster.BootstrapServers, _topicName);
            }
            catch (Exception ex)
            {
                System.Windows.MessageBox.Show(
                    $"Failed to analyze topic '{_topicName}':\n{ex.Message}",
                    "Analysis Failed", System.Windows.MessageBoxButton.OK, System.Windows.MessageBoxImage.Error);
            }
            finally
            {
                IsAnalyzing = false;
                System.Windows.Input.CommandManager.InvalidateRequerySuggested();
            }
        }

        private async Task PurgeTopicAsync()
        {
            var result = System.Windows.MessageBox.Show(
                $"Clear all messages in '{_topicName}'? This cannot be undone.",
                "Confirm Clear Messages", System.Windows.MessageBoxButton.YesNo, System.Windows.MessageBoxImage.Warning);
            if (result != System.Windows.MessageBoxResult.Yes) return;
            try
            {
                await _kafka.ClearTopicMessagesAsync(_cluster.BootstrapServers, _topicName);
                await LoadMessagesAsync();
            }
            catch (Exception ex)
            {
                System.Windows.MessageBox.Show(
                    $"Failed to clear messages in '{_topicName}':\n{ex.Message}",
                    "Clear Messages Failed", System.Windows.MessageBoxButton.OK, System.Windows.MessageBoxImage.Error);
            }
        }

        private async Task RecreateTopicAsync()
        {
            var result = System.Windows.MessageBox.Show(
                $"Recreate topic '{_topicName}'? All messages will be lost and the topic will be re-created with the same settings.",
                "Confirm Recreate Topic", System.Windows.MessageBoxButton.YesNo, System.Windows.MessageBoxImage.Warning);
            if (result != System.Windows.MessageBoxResult.Yes) return;
            try
            {
                await _kafka.RecreateTopicAsync(_cluster.BootstrapServers, _topicName);
                await LoadAsync();
            }
            catch (Exception ex)
            {
                System.Windows.MessageBox.Show(
                    $"Failed to recreate topic '{_topicName}':\n{ex.Message}",
                    "Recreate Topic Failed", System.Windows.MessageBoxButton.OK, System.Windows.MessageBoxImage.Error);
            }
        }

        private async Task RemoveTopicAsync()
        {
            var result = System.Windows.MessageBox.Show(
                $"Remove topic '{_topicName}'? This cannot be undone.",
                "Confirm Remove Topic", System.Windows.MessageBoxButton.YesNo, System.Windows.MessageBoxImage.Warning);
            if (result != System.Windows.MessageBoxResult.Yes) return;
            try
            {
                await _kafka.DeleteTopicAsync(_cluster.BootstrapServers, _topicName);
                TopicRemoved?.Invoke();
            }
            catch (Exception ex)
            {
                System.Windows.MessageBox.Show(
                    $"Failed to remove topic '{_topicName}':\n{ex.Message}",
                    "Remove Topic Failed", System.Windows.MessageBoxButton.OK, System.Windows.MessageBoxImage.Error);
            }
        }

        public async Task LoadConsumerGroupsAsync()
        {
            var groups = await _kafka.GetConsumerGroupsForTopicAsync(_cluster.BootstrapServers, _topicName);
            ConsumerGroups.Clear();
            foreach (var g in groups) ConsumerGroups.Add(g);
            ApplyConsumerFilter();
        }

        private void ApplyConsumerFilter()
        {
            FilteredConsumerGroups.Clear();
            var q = _consumerSearchText.Trim();
            foreach (var g in ConsumerGroups)
                if (string.IsNullOrEmpty(q) || g.GroupId.Contains(q, StringComparison.OrdinalIgnoreCase))
                    FilteredConsumerGroups.Add(g);
            OnPropertyChanged(nameof(HasConsumerGroups));
        }

        public async Task LoadAsync()
        {
            Topic = await _kafka.GetTopicDetailsAsync(_cluster.BootstrapServers, _topicName);
            OnPropertyChanged(nameof(Topic));
            OnPropertyChanged(nameof(CanPurge));
            System.Windows.Input.CommandManager.InvalidateRequerySuggested();

            PartitionItems.Clear();
            PartitionItems.Add("All items are selected.");
            if (Topic != null)
                for (int i = 0; i < Topic.Partitions; i++)
                    PartitionItems.Add($"Partition {i}");
            SelectedPartitionItem = "All items are selected.";

            await LoadMessagesAsync();
            await LoadConsumerGroupsAsync();
        }

        private async Task LoadMessagesAsync()
        {
            IsLoadingMessages = true;
            var sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                long offset = long.TryParse(_offsetValue, out var o) ? o : 0;
                DateTime? seekTimestamp = IsTimestampSeek ? _timestampValue : null;
                var msgs = await _kafka.GetMessagesAsync(
                    _cluster.BootstrapServers, _topicName, _selectedPartition, offset, 100,
                    seekTimestamp: seekTimestamp);

                Messages.Clear();
                foreach (var m in msgs) Messages.Add(m);

                _elapsedMs    = sw.ElapsedMilliseconds;
                _totalBytes   = msgs.Sum(m => (long)(m.Value?.Length ?? 0) + (m.Key?.Length ?? 0));
                _consumedCount = msgs.Count;
                NotifyStats();
                ApplyFilter();
            }
            finally { IsLoadingMessages = false; }
        }

        // Rebuilds FilteredMessages (the DataGrid's actual ItemsSource) from Messages,
        // applying both the search text filter and the current sort order. Messages
        // itself is never mutated here, so this is safe to call any time the search
        // text or the Sort Order dropdown (Oldest First / Newest First) changes -
        // sorting takes effect immediately without needing to press Submit again.
        private void ApplyFilter()
        {
            var q = _searchText.Trim();
            IEnumerable<KafkaMessage> query = Messages;

            if (!string.IsNullOrEmpty(q))
                query = query.Where(m =>
                    (m.Key?.Contains(q, StringComparison.OrdinalIgnoreCase) ?? false) ||
                    (m.Value?.Contains(q, StringComparison.OrdinalIgnoreCase) ?? false));

            query = _sortOrder == "Newest First"
                ? query.OrderByDescending(m => m.Timestamp)
                : query.OrderBy(m => m.Timestamp);

            FilteredMessages.Clear();
            foreach (var m in query) FilteredMessages.Add(m);
        }

        private void NotifyStats()
        {
            OnPropertyChanged(nameof(ElapsedDisplay));
            OnPropertyChanged(nameof(BytesDisplay));
            OnPropertyChanged(nameof(ConsumedDisplay));
            OnPropertyChanged(nameof(HasStats));
        }

        private static string FormatBytes(long b) => b switch
        {
            < 1024 => $"{b} Bytes",
            < 1024 * 1024 => $"{b / 1024.0:F1} KB",
            _ => $"{b / 1024.0 / 1024:F1} MB"
        };

        private async Task ClearPartitionAsync(KafkaPartition? partition)
        {
            if (partition == null) return;
            var result = System.Windows.MessageBox.Show(
                $"Clear all messages in partition {partition.Id} of '{_topicName}'? This cannot be undone.",
                "Confirm Clear Messages", System.Windows.MessageBoxButton.YesNo, System.Windows.MessageBoxImage.Warning);
            if (result != System.Windows.MessageBoxResult.Yes) return;
            try
            {
                await _kafka.ClearPartitionMessagesAsync(_cluster.BootstrapServers, _topicName, partition.Id);
                await LoadMessagesAsync();
            }
            catch (Exception ex)
            {
                System.Windows.MessageBox.Show(
                    $"Failed to clear partition {partition.Id}:\n{ex.Message}",
                    "Clear Messages Failed", System.Windows.MessageBoxButton.OK, System.Windows.MessageBoxImage.Error);
            }
        }

        private void OpenProducePage()
        {
            var vm = new ProduceMessageViewModel();
            vm.Cancelled       += () => { ProduceVM = null; };
            vm.MessageProduced += async args =>
            {
                ProduceVM = null;
                await _kafka.ProduceMessageAsync(
                    _cluster.BootstrapServers, _topicName,
                    args.Key, args.Value, args.Headers, args.Partition);
                await LoadMessagesAsync();
            };
            ProduceVM = vm;
        }
    }

    // ─── Consumers ───────────────────────────────────────────────────────────
    public class ConsumersViewModel : ViewModelBase
    {
        private readonly IKafkaService _kafka;
        private readonly KafkaCluster _cluster;
        private KafkaConsumerGroup? _selectedGroup;

        public ObservableCollection<KafkaConsumerGroup> Groups { get; } = new();
        public KafkaConsumerGroup? SelectedGroup
        {
            get => _selectedGroup;
            set { SetField(ref _selectedGroup, value); if (value != null) LoadGroupDetails(value.GroupId); }
        }
        public KafkaConsumerGroup? GroupDetails { get; private set; }

        public ConsumersViewModel(IKafkaService kafka, KafkaCluster cluster)
        {
            _kafka = kafka;
            _cluster = cluster;
        }

        public async Task LoadAsync()
        {
            var groups = await _kafka.GetConsumerGroupsAsync(_cluster.BootstrapServers);
            Groups.Clear();
            foreach (var g in groups) Groups.Add(g);
        }

        private async void LoadGroupDetails(string groupId)
        {
            GroupDetails = await _kafka.GetConsumerGroupDetailsAsync(_cluster.BootstrapServers, groupId);
            OnPropertyChanged(nameof(GroupDetails));
        }
    }


    // ─── Kafka Connect ────────────────────────────────────────────────────────
    public class KafkaConnectViewModel : ViewModelBase
    {
        private readonly KafkaCluster _cluster;
        public string ConnectUrl => _cluster.KafkaConnectUrl ?? "(not configured)";
        public bool IsConfigured => !string.IsNullOrWhiteSpace(_cluster.KafkaConnectUrl);
        public ObservableCollection<KafkaConnector> Connectors { get; } = new();

        public KafkaConnectViewModel(KafkaCluster cluster) { _cluster = cluster; }
    }

    // ─── Add Cluster Dialog ───────────────────────────────────────────────────
    public class AddClusterViewModel : ViewModelBase
    {
        private readonly IKafkaService _kafka;
        private string _name = string.Empty;
        private string _bootstrapServers = "localhost:9092";
        private string _schemaRegistry = string.Empty;
        private string _kafkaConnect = string.Empty;
        private string _statusMessage = string.Empty;
        private bool _isTesting;
        private bool _isDefault;

        public event Action<KafkaCluster>? ClusterAdded;

        public string Name { get => _name; set => SetField(ref _name, value); }
        public string BootstrapServers { get => _bootstrapServers; set => SetField(ref _bootstrapServers, value); }
        public string SchemaRegistry { get => _schemaRegistry; set => SetField(ref _schemaRegistry, value); }
        public string KafkaConnect { get => _kafkaConnect; set => SetField(ref _kafkaConnect, value); }
        public string StatusMessage { get => _statusMessage; set => SetField(ref _statusMessage, value); }
        public bool IsTesting { get => _isTesting; set => SetField(ref _isTesting, value); }
        public bool IsDefault { get => _isDefault; set => SetField(ref _isDefault, value); }

        public ICommand TestConnectionCommand { get; }
        public ICommand SaveCommand { get; }

        public AddClusterViewModel(IKafkaService kafka)
        {
            _kafka = kafka;
            TestConnectionCommand = new AsyncRelayCommand(TestConnectionAsync);
            SaveCommand = new RelayCommand(Save, () => !string.IsNullOrWhiteSpace(Name) && !string.IsNullOrWhiteSpace(BootstrapServers));
        }

        private async Task TestConnectionAsync()
        {
            IsTesting = true;
            StatusMessage = "Testing connection...";
            var ok = await _kafka.TestConnectionAsync(BootstrapServers);
            StatusMessage = ok ? "✓ Connection successful!" : "✗ Connection failed";
            IsTesting = false;
        }

        private void Save()
        {
            var cluster = new KafkaCluster
            {
                Name = Name,
                BootstrapServers = BootstrapServers,
                SchemaRegistryUrl = string.IsNullOrWhiteSpace(SchemaRegistry) ? null : SchemaRegistry,
                KafkaConnectUrl = string.IsNullOrWhiteSpace(KafkaConnect) ? null : KafkaConnect,
                IsDefault = IsDefault
            };
            ClusterAdded?.Invoke(cluster);
        }
    }

    // ─── View Clusters Dialog ─────────────────────────────────────────────────
    public class ViewClustersViewModel : ViewModelBase
    {
        private readonly IKafkaService _kafkaService;
        private readonly ClusterStore _clusterStore;
        private KafkaCluster? _selectedCluster;

        public ObservableCollection<KafkaCluster> Clusters { get; }

        public KafkaCluster? SelectedCluster
        {
            get => _selectedCluster;
            set { SetField(ref _selectedCluster, value); System.Windows.Input.CommandManager.InvalidateRequerySuggested(); }
        }

        public event Action<KafkaCluster>? ClusterConnected;
        public event Action? AddNewRequested;
        public event Action<KafkaCluster>? EditRequested;

        public ICommand ConnectCommand { get; }
        public ICommand AddCommand { get; }
        public ICommand DeleteCommand { get; }
        public ICommand EditCommand { get; }

        public ViewClustersViewModel(IKafkaService kafkaService, ClusterStore clusterStore, IEnumerable<KafkaCluster> existing)
        {
            _kafkaService = kafkaService;
            _clusterStore = clusterStore;
            Clusters = new ObservableCollection<KafkaCluster>(existing);
            ConnectCommand = new RelayCommand(param => Connect(param as KafkaCluster), _ => true);
            AddCommand = new RelayCommand(_ => AddNewRequested?.Invoke());
            DeleteCommand = new RelayCommand(DeleteCluster);
            EditCommand = new RelayCommand(param => { if (param is KafkaCluster c) EditRequested?.Invoke(c); });
        }

        private void Connect(KafkaCluster? cluster = null)
        {
            var target = cluster ?? SelectedCluster;
            if (target == null) return;
            ClusterConnected?.Invoke(target);
        }

        private void DeleteCluster(object? param)
        {
            var cluster = param as KafkaCluster ?? SelectedCluster;
            if (cluster == null) return;
            _clusterStore.DeleteCluster(cluster.Id);
            Clusters.Remove(cluster);
            if (SelectedCluster?.Id == cluster.Id) SelectedCluster = null;
        }

        public void AddCluster(KafkaCluster cluster)
        {
            Clusters.Add(cluster);
        }

        public void RemoveCluster(KafkaCluster cluster)
        {
            Clusters.Remove(cluster);
        }

        public void UpdateCluster(KafkaCluster updated)
        {
            var existing = Clusters.FirstOrDefault(c => c.Id == updated.Id);
            if (existing != null)
            {
                var idx = Clusters.IndexOf(existing);
                Clusters[idx] = updated;
            }
        }
    }

    // ─── Create Topic Dialog ──────────────────────────────────────────────────
    public class CustomParameter : ViewModelBase
    {
        private string _key = string.Empty;
        private string _value = string.Empty;
        public string Key   { get => _key;   set => SetField(ref _key, value); }
        public string Value { get => _value; set => SetField(ref _value, value); }
    }

    public class CreateTopicViewModel : ViewModelBase
    {
        private string _name = string.Empty;
        private int _partitions = 1;
        private short _replicationFactor = 1;
        private int _minInSyncReplicas = 1;
        private long _retentionMs = 604800000;
        private string _cleanupPolicy = "Delete";
        private string _maxSizeOnDisk = "Not Set";
        private string _maxMessageBytes = string.Empty;

        public event Action<TopicCreationSpec>? TopicCreated;
        public event Action? Cancelled;

        public string Name                { get => _name;               set { SetField(ref _name, value); OnPropertyChanged(nameof(CanCreate)); } }
        public int    Partitions          { get => _partitions;         set => SetField(ref _partitions, value); }
        public short  ReplicationFactor   { get => _replicationFactor;  set => SetField(ref _replicationFactor, value); }
        public int    MinInSyncReplicas   { get => _minInSyncReplicas;  set => SetField(ref _minInSyncReplicas, value); }
        public long   RetentionMs         { get => _retentionMs;        set { SetField(ref _retentionMs, value); OnPropertyChanged(nameof(RetentionDisplay)); } }
        public string CleanupPolicy       { get => _cleanupPolicy;      set => SetField(ref _cleanupPolicy, value); }
        public string MaxSizeOnDisk       { get => _maxSizeOnDisk;      set => SetField(ref _maxSizeOnDisk, value); }
        public string MaxMessageBytes     { get => _maxMessageBytes;    set => SetField(ref _maxMessageBytes, value); }

        public bool CanCreate => !string.IsNullOrWhiteSpace(Name);

        public string RetentionDisplay => RetentionMs switch
        {
            43200000  => "12h",
            86400000  => "1d",
            172800000 => "2d",
            604800000 => "7d",
            2419200000 => "4w",
            _ => $"{RetentionMs}ms"
        };

        public ObservableCollection<CustomParameter> CustomParameters { get; } = new();

        public IEnumerable<string> CleanupPolicies  => new[] { "Delete", "Compact", "Compact,Delete" };
        public IEnumerable<string> MaxSizeOptions   => new[] { "Not Set", "1 GB", "5 GB", "10 GB", "50 GB", "100 GB" };

        public ICommand CreateCommand    { get; }
        public ICommand CancelCommand    { get; }
        public ICommand SetRetentionCommand { get; }
        public ICommand AddCustomParamCommand { get; }
        public ICommand RemoveCustomParamCommand { get; }

        public CreateTopicViewModel()
        {
            CreateCommand           = new RelayCommand(_ => Create(), _ => CanCreate);
            CancelCommand           = new RelayCommand(_ => Cancelled?.Invoke());
            SetRetentionCommand     = new RelayCommand(p => { if (long.TryParse(p?.ToString(), out var v)) RetentionMs = v; });
            AddCustomParamCommand   = new RelayCommand(_ => CustomParameters.Add(new CustomParameter()));
            RemoveCustomParamCommand = new RelayCommand(p => { if (p is CustomParameter cp) CustomParameters.Remove(cp); });
        }

        private void Create()
        {
            var config = new Dictionary<string, string>
            {
                ["cleanup.policy"] = CleanupPolicy.ToLower(),
                ["retention.ms"]   = RetentionMs.ToString(),
            };
            if (!string.IsNullOrWhiteSpace(MaxMessageBytes) && long.TryParse(MaxMessageBytes, out _))
                config["max.message.bytes"] = MaxMessageBytes;
            foreach (var p in CustomParameters)
                if (!string.IsNullOrWhiteSpace(p.Key))
                    config[p.Key] = p.Value;
            TopicCreated?.Invoke(new TopicCreationSpec(Name, Partitions, ReplicationFactor, config));
        }
    }

    // ─── Produce Message Dialog ───────────────────────────────────────────────
    public record ProduceMessageArgs(string? Key, string Value, Dictionary<string, string>? Headers, int? Partition);

    public class ProduceMessageViewModel : ViewModelBase
    {
        private string? _key;
        private string _value = string.Empty;
        private string _headersText = "{}";
        private string _partitionText = "Partition #0";
        private string _keySerde = "String";
        private string _valueSerde = "String";
        private bool   _keepContents;

        public event Action<ProduceMessageArgs>? MessageProduced;
        public event Action? Cancelled;

        public string? Key          { get => _key;           set => SetField(ref _key, value); }
        public string  Value        { get => _value;         set => SetField(ref _value, value); }
        public string  HeadersText  { get => _headersText;   set => SetField(ref _headersText, value); }
        public string  PartitionText{ get => _partitionText; set => SetField(ref _partitionText, value); }
        public string  KeySerde     { get => _keySerde;      set => SetField(ref _keySerde, value); }
        public string  ValueSerde   { get => _valueSerde;    set => SetField(ref _valueSerde, value); }
        public bool    KeepContents { get => _keepContents;  set => SetField(ref _keepContents, value); }

        public IEnumerable<string> PartitionOptions => Enumerable.Range(0, 10).Select(i => $"Partition #{i}");
        public IEnumerable<string> SerdeOptions     => new[] { "String", "JSON", "Avro", "Protobuf", "Long", "Int" };

        public ICommand ProduceCommand { get; }
        public ICommand CancelCommand  { get; }

        public ProduceMessageViewModel()
        {
            ProduceCommand = new RelayCommand(Produce, () => !string.IsNullOrWhiteSpace(Value));
            CancelCommand  = new RelayCommand(_ => Cancelled?.Invoke());
        }

        private void Produce()
        {
            int? partition = int.TryParse(PartitionText, out var p) ? p : null;
            Dictionary<string, string>? headers = null;
            if (!string.IsNullOrWhiteSpace(HeadersText))
            {
                headers = new();
                foreach (var line in HeadersText.Split('\n'))
                {
                    var parts = line.Split(':', 2);
                    if (parts.Length == 2) headers[parts[0].Trim()] = parts[1].Trim();
                }
            }
            MessageProduced?.Invoke(new ProduceMessageArgs(string.IsNullOrWhiteSpace(Key) ? null : Key, Value, headers, partition));
            if (!KeepContents) { Key = null; Value = string.Empty; HeadersText = "{}"; }
        }
    }
}
