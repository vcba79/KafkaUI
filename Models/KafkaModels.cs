using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace KafkaUI.Models
{
    public class KafkaCluster : INotifyPropertyChanged
    {
        private bool _isDefault;

        public event PropertyChangedEventHandler? PropertyChanged;
        private void OnPropertyChanged([CallerMemberName] string? name = null)
            => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));

        public string Id { get; set; } = Guid.NewGuid().ToString();
        public string Name { get; set; } = string.Empty;
        public string BootstrapServers { get; set; } = string.Empty;
        public bool IsConnected { get; set; }
        public int BrokerCount { get; set; }
        public int TopicCount { get; set; }
        public long MessagesPerSecond { get; set; }
        public string? SchemaRegistryUrl { get; set; }
        public string? SchemaRegistryUsername { get; set; }
        public string? SchemaRegistryPassword { get; set; }
        public string? KafkaConnectUrl { get; set; }

        public bool IsDefault
        {
            get => _isDefault;
            set { if (_isDefault == value) return; _isDefault = value; OnPropertyChanged(); }
        }
    }

    public class KafkaBroker
    {
        public int Id { get; set; }
        public string Host { get; set; } = string.Empty;
        public int Port { get; set; }
        public bool IsController { get; set; }
        public int PartitionCount { get; set; }
        public int LeaderCount { get; set; }
        public long BytesIn { get; set; }
        public long BytesOut { get; set; }
        public long DiskUsageBytes { get; set; }
        public int SegmentCount { get; set; }
        public int OnlinePartitions { get; set; }
        public string? PartitionsSkew { get; set; }
        public string? LeaderSkew { get; set; }
        public string Address => $"{Host}:{Port}";
        public string DiskUsageFormatted => SegmentCount > 0
            ? $"{FormatBytes(DiskUsageBytes)}, {SegmentCount} segment(s)"
            : FormatBytes(DiskUsageBytes);

        private static string FormatBytes(long b) => b switch
        {
            < 1024 => $"{b} B",
            < 1024 * 1024 => $"{b / 1024.0:F1} KB",
            < 1024L * 1024 * 1024 => $"{b / 1024.0 / 1024:F1} MB",
            _ => $"{b / 1024.0 / 1024 / 1024:F2} GB"
        };
    }

    public class KafkaClusterStats
    {
        public int BrokerCount { get; set; }
        public int ActiveController { get; set; }
        public string Version { get; set; } = string.Empty;
        public int OnlinePartitions { get; set; }
        public int TotalPartitions { get; set; }
        public int UnderReplicatedPartitions { get; set; }
        public int InSyncReplicas { get; set; }
        public int TotalReplicas { get; set; }
        public int OutOfSyncReplicas { get; set; }
    }

    public class BrokerConfigEntry : System.ComponentModel.INotifyPropertyChanged
    {
        public string Key { get; set; } = string.Empty;

        private string _value = string.Empty;
        public string Value
        {
            get => _value;
            set { _value = value; OnPropertyChanged(); }
        }

        private string _editValue = string.Empty;
        public string EditValue
        {
            get => _editValue;
            set { _editValue = value; OnPropertyChanged(); }
        }

        private bool _isEditing;
        public bool IsEditing
        {
            get => _isEditing;
            set { _isEditing = value; OnPropertyChanged(); OnPropertyChanged(nameof(IsNotEditing)); }
        }

        public bool IsNotEditing => !_isEditing;

        public event System.ComponentModel.PropertyChangedEventHandler? PropertyChanged;
        private void OnPropertyChanged([System.Runtime.CompilerServices.CallerMemberName] string? name = null)
            => PropertyChanged?.Invoke(this, new System.ComponentModel.PropertyChangedEventArgs(name));
    }

    public class TopicPartitionStats
    {
        public int PartitionId { get; set; }
        public long TotalMessages { get; set; }
        public long MinOffset { get; set; }
        public long MaxOffset { get; set; }
    }

    public class TopicStatistics
    {
        public DateTime AnalyzedAt { get; set; }
        public long TotalMessages { get; set; }
        public long? MinOffset { get; set; }
        public long? MaxOffset { get; set; }
        public DateTime? MinTimestamp { get; set; }
        public DateTime? MaxTimestamp { get; set; }
        public long NullKeys { get; set; }
        public long UniqueKeys { get; set; }
        public long NullValues { get; set; }
        public long UniqueValues { get; set; }
        public List<TopicPartitionStats> Partitions { get; set; } = new();

        public string OffsetsDisplay => MinOffset.HasValue && MaxOffset.HasValue
            ? $"{MinOffset} - {MaxOffset}"
            : "undefined - undefined";

        public string TimestampDisplay => MinTimestamp.HasValue && MaxTimestamp.HasValue
            ? $"{MinTimestamp:M/d/yyyy, HH:mm:ss} - {MaxTimestamp:M/d/yyyy, HH:mm:ss}"
            : "-";

        public string AnalyzedAtDisplay => AnalyzedAt.ToString("M/d/yyyy, HH:mm:ss");
    }

    public class TopicConfigEntry
    {
        public string Key { get; set; } = string.Empty;
        public string Value { get; set; } = string.Empty;
        public string? DefaultValue { get; set; }
    }

    public class KafkaBrokerDetail
    {
        public int Id { get; set; }
        public string Host { get; set; } = string.Empty;
        public int Port { get; set; }
        public Dictionary<string, string> Configs { get; set; } = new();
        public List<BrokerConfigEntry> ConfigEntries { get; set; } = new();
    }

    public class KafkaTopic : System.ComponentModel.INotifyPropertyChanged
    {
        public string Name { get; set; } = string.Empty;
        public int Partitions { get; set; }
        public int ReplicationFactor { get; set; }
        public long MessageCount { get; set; }
        public long Size { get; set; }
        public bool IsInternal { get; set; }

        // Checked state for bulk-action selection (not persisted)
        private bool _isChecked;
        public bool IsChecked
        {
            get => _isChecked;
            set { _isChecked = value; PropertyChanged?.Invoke(this, new System.ComponentModel.PropertyChangedEventArgs(nameof(IsChecked))); }
        }
        public event System.ComponentModel.PropertyChangedEventHandler? PropertyChanged;
        public int OutOfSyncReplicas { get; set; }
        public bool CleanupPolicyCompact { get; set; }
        public string CleanupPolicy { get; set; } = "delete";
        public long RetentionMs { get; set; } = 604800000;
        public List<KafkaPartition> PartitionList { get; set; } = new();
        public Dictionary<string, string> Config { get; set; } = new();
        public List<TopicConfigEntry> ConfigEntries { get; set; } = new();
        public long SegmentSize { get; set; }
        public int SegmentCount { get; set; }
        public int UnderReplicatedPartitions => PartitionList.Count(p => p.IsUnderReplicated);
        public int InSyncReplicaCount => PartitionList.Sum(p => p.InSyncReplicas.Count);
        public string TypeLabel => IsInternal ? "Internal" : "External";
        public string SizeFormatted => FormatBytes(Size);
        public string SegmentSizeFormatted => FormatBytes(SegmentSize);
        public string ReplicationStatus => ReplicationFactor > 1 ? "In Sync" : "Single";

        private static string FormatBytes(long bytes)
        {
            string[] sizes = { "B", "KB", "MB", "GB", "TB" };
            double d = bytes;
            int order = 0;
            while (d >= 1024 && order < sizes.Length - 1) { order++; d /= 1024; }
            return $"{d:0.##} {sizes[order]}";
        }
    }

    public class KafkaPartition
    {
        public int Id { get; set; }
        public int Leader { get; set; }
        public List<int> Replicas { get; set; } = new();
        public List<int> InSyncReplicas { get; set; } = new();
        public long OffsetEarliest { get; set; }
        public long OffsetLatest { get; set; }
        public long MessageCount => OffsetLatest - OffsetEarliest;
        public bool IsUnderReplicated => Replicas.Count != InSyncReplicas.Count;
    }

    public class KafkaConsumerGroup
    {
        public string GroupId { get; set; } = string.Empty;
        public string State { get; set; } = string.Empty;
        public string ProtocolType { get; set; } = string.Empty;
        public int MemberCount { get; set; }
        public int NumOfTopics { get; set; }
        public int Coordinator { get; set; }
        public long TotalLag { get; set; }
        public List<KafkaConsumerGroupMember> Members { get; set; } = new();
        public List<KafkaConsumerGroupOffset> Offsets { get; set; } = new();
    }

    public class KafkaConsumerGroupMember
    {
        public string MemberId { get; set; } = string.Empty;
        public string ClientId { get; set; } = string.Empty;
        public string Host { get; set; } = string.Empty;
        public List<string> Assignments { get; set; } = new();
    }

    public class KafkaConsumerGroupOffset
    {
        public string Topic { get; set; } = string.Empty;
        public int Partition { get; set; }
        public long CurrentOffset { get; set; }
        public long EndOffset { get; set; }
        public long Lag => EndOffset - CurrentOffset;
    }

    public class KafkaMessage
    {
        public int Partition { get; set; }
        public long Offset { get; set; }
        public DateTime Timestamp { get; set; }
        public string? Key { get; set; }
        public string? Value { get; set; }
        public Dictionary<string, string> Headers { get; set; } = new();
        public string TimestampFormatted => Timestamp.ToString("M/d/yyyy, HH:mm:ss");
        public string KeyDisplay => Key ?? "(null)";
        public string ValuePreview => Value?.Length > 100 ? Value[..100] + "..." : Value ?? "(null)";
    }

    public class KafkaSchema
    {
        public int Id { get; set; }
        public string Subject { get; set; } = string.Empty;
        public int Version { get; set; }
        public string SchemaType { get; set; } = "AVRO";
        public string Schema { get; set; } = string.Empty;
        public string CompatibilityLevel { get; set; } = "BACKWARD";
    }

    public class KafkaConnector
    {
        public string Name { get; set; } = string.Empty;
        public string ConnectorClass { get; set; } = string.Empty;
        public string Status { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty;
        public int TasksTotal { get; set; }
        public int TasksRunning { get; set; }
        public Dictionary<string, string> Config { get; set; } = new();
    }

    public class ClusterStats
    {
        public long MessagesIn { get; set; }
        public long MessagesOut { get; set; }
        public long BytesIn { get; set; }
        public long BytesOut { get; set; }
        public int ActiveControllerCount { get; set; }
        public int OfflinePartitionsCount { get; set; }
        public int UnderReplicatedPartitions { get; set; }
        public List<TimeSeriesPoint> ThroughputHistory { get; set; } = new();
    }

    public class TimeSeriesPoint
    {
        public DateTime Time { get; set; }
        public double Value { get; set; }
    }
}
