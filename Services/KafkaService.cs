using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaUI.Models;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaUI.Services
{
    public interface IKafkaService
    {
        Task<bool> TestConnectionAsync(string bootstrapServers);
        Task<List<KafkaBroker>> GetBrokersAsync(string bootstrapServers);
        Task<KafkaBrokerDetail> GetBrokerDetailAsync(string bootstrapServers, int brokerId);
        Task<List<KafkaTopic>> GetTopicsAsync(string bootstrapServers, bool includeInternal = false);
        Task<KafkaTopic> GetTopicDetailsAsync(string bootstrapServers, string topicName);
        Task CreateTopicAsync(string bootstrapServers, string topicName, int partitions, short replicationFactor, Dictionary<string, string>? config = null);
        Task DeleteTopicAsync(string bootstrapServers, string topicName);
        Task RecreateTopicAsync(string bootstrapServers, string topicName);
        Task ClearTopicMessagesAsync(string bootstrapServers, string topicName);
        Task ClearPartitionMessagesAsync(string bootstrapServers, string topicName, int partitionId);
        Task<List<KafkaConsumerGroup>> GetConsumerGroupsAsync(string bootstrapServers);
        Task<List<KafkaConsumerGroup>> GetConsumerGroupsForTopicAsync(string bootstrapServers, string topicName);
        Task<KafkaConsumerGroup> GetConsumerGroupDetailsAsync(string bootstrapServers, string groupId);
        Task<List<KafkaMessage>> GetMessagesAsync(string bootstrapServers, string topicName, int partition, long offset, int count, CancellationToken ct = default, DateTime? seekTimestamp = null);
        Task<TopicStatistics> AnalyzeTopicAsync(string bootstrapServers, string topicName, CancellationToken ct = default);
        Task ProduceMessageAsync(string bootstrapServers, string topicName, string? key, string value, Dictionary<string, string>? headers = null, int? partition = null);
        Task<ClusterStats> GetClusterStatsAsync(string bootstrapServers);
        Task UpdateBrokerConfigAsync(string bootstrapServers, int brokerId, string key, string value);
    }

    public class KafkaService : IKafkaService
    {
        private readonly Dictionary<string, IAdminClient> _adminClients = new();

        private IAdminClient GetAdminClient(string bootstrapServers)
        {
            if (!_adminClients.TryGetValue(bootstrapServers, out var client))
            {
                var config = new AdminClientConfig { BootstrapServers = bootstrapServers };
                client = new AdminClientBuilder(config).Build();
                _adminClients[bootstrapServers] = client;
            }
            return client;
        }

        // Well-known Apache Kafka topic-level config defaults, used to populate the
        // "Default Value" column in the topic Settings tab (matches kafka-ui's display).
        private static readonly Dictionary<string, string> _topicConfigDefaults = new()
        {
            ["cleanup.policy"] = "delete",
            ["compression.type"] = "producer",
            ["compression.gzip.level"] = "-1",
            ["compression.lz4.level"] = "9",
            ["compression.zstd.level"] = "3",
            ["delete.retention.ms"] = "86400000",
            ["file.delete.delay.ms"] = "60000",
            ["flush.messages"] = "9223372036854775807",
            ["flush.ms"] = "9223372036854775807",
            ["follower.replication.throttled.replicas"] = "",
            ["index.interval.bytes"] = "4096",
            ["leader.replication.throttled.replicas"] = "",
            ["local.retention.bytes"] = "-2",
            ["local.retention.ms"] = "-2",
            ["max.compaction.lag.ms"] = "9223372036854775807",
            ["max.message.bytes"] = "1048588",
            ["message.format.version"] = "3.0-IV1",
            ["message.timestamp.difference.max.ms"] = "9223372036854775807",
            ["message.timestamp.type"] = "CreateTime",
            ["min.cleanable.dirty.ratio"] = "0.5",
            ["min.compaction.lag.ms"] = "0",
            ["min.insync.replicas"] = "1",
            ["preallocate"] = "false",
            ["remote.log.copy.disable"] = "false",
            ["remote.log.delete.on.disable"] = "false",
            ["remote.storage.enable"] = "false",
            ["retention.bytes"] = "-1",
            ["retention.ms"] = "604800000",
            ["segment.bytes"] = "1073741824",
            ["segment.index.bytes"] = "10485760",
            ["segment.jitter.ms"] = "0",
            ["segment.ms"] = "604800000",
            ["unclean.leader.election.enable"] = "false",
        };

        private static string? LookupDefaultTopicConfig(string key) =>
            _topicConfigDefaults.TryGetValue(key, out var v) ? v : null;

        public async Task<bool> TestConnectionAsync(string bootstrapServers)
        {
            try
            {
                var admin = GetAdminClient(bootstrapServers);
                var meta = admin.GetMetadata(TimeSpan.FromSeconds(5));
                return meta.Brokers.Count > 0;
            }
            catch
            {
                return false;
            }
        }

        public async Task<List<KafkaBroker>> GetBrokersAsync(string bootstrapServers)
        {
            var admin = GetAdminClient(bootstrapServers);
            var meta = admin.GetMetadata(TimeSpan.FromSeconds(10));

            int controllerId = -1;
            try
            {
                var clusterDesc = await admin.DescribeClusterAsync();
                controllerId = clusterDesc.Controller?.Id ?? -1;
            }
            catch { /* DescribeCluster unsupported on older brokers — leave unknown */ }

            return meta.Brokers.Select(b => new KafkaBroker
            {
                Id = b.BrokerId,
                Host = b.Host,
                Port = b.Port,
                IsController = b.BrokerId == controllerId,
                PartitionCount = meta.Topics.SelectMany(t => t.Partitions).Count(p => p.Replicas.Contains(b.BrokerId)),
                LeaderCount = meta.Topics.SelectMany(t => t.Partitions).Count(p => p.Leader == b.BrokerId),
                // A partition this broker replicates is "online" if it currently has a live leader assigned.
                OnlinePartitions = meta.Topics.SelectMany(t => t.Partitions)
                    .Count(p => p.Replicas.Contains(b.BrokerId) && p.Leader >= 0)
            }).ToList();
        }

        public async Task<KafkaBrokerDetail> GetBrokerDetailAsync(string bootstrapServers, int brokerId)
        {
            var admin = GetAdminClient(bootstrapServers);
            var meta = admin.GetMetadata(TimeSpan.FromSeconds(10));
            var broker = meta.Brokers.FirstOrDefault(b => b.BrokerId == brokerId);

            // NOTE: Log Directories tab was removed — it relied on the Java admin client's
            // DescribeLogDirs API (real on-disk paths and segment sizes), which Confluent.Kafka's
            // .NET client does not expose. Rather than show fabricated paths/sizes, this data
            // is simply not surfaced.
            //
            // NOTE: The Metrics tab was likewise removed — the original kafka-ui's Metrics tab
            // is entirely JMX-backed (BytesInPerSec, RequestHandlerAvgIdlePercent, etc.), and
            // JMX has no .NET client equivalent without a separate bridge/sidecar.
            return new KafkaBrokerDetail
            {
                Id = brokerId,
                Host = broker?.Host ?? string.Empty,
                Port = broker?.Port ?? 0,
                Configs = await FetchBrokerConfigsAsync(admin, brokerId),
                ConfigEntries = (await FetchBrokerConfigsAsync(admin, brokerId))
                    .Select(kv => new BrokerConfigEntry { Key = kv.Key, Value = kv.Value, EditValue = kv.Value })
                    .ToList()
            };
        }

        public async Task UpdateBrokerConfigAsync(string bootstrapServers, int brokerId, string key, string value)
        {
            var admin = GetAdminClient(bootstrapServers);
            var configEntry = new ConfigEntry { Name = key, Value = value };
            var configResource = new ConfigResource { Name = brokerId.ToString(), Type = ResourceType.Broker };
            await admin.AlterConfigsAsync(new Dictionary<ConfigResource, List<ConfigEntry>>
            {
                [configResource] = new List<ConfigEntry> { configEntry }
            });
        }

        private static async Task<Dictionary<string, string>> FetchBrokerConfigsAsync(
            Confluent.Kafka.IAdminClient admin, int brokerId)
        {
            try
            {
                var result = await admin.DescribeConfigsAsync(new[]
                {
                    new ConfigResource { Name = brokerId.ToString(), Type = ResourceType.Broker }
                });
                return result.First<DescribeConfigsResult>().Entries
                    .OrderBy(e => e.Key)
                    .ToDictionary(e => e.Key, e => e.Value.Value ?? "");
            }
            catch
            {
                return new Dictionary<string, string>();
            }
        }

        public async Task<List<KafkaTopic>> GetTopicsAsync(string bootstrapServers, bool includeInternal = false)
        {
            var admin = GetAdminClient(bootstrapServers);
            var meta = admin.GetMetadata(TimeSpan.FromSeconds(10));
            var topics = meta.Topics
                .Where(t => includeInternal || !t.Topic.StartsWith("__"))
                .Select(t => new KafkaTopic
                {
                    Name = t.Topic,
                    Partitions = t.Partitions.Count,
                    ReplicationFactor = t.Partitions.FirstOrDefault()?.Replicas.Length ?? 1,
                    IsInternal = t.Topic.StartsWith("__"),
                    PartitionList = t.Partitions.Select(p => new KafkaPartition
                    {
                        Id = p.PartitionId,
                        Leader = p.Leader,
                        Replicas = p.Replicas.ToList(),
                        InSyncReplicas = p.InSyncReplicas.ToList()
                    }).ToList()
                }).ToList();

            // Enrich with offsets for a sample
            foreach (var topic in topics.Take(50))
            {
                try
                {
                    var consumerConfig = new ConsumerConfig
                    {
                        BootstrapServers = bootstrapServers,
                        GroupId = $"kafka-ui-introspect-{Guid.NewGuid():N}",
                        AutoOffsetReset = AutoOffsetReset.Latest
                    };
                    using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();
                    var tps = topic.PartitionList.Select(p => new TopicPartition(topic.Name, p.Id)).ToList();
                    var endOffsets = consumer.QueryWatermarkOffsets(tps[0], TimeSpan.FromSeconds(2));
                    topic.MessageCount = endOffsets.High.Value;
                }
                catch { }
            }

            return topics;
        }

        public async Task<KafkaTopic> GetTopicDetailsAsync(string bootstrapServers, string topicName)
        {
            var admin = GetAdminClient(bootstrapServers);
            var meta = admin.GetMetadata(topicName, TimeSpan.FromSeconds(10));
            var topicMeta = meta.Topics.First();

            var topic = new KafkaTopic
            {
                Name = topicName,
                Partitions = topicMeta.Partitions.Count,
                ReplicationFactor = topicMeta.Partitions.FirstOrDefault()?.Replicas.Length ?? 1,
                IsInternal = topicName.StartsWith("__"),
                PartitionList = topicMeta.Partitions.Select(p => new KafkaPartition
                {
                    Id = p.PartitionId,
                    Leader = p.Leader,
                    Replicas = p.Replicas.ToList(),
                    InSyncReplicas = p.InSyncReplicas.ToList()
                }).ToList()
            };

            // Get offsets for each partition
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"kafka-ui-introspect-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Latest
            };
            using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();
            long totalMessages = 0;
            foreach (var partition in topic.PartitionList)
            {
                try
                {
                    var tp = new TopicPartition(topicName, partition.Id);
                    var offsets = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(2));
                    partition.OffsetEarliest = offsets.Low.Value;
                    partition.OffsetLatest = offsets.High.Value;
                    totalMessages += partition.MessageCount;
                }
                catch { }
            }
            topic.MessageCount = totalMessages;

            // Get topic config — show ALL entries (default or not), matching the original
            // kafka-ui Settings tab, with a separate Default Value column.
            try
            {
                var configResult = await admin.DescribeConfigsAsync(new[]
                {
                    new ConfigResource { Name = topicName, Type = ResourceType.Topic }
                });
                var entries = configResult.First<DescribeConfigsResult>().Entries;

                topic.Config = entries.Where(e => !e.Value.IsDefault)
                    .ToDictionary(e => e.Key, e => e.Value.Value ?? "");

                topic.ConfigEntries = entries
                    .OrderBy(e => e.Key)
                    .Select(e => new TopicConfigEntry
                    {
                        Key = e.Key,
                        Value = e.Value.Value ?? "",
                        DefaultValue = e.Value.IsDefault ? null : LookupDefaultTopicConfig(e.Key)
                    })
                    .ToList();

                if (topic.Config.TryGetValue("cleanup.policy", out var cp)) topic.CleanupPolicy = cp;
                if (topic.Config.TryGetValue("retention.ms", out var rm) && long.TryParse(rm, out var retMs)) topic.RetentionMs = retMs;
            }
            catch { }

            return topic;
        }

        public async Task CreateTopicAsync(string bootstrapServers, string topicName, int partitions, short replicationFactor, Dictionary<string, string>? config = null)
        {
            var admin = GetAdminClient(bootstrapServers);
            var spec = new TopicSpecification
            {
                Name = topicName,
                NumPartitions = partitions,
                ReplicationFactor = replicationFactor,
                Configs = config ?? new Dictionary<string, string>()
            };
            await admin.CreateTopicsAsync(new[] { spec });
        }

        public async Task RecreateTopicAsync(string bootstrapServers, string topicName)
        {
            var admin = GetAdminClient(bootstrapServers);
            var meta = admin.GetMetadata(TimeSpan.FromSeconds(10));
            var topicMeta = meta.Topics.FirstOrDefault(t => t.Topic == topicName);
            if (topicMeta == null) throw new Exception($"Topic '{topicName}' not found.");

            int partitions = topicMeta.Partitions.Count;
            short replicationFactor = (short)(topicMeta.Partitions.FirstOrDefault()?.Replicas.Length ?? 1);

            // Preserve config
            Dictionary<string, string> config;
            try
            {
                var configResult = await admin.DescribeConfigsAsync(new[]
                {
                    new ConfigResource { Name = topicName, Type = ResourceType.Topic }
                });
                config = configResult.First<DescribeConfigsResult>().Entries
                    .Where(e => !e.Value.IsDefault)
                    .ToDictionary(e => e.Key, e => e.Value.Value ?? "");
            }
            catch { config = new Dictionary<string, string>(); }

            await DeleteTopicAsync(bootstrapServers, topicName); // polls until broker confirms deletion
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = partitions,
                    ReplicationFactor = replicationFactor,
                    Configs = config
                }
            });
        }

        public async Task ClearTopicMessagesAsync(string bootstrapServers, string topicName)
        {
            var admin = GetAdminClient(bootstrapServers);
            var meta = admin.GetMetadata(TimeSpan.FromSeconds(10));
            var topicMeta = meta.Topics.FirstOrDefault(t => t.Topic == topicName);
            if (topicMeta == null) throw new Exception($"Topic '{topicName}' not found.");

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"kafka-ui-purge-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Latest
            };
            using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();

            var deleteRecords = new List<TopicPartitionOffset>();
            foreach (var p in topicMeta.Partitions)
            {
                var tp = new TopicPartition(topicName, p.PartitionId);
                var watermarks = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
                deleteRecords.Add(new TopicPartitionOffset(tp, watermarks.High));
            }

            await admin.DeleteRecordsAsync(deleteRecords);
        }

        public async Task ClearPartitionMessagesAsync(string bootstrapServers, string topicName, int partitionId)
        {
            var admin = GetAdminClient(bootstrapServers);
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"kafka-ui-purge-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Latest
            };
            using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();
            var tp = new TopicPartition(topicName, partitionId);
            var watermarks = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
            await admin.DeleteRecordsAsync(new[] { new TopicPartitionOffset(tp, watermarks.High) });
        }

        public async Task DeleteTopicAsync(string bootstrapServers, string topicName)
        {
            var admin = GetAdminClient(bootstrapServers);
            await admin.DeleteTopicsAsync(new[] { topicName });

            // librdkafka / the broker can take a moment to propagate the deletion.
            // Poll metadata (forcing a fresh broker round-trip each time) until the
            // topic is actually gone, instead of assuming a fixed delay is enough.
            for (int attempt = 0; attempt < 10; attempt++)
            {
                await Task.Delay(500);
                try
                {
                    var meta = admin.GetMetadata(TimeSpan.FromSeconds(5));
                    bool stillExists = meta.Topics.Any(t => t.Topic == topicName && t.Error.Code == ErrorCode.NoError);
                    if (!stillExists) return;
                }
                catch { /* keep retrying */ }
            }
        }

        public async Task<List<KafkaConsumerGroup>> GetConsumerGroupsAsync(string bootstrapServers)
        {
            var admin = GetAdminClient(bootstrapServers);
            var groups = await admin.ListConsumerGroupsAsync();
            var result = new List<KafkaConsumerGroup>();
            foreach (var g in groups.Valid)
            {
                result.Add(new KafkaConsumerGroup
                {
                    GroupId = g.GroupId,
                    State = g.State.ToString(),
                    ProtocolType = g.IsSimpleConsumerGroup ? "simple" : "consumer"
                });
            }
            return result;
        }

        public async Task<List<KafkaConsumerGroup>> GetConsumerGroupsForTopicAsync(string bootstrapServers, string topicName)
        {
            var admin = GetAdminClient(bootstrapServers);
            var groups = await admin.ListConsumerGroupsAsync();
            var result = new List<KafkaConsumerGroup>();

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"kafka-ui-introspect-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Latest
            };
            using var watermarkConsumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();

            foreach (var g in groups.Valid)
            {
                try
                {
                    var describeResult = await admin.DescribeConsumerGroupsAsync(new[] { g.GroupId });
                    var desc = describeResult.ConsumerGroupDescriptions[0];

                    bool consumesTopic = desc.Members.Any(m =>
                        m.Assignment.TopicPartitions.Any(tp => tp.Topic == topicName));
                    if (!consumesTopic) continue;

                    long lag = 0;
                    try
                    {
                        var offsets = await admin.ListConsumerGroupOffsetsAsync(new[]
                        {
                            new ConsumerGroupTopicPartitions(g.GroupId, new List<TopicPartition>())
                        });
                        var groupOffsets = offsets.FirstOrDefault(o => o.Group == g.GroupId);
                        if (groupOffsets != null)
                        {
                            foreach (var tpo in groupOffsets.Partitions.Where(p => p.Topic == topicName))
                            {
                                var watermarks = watermarkConsumer.QueryWatermarkOffsets(
                                    new TopicPartition(tpo.Topic, tpo.Partition), TimeSpan.FromSeconds(5));
                                lag += Math.Max(0, watermarks.High.Value - tpo.Offset.Value);
                            }
                        }
                    }
                    catch { /* lag calc best-effort */ }

                    result.Add(new KafkaConsumerGroup
                    {
                        GroupId = g.GroupId,
                        State = desc.State.ToString(),
                        ProtocolType = desc.PartitionAssignor,
                        MemberCount = desc.Members.Count,
                        Coordinator = desc.Coordinator?.Id ?? 0,
                        TotalLag = lag
                    });
                }
                catch { /* skip groups that fail to describe */ }
            }
            return result;
        }

        public async Task<KafkaConsumerGroup> GetConsumerGroupDetailsAsync(string bootstrapServers, string groupId)
        {
            var admin = GetAdminClient(bootstrapServers);
            var describeResult = await admin.DescribeConsumerGroupsAsync(new[] { groupId });
            var g = describeResult.ConsumerGroupDescriptions[0];
            var group = new KafkaConsumerGroup
            {
                GroupId = g.GroupId,
                State = g.State.ToString(),
                ProtocolType = g.PartitionAssignor,
                MemberCount = g.Members.Count,
                Members = g.Members.Select(m => new KafkaConsumerGroupMember
                {
                    MemberId = m.ConsumerId,
                    ClientId = m.ClientId,
                    Host = m.Host,
                    Assignments = m.Assignment.TopicPartitions.Select(tp => $"{tp.Topic}:{tp.Partition.Value}").ToList()
                }).ToList()
            };

            // Get lag
            try
            {
                // Fix #1: ConsumerGroupTopicPartitions requires (string groupId, List<TopicPartition> partitions)
                // Pass empty list to request offsets for all assigned partitions
                var offsets = await admin.ListConsumerGroupOffsetsAsync(new[] {
                    new ConsumerGroupTopicPartitions(groupId, new List<TopicPartition>())
                });
                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = $"kafka-ui-lag-check-{Guid.NewGuid():N}",
                    AutoOffsetReset = AutoOffsetReset.Latest
                };
                using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();
                // Fix #2: ImmutableArray.First() requires explicit type argument
                foreach (var o in offsets[0].Partitions)
                {
                    var watermark = consumer.QueryWatermarkOffsets(o.TopicPartition, TimeSpan.FromSeconds(2));
                    group.Offsets.Add(new KafkaConsumerGroupOffset
                    {
                        Topic = o.TopicPartition.Topic,
                        Partition = o.TopicPartition.Partition.Value,
                        CurrentOffset = o.Offset.Value,
                        EndOffset = watermark.High.Value
                    });
                }
                group.TotalLag = group.Offsets.Sum(x => x.Lag);
            }
            catch { }

            return group;
        }

        public async Task<TopicStatistics> AnalyzeTopicAsync(string bootstrapServers, string topicName, CancellationToken ct = default)
        {
            var admin = GetAdminClient(bootstrapServers);
            var meta = admin.GetMetadata(TimeSpan.FromSeconds(10));
            var topicMeta = meta.Topics.FirstOrDefault(t => t.Topic == topicName)
                ?? throw new Exception($"Topic '{topicName}' not found.");

            var stats = new TopicStatistics { AnalyzedAt = DateTime.Now };

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"kafka-ui-analyze-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
            using var consumer = new ConsumerBuilder<byte[]?, byte[]?>(consumerConfig).Build();

            var keys = new HashSet<string>();
            var values = new HashSet<string>();
            long totalMessages = 0;
            long? minOffsetOverall = null, maxOffsetOverall = null;
            DateTime? minTs = null, maxTs = null;

            foreach (var p in topicMeta.Partitions)
            {
                var tp = new TopicPartition(topicName, p.PartitionId);
                var watermarks = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
                long low = watermarks.Low.Value;
                long high = watermarks.High.Value;
                long partitionCount = Math.Max(0, high - low);

                stats.Partitions.Add(new TopicPartitionStats
                {
                    PartitionId = p.PartitionId,
                    TotalMessages = partitionCount,
                    MinOffset = low,
                    MaxOffset = high - 1 < low ? low : high - 1
                });

                totalMessages += partitionCount;
                if (partitionCount <= 0) continue;

                minOffsetOverall = minOffsetOverall.HasValue ? Math.Min(minOffsetOverall.Value, low) : low;
                maxOffsetOverall = maxOffsetOverall.HasValue ? Math.Max(maxOffsetOverall.Value, high - 1) : high - 1;

                consumer.Assign(new TopicPartitionOffset(tp, low));
                var deadline = DateTime.UtcNow.AddSeconds(15);
                long consumed = 0;
                while (consumed < partitionCount && DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
                {
                    var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                    if (result == null) break;
                    consumed++;

                    var ts = result.Message.Timestamp.UtcDateTime;
                    minTs = minTs.HasValue ? (ts < minTs ? ts : minTs) : ts;
                    maxTs = maxTs.HasValue ? (ts > maxTs ? ts : maxTs) : ts;

                    if (result.Message.Key == null) stats.NullKeys++;
                    else keys.Add(Convert.ToBase64String(result.Message.Key));

                    if (result.Message.Value == null) stats.NullValues++;
                    else values.Add(Convert.ToBase64String(result.Message.Value));
                }
                consumer.Unassign();
            }

            stats.TotalMessages = totalMessages;
            stats.MinOffset = minOffsetOverall;
            stats.MaxOffset = maxOffsetOverall;
            stats.MinTimestamp = minTs;
            stats.MaxTimestamp = maxTs;
            stats.UniqueKeys = keys.Count;
            stats.UniqueValues = values.Count;

            return stats;
        }

        public async Task<List<KafkaMessage>> GetMessagesAsync(string bootstrapServers, string topicName, int partition, long offset, int count, CancellationToken ct = default, DateTime? seekTimestamp = null)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"kafka-ui-read-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            var messages = new List<KafkaMessage>();
            using var consumer = new ConsumerBuilder<string?, string?>(consumerConfig).Build();

            var admin = GetAdminClient(bootstrapServers);
            var meta = admin.GetMetadata(topicName, TimeSpan.FromSeconds(5));
            var topicMeta = meta.Topics.Find(t => t.Topic == topicName);
            if (topicMeta == null || topicMeta.Partitions.Count == 0)
                return messages;

            // partition < 0 means "All items are selected" - target every partition of the
            // topic instead of silently falling back to partition 0, otherwise messages that
            // only live in other partitions (e.g. partition 1) never get consumed.
            var targetPartitionIds = partition < 0
                ? topicMeta.Partitions.Select(p => p.PartitionId).ToList()
                : new List<int> { partition };

            if (seekTimestamp.HasValue)
            {
                // Seek Type = Timestamp: resolve the earliest offset at/after the given
                // timestamp for each targeted partition, then start consuming from there.
                var epochMs = new DateTimeOffset(DateTime.SpecifyKind(seekTimestamp.Value, DateTimeKind.Local)).ToUnixTimeMilliseconds();
                var searchTimes = targetPartitionIds
                    .Select(p => new TopicPartitionTimestamp(topicName, p, new Timestamp(epochMs, TimestampType.CreateTime)))
                    .ToList();

                var resolved = consumer.OffsetsForTimes(searchTimes, TimeSpan.FromSeconds(10));
                var assignments = resolved
                    .Where(r => r.Offset.Value >= 0)
                    .Select(r => new TopicPartitionOffset(r.TopicPartition, r.Offset))
                    .ToList();

                if (assignments.Count == 0)
                    return messages;

                consumer.Assign(assignments);
            }
            else
            {
                var assignments = targetPartitionIds
                    .Select(p => new TopicPartitionOffset(topicName, p, offset))
                    .ToList();
                consumer.Assign(assignments);
            }

            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (messages.Count < count && DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result == null) break;
                messages.Add(new KafkaMessage
                {
                    Partition = result.Partition.Value,
                    Offset = result.Offset.Value,
                    Timestamp = result.Message.Timestamp.UtcDateTime,
                    Key = result.Message.Key,
                    Value = result.Message.Value,
                    Headers = result.Message.Headers?.ToDictionary(h => h.Key, h => System.Text.Encoding.UTF8.GetString(h.GetValueBytes())) ?? new()
                });
            }
            return messages;
        }

        public async Task ProduceMessageAsync(string bootstrapServers, string topicName, string? key, string value, Dictionary<string, string>? headers = null, int? partition = null)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            using var producer = new ProducerBuilder<string?, string>(producerConfig).Build();
            var msg = new Message<string?, string> { Key = key, Value = value };
            if (headers != null)
            {
                msg.Headers = new Headers();
                foreach (var h in headers)
                    msg.Headers.Add(h.Key, System.Text.Encoding.UTF8.GetBytes(h.Value));
            }
            if (partition.HasValue)
                await producer.ProduceAsync(new TopicPartition(topicName, partition.Value), msg);
            else
                await producer.ProduceAsync(topicName, msg);
        }

        public async Task<ClusterStats> GetClusterStatsAsync(string bootstrapServers)
        {
            var admin = GetAdminClient(bootstrapServers);
            var meta = admin.GetMetadata(TimeSpan.FromSeconds(5));
            return new ClusterStats
            {
                ActiveControllerCount = 1,
                OfflinePartitionsCount = meta.Topics.SelectMany(t => t.Partitions).Count(p => p.Leader < 0),
                UnderReplicatedPartitions = meta.Topics.SelectMany(t => t.Partitions).Count(p => p.InSyncReplicas.Length < p.Replicas.Length)
            };
        }
    }
}
