using KafkaUI.Models;
using Microsoft.Data.Sqlite;
using System.Collections.Generic;
using System.IO;

namespace KafkaUI.Services
{
    public class ClusterStore
    {
        private readonly string _dbPath = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "KafkaUI", "clusters.db");

        private string ConnectionString => $"Data Source={_dbPath}";

        public ClusterStore()
        {
            InitialiseDatabase();
        }

        private void InitialiseDatabase()
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_dbPath)!);
            using var conn = new SqliteConnection(ConnectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                CREATE TABLE IF NOT EXISTS Clusters (
                    Id                 TEXT PRIMARY KEY,
                    Name               TEXT NOT NULL,
                    BootstrapServers   TEXT NOT NULL,
                    IsConnected        INTEGER NOT NULL DEFAULT 0,
                    BrokerCount        INTEGER NOT NULL DEFAULT 0,
                    TopicCount         INTEGER NOT NULL DEFAULT 0,
                    MessagesPerSecond  INTEGER NOT NULL DEFAULT 0,
                    SchemaRegistryUrl  TEXT,
                    KafkaConnectUrl    TEXT,
                    IsDefault          INTEGER NOT NULL DEFAULT 0
                );
                """;
            cmd.ExecuteNonQuery();
            // Migration: add IsDefault column if it doesn't exist yet
            try
            {
                using var alter = conn.CreateCommand();
                alter.CommandText = "ALTER TABLE Clusters ADD COLUMN IsDefault INTEGER NOT NULL DEFAULT 0;";
                alter.ExecuteNonQuery();
            }
            catch { /* column already exists */ }
        }

        public List<KafkaCluster> LoadClusters()
        {
            var clusters = new List<KafkaCluster>();
            try
            {
                using var conn = new SqliteConnection(ConnectionString);
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT Id, Name, BootstrapServers, IsConnected, BrokerCount, TopicCount, MessagesPerSecond, SchemaRegistryUrl, KafkaConnectUrl, IsDefault FROM Clusters ORDER BY rowid;";
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    clusters.Add(new KafkaCluster
                    {
                        Id                = reader.GetString(0),
                        Name              = reader.GetString(1),
                        BootstrapServers  = reader.GetString(2),
                        IsConnected       = reader.GetInt32(3) == 1,
                        BrokerCount       = reader.GetInt32(4),
                        TopicCount        = reader.GetInt32(5),
                        MessagesPerSecond = reader.GetInt64(6),
                        SchemaRegistryUrl = reader.IsDBNull(7) ? null : reader.GetString(7),
                        KafkaConnectUrl   = reader.IsDBNull(8) ? null : reader.GetString(8),
                        IsDefault         = !reader.IsDBNull(9) && reader.GetInt32(9) == 1
                    });
                }
            }
            catch { }
            return clusters;
        }

        public void SaveClusters(IEnumerable<KafkaCluster> clusters)
        {
            try
            {
                using var conn = new SqliteConnection(ConnectionString);
                conn.Open();
                using var tx = conn.BeginTransaction();

                // Replace all rows with the current in-memory list
                using var del = conn.CreateCommand();
                del.Transaction = tx;
                del.CommandText = "DELETE FROM Clusters;";
                del.ExecuteNonQuery();

                using var ins = conn.CreateCommand();
                ins.Transaction = tx;
                ins.CommandText = """
                    INSERT INTO Clusters (Id, Name, BootstrapServers, IsConnected, BrokerCount, TopicCount, MessagesPerSecond, SchemaRegistryUrl, KafkaConnectUrl, IsDefault)
                    VALUES ($id, $name, $bs, $conn, $brokers, $topics, $mps, $schema, $connect, $isDefault)
                    """;

                var pId        = ins.Parameters.Add("$id",        SqliteType.Text);
                var pName      = ins.Parameters.Add("$name",      SqliteType.Text);
                var pBs        = ins.Parameters.Add("$bs",        SqliteType.Text);
                var pConn      = ins.Parameters.Add("$conn",      SqliteType.Integer);
                var pBrokers   = ins.Parameters.Add("$brokers",   SqliteType.Integer);
                var pTopics    = ins.Parameters.Add("$topics",    SqliteType.Integer);
                var pMps       = ins.Parameters.Add("$mps",       SqliteType.Integer);
                var pSchema    = ins.Parameters.Add("$schema",    SqliteType.Text);
                var pConnect   = ins.Parameters.Add("$connect",   SqliteType.Text);
                var pIsDefault = ins.Parameters.Add("$isDefault", SqliteType.Integer);

                foreach (var c in clusters)
                {
                    pId.Value        = c.Id;
                    pName.Value      = c.Name;
                    pBs.Value        = c.BootstrapServers;
                    pConn.Value      = c.IsConnected ? 1 : 0;
                    pBrokers.Value   = c.BrokerCount;
                    pTopics.Value    = c.TopicCount;
                    pMps.Value       = c.MessagesPerSecond;
                    pSchema.Value    = (object?)c.SchemaRegistryUrl ?? DBNull.Value;
                    pConnect.Value   = (object?)c.KafkaConnectUrl   ?? DBNull.Value;
                    pIsDefault.Value = c.IsDefault ? 1 : 0;
                    ins.ExecuteNonQuery();
                }

                tx.Commit();
            }
            catch { }
        }

        /// <summary>Upserts a single cluster without rewriting the whole table.</summary>
        public void UpsertCluster(KafkaCluster c)
        {
            try
            {
                using var conn = new SqliteConnection(ConnectionString);
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = """
                    INSERT INTO Clusters (Id, Name, BootstrapServers, IsConnected, BrokerCount, TopicCount, MessagesPerSecond, SchemaRegistryUrl, KafkaConnectUrl, IsDefault)
                    VALUES ($id, $name, $bs, $conn, $brokers, $topics, $mps, $schema, $connect, $isDefault)
                    ON CONFLICT(Id) DO UPDATE SET
                        Name              = excluded.Name,
                        BootstrapServers  = excluded.BootstrapServers,
                        IsConnected       = excluded.IsConnected,
                        BrokerCount       = excluded.BrokerCount,
                        TopicCount        = excluded.TopicCount,
                        MessagesPerSecond = excluded.MessagesPerSecond,
                        SchemaRegistryUrl = excluded.SchemaRegistryUrl,
                        KafkaConnectUrl   = excluded.KafkaConnectUrl,
                        IsDefault         = excluded.IsDefault;
                    """;
                cmd.Parameters.AddWithValue("$id",        c.Id);
                cmd.Parameters.AddWithValue("$name",      c.Name);
                cmd.Parameters.AddWithValue("$bs",        c.BootstrapServers);
                cmd.Parameters.AddWithValue("$conn",      c.IsConnected ? 1 : 0);
                cmd.Parameters.AddWithValue("$brokers",   c.BrokerCount);
                cmd.Parameters.AddWithValue("$topics",    c.TopicCount);
                cmd.Parameters.AddWithValue("$mps",       c.MessagesPerSecond);
                cmd.Parameters.AddWithValue("$schema",    (object?)c.SchemaRegistryUrl ?? DBNull.Value);
                cmd.Parameters.AddWithValue("$connect",   (object?)c.KafkaConnectUrl   ?? DBNull.Value);
                cmd.Parameters.AddWithValue("$isDefault", c.IsDefault ? 1 : 0);
                cmd.ExecuteNonQuery();
            }
            catch { }
        }

        /// <summary>Sets IsDefault=0 for all clusters, used before marking a new default.</summary>
        public void ClearDefaultFlag()
        {
            try
            {
                using var conn = new SqliteConnection(ConnectionString);
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "UPDATE Clusters SET IsDefault = 0;";
                cmd.ExecuteNonQuery();
            }
            catch { }
        }

        /// <summary>Deletes a single cluster by Id.</summary>
        public void DeleteCluster(string id)
        {
            try
            {
                using var conn = new SqliteConnection(ConnectionString);
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "DELETE FROM Clusters WHERE Id = $id;";
                cmd.Parameters.AddWithValue("$id", id);
                cmd.ExecuteNonQuery();
            }
            catch { }
        }
    }
}
