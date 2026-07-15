using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace KafkaUI.Models
{
    public enum SchemaType
    {
        AVRO,
        JSON,
        PROTOBUF
    }

    public enum CompatibilityLevel
    {
        NONE,
        BACKWARD,
        BACKWARD_TRANSITIVE,
        FORWARD,
        FORWARD_TRANSITIVE,
        FULL,
        FULL_TRANSITIVE
    }

    public class SchemaSubject
    {
        public string Name { get; set; } = string.Empty;
        public int LatestVersion { get; set; }
        public SchemaType SchemaType { get; set; }
        public CompatibilityLevel Compatibility { get; set; }
        public string? LatestSchema { get; set; }
        public int SchemaId { get; set; }
    }

    public class SchemaVersion
    {
        public string Subject { get; set; } = string.Empty;
        public int Version { get; set; }
        public int Id { get; set; }
        public string Schema { get; set; } = string.Empty;
        public SchemaType SchemaType { get; set; }
    }

    // ---- Confluent REST API response shapes ----

    internal class SchemaRegistrySchemaResponse
    {
        [JsonPropertyName("subject")]
        public string Subject { get; set; } = string.Empty;

        [JsonPropertyName("version")]
        public int Version { get; set; }

        [JsonPropertyName("id")]
        public int Id { get; set; }

        [JsonPropertyName("schemaType")]
        public string? SchemaType { get; set; }

        [JsonPropertyName("schema")]
        public string Schema { get; set; } = string.Empty;
    }

    internal class SchemaRegistryCompatibilityResponse
    {
        [JsonPropertyName("compatibilityLevel")]
        public string CompatibilityLevel { get; set; } = "NONE";
    }

    internal class SchemaRegistryRegisterRequest
    {
        [JsonPropertyName("schema")]
        public string Schema { get; set; } = string.Empty;

        [JsonPropertyName("schemaType")]
        public string SchemaType { get; set; } = "AVRO";
    }

    internal class SchemaRegistryRegisterResponse
    {
        [JsonPropertyName("id")]
        public int Id { get; set; }
    }

    internal class SchemaRegistryCompatibilityRequest
    {
        [JsonPropertyName("compatibility")]
        public string Compatibility { get; set; } = "NONE";
    }
}
