using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using KafkaUI.Models;
using SchemaType = KafkaUI.Models.SchemaType;
using CompatibilityLevel = KafkaUI.Models.CompatibilityLevel;

namespace KafkaUI.Services
{
    /// <summary>
    /// Wraps the Confluent Schema Registry REST API.
    /// Docs: https://docs.confluent.io/platform/current/schema-registry/develop/api.html
    /// </summary>
    public class SchemaRegistryService
    {
        private readonly HttpClient _http;
        private readonly string _baseUrl;

        private static readonly JsonSerializerOptions _json = new()
        {
            PropertyNameCaseInsensitive = true
        };

        public SchemaRegistryService(string baseUrl, string? username = null, string? password = null)
        {
            _baseUrl = baseUrl.TrimEnd('/');
            _http = new HttpClient();
            _http.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/vnd.schemaregistry.v1+json"));

            if (!string.IsNullOrWhiteSpace(username) && !string.IsNullOrWhiteSpace(password))
            {
                var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
                _http.DefaultRequestHeaders.Authorization =
                    new AuthenticationHeaderValue("Basic", encoded);
            }
        }

        // ── Subjects ────────────────────────────────────────────────────────

        /// <summary>Returns all subject names registered in Schema Registry.</summary>
        public async Task<List<string>> GetSubjectsAsync()
        {
            var json = await GetStringAsync("/subjects");
            return JsonSerializer.Deserialize<List<string>>(json, _json) ?? new();
        }

        /// <summary>Returns all versions for a subject.</summary>
        public async Task<List<int>> GetVersionsAsync(string subject)
        {
            var json = await GetStringAsync($"/subjects/{Uri.EscapeDataString(subject)}/versions");
            return JsonSerializer.Deserialize<List<int>>(json, _json) ?? new();
        }

        /// <summary>Returns the latest registered schema for a subject, enriched with compatibility.</summary>
        public async Task<SchemaSubject> GetSubjectLatestAsync(string subject)
        {
            var schemaResp = await GetSchemaVersionAsync(subject, "latest");
            var compatibility = await GetSubjectCompatibilityAsync(subject);

            return new SchemaSubject
            {
                Name = subject,
                LatestVersion = schemaResp.Version,
                SchemaId = schemaResp.Id,
                SchemaType = schemaResp.SchemaType,
                Compatibility = compatibility,
                LatestSchema = schemaResp.Schema
            };
        }

        /// <summary>Returns a specific version of a schema (pass "latest" for latest).</summary>
        public async Task<SchemaVersion> GetSchemaVersionAsync(string subject, string version)
        {
            var json = await GetStringAsync(
                $"/subjects/{Uri.EscapeDataString(subject)}/versions/{version}");
            var resp = JsonSerializer.Deserialize<SchemaRegistrySchemaResponse>(json, _json)!;
            return new SchemaVersion
            {
                Subject = resp.Subject,
                Version = resp.Version,
                Id = resp.Id,
                Schema = resp.Schema,
                SchemaType = ParseSchemaType(resp.SchemaType)
            };
        }

        /// <summary>Loads all subjects with their latest schema in bulk (parallelised).</summary>
        public async Task<List<SchemaSubject>> GetAllSubjectsAsync()
        {
            var names = await GetSubjectsAsync();
            var tasks = names.Select(GetSubjectLatestAsync);
            var results = await Task.WhenAll(tasks);
            return results.ToList();
        }

        // ── Register / Update ────────────────────────────────────────────────

        /// <summary>Registers a new schema (or a new version of an existing subject).</summary>
        public async Task<int> RegisterSchemaAsync(string subject, string schema, SchemaType type)
        {
            var body = JsonSerializer.Serialize(new SchemaRegistryRegisterRequest
            {
                Schema = schema,
                SchemaType = type == SchemaType.AVRO ? "AVRO"
                           : type == SchemaType.JSON ? "JSON"
                           : "PROTOBUF"
            });
            var json = await PostStringAsync(
                $"/subjects/{Uri.EscapeDataString(subject)}/versions", body);
            var resp = JsonSerializer.Deserialize<SchemaRegistryRegisterResponse>(json, _json)!;
            return resp.Id;
        }

        // ── Delete ───────────────────────────────────────────────────────────

        /// <summary>Soft-deletes all versions of a subject.</summary>
        public async Task DeleteSubjectAsync(string subject)
        {
            var url = $"{_baseUrl}/subjects/{Uri.EscapeDataString(subject)}";
            var resp = await _http.DeleteAsync(url);
            await EnsureSuccessAsync(resp);
        }

        /// <summary>Deletes a specific version of a subject.</summary>
        public async Task DeleteVersionAsync(string subject, int version)
        {
            var url = $"{_baseUrl}/subjects/{Uri.EscapeDataString(subject)}/versions/{version}";
            var resp = await _http.DeleteAsync(url);
            await EnsureSuccessAsync(resp);
        }

        // ── Compatibility ────────────────────────────────────────────────────

        /// <summary>Gets the global compatibility level.</summary>
        public async Task<CompatibilityLevel> GetGlobalCompatibilityAsync()
        {
            var json = await GetStringAsync("/config");
            var resp = JsonSerializer.Deserialize<SchemaRegistryCompatibilityResponse>(json, _json)!;
            return ParseCompatibility(resp.CompatibilityLevel);
        }

        /// <summary>Gets the subject-level compatibility (falls back to global if not set).</summary>
        public async Task<CompatibilityLevel> GetSubjectCompatibilityAsync(string subject)
        {
            try
            {
                var json = await GetStringAsync($"/config/{Uri.EscapeDataString(subject)}");
                var resp = JsonSerializer.Deserialize<SchemaRegistryCompatibilityResponse>(json, _json)!;
                return ParseCompatibility(resp.CompatibilityLevel);
            }
            catch
            {
                // Subject has no override — fall back to global
                return await GetGlobalCompatibilityAsync();
            }
        }

        /// <summary>Sets the compatibility level for a subject.</summary>
        public async Task SetSubjectCompatibilityAsync(string subject, CompatibilityLevel level)
        {
            var body = JsonSerializer.Serialize(new SchemaRegistryCompatibilityRequest
            {
                Compatibility = level.ToString()
            });
            await PutStringAsync($"/config/{Uri.EscapeDataString(subject)}", body);
        }

        /// <summary>Sets the global compatibility level.</summary>
        public async Task SetGlobalCompatibilityAsync(CompatibilityLevel level)
        {
            var body = JsonSerializer.Serialize(new SchemaRegistryCompatibilityRequest
            {
                Compatibility = level.ToString()
            });
            await PutStringAsync("/config", body);
        }

        // ── Compatibility check ───────────────────────────────────────────────

        /// <summary>Tests whether a schema is compatible with the latest version of a subject.</summary>
        public async Task<bool> IsCompatibleAsync(string subject, string schema, SchemaType type)
        {
            var body = JsonSerializer.Serialize(new SchemaRegistryRegisterRequest
            {
                Schema = schema,
                SchemaType = type.ToString()
            });
            try
            {
                var json = await PostStringAsync(
                    $"/compatibility/subjects/{Uri.EscapeDataString(subject)}/versions/latest", body);
                using var doc = JsonDocument.Parse(json);
                return doc.RootElement.TryGetProperty("is_compatible", out var v) && v.GetBoolean();
            }
            catch
            {
                return false;
            }
        }

        // ── Private helpers ──────────────────────────────────────────────────

        private async Task<string> GetStringAsync(string path)
        {
            var resp = await _http.GetAsync(_baseUrl + path);
            await EnsureSuccessAsync(resp);
            return await resp.Content.ReadAsStringAsync();
        }

        private async Task<string> PostStringAsync(string path, string body)
        {
            var content = new StringContent(body, Encoding.UTF8,
                "application/vnd.schemaregistry.v1+json");
            var resp = await _http.PostAsync(_baseUrl + path, content);
            await EnsureSuccessAsync(resp);
            return await resp.Content.ReadAsStringAsync();
        }

        private async Task<string> PutStringAsync(string path, string body)
        {
            var content = new StringContent(body, Encoding.UTF8,
                "application/vnd.schemaregistry.v1+json");
            var resp = await _http.PutAsync(_baseUrl + path, content);
            await EnsureSuccessAsync(resp);
            return await resp.Content.ReadAsStringAsync();
        }

        private static async Task EnsureSuccessAsync(HttpResponseMessage resp)
        {
            if (!resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync();
                throw new Exception(
                    $"Schema Registry error {(int)resp.StatusCode}: {body}");
            }
        }

        private static SchemaType ParseSchemaType(string? raw) =>
            raw?.ToUpperInvariant() switch
            {
                "JSON" => SchemaType.JSON,
                "PROTOBUF" => SchemaType.PROTOBUF,
                _ => SchemaType.AVRO   // default / null
            };

        private static CompatibilityLevel ParseCompatibility(string? raw) =>
            Enum.TryParse<CompatibilityLevel>(raw, ignoreCase: true, out var v) ? v : CompatibilityLevel.NONE;
    }
}
