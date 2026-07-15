using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using KafkaUI.Models;
using KafkaUI.Services;

namespace KafkaUI.ViewModels
{
    public class SchemaRegistryViewModel : ViewModelBase
    {
        private SchemaRegistryService? _service;

        // ── State ────────────────────────────────────────────────────────────

        private bool _isLoading;
        public bool IsLoading
        {
            get => _isLoading;
            set { _isLoading = value; OnPropertyChanged(); }
        }

        private string _statusMessage = string.Empty;
        public string StatusMessage
        {
            get => _statusMessage;
            set { _statusMessage = value; OnPropertyChanged(); }
        }

        private bool _isConfigured;
        public bool IsConfigured
        {
            get => _isConfigured;
            set { _isConfigured = value; OnPropertyChanged(); }
        }

        // ── Subject list ─────────────────────────────────────────────────────

        public ObservableCollection<SchemaSubject> Subjects { get; } = new();

        private SchemaSubject? _selectedSubject;
        public SchemaSubject? SelectedSubject
        {
            get => _selectedSubject;
            set
            {
                _selectedSubject = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(HasSelection));
                _ = LoadVersionsAsync();
            }
        }

        public bool HasSelection => _selectedSubject != null;

        private string _searchText = string.Empty;
        public string SearchText
        {
            get => _searchText;
            set { _searchText = value; OnPropertyChanged(); ApplyFilter(); }
        }

        public ObservableCollection<SchemaSubject> FilteredSubjects { get; } = new();

        // ── Version list ─────────────────────────────────────────────────────

        public ObservableCollection<SchemaVersion> Versions { get; } = new();

        private SchemaVersion? _selectedVersion;
        public SchemaVersion? SelectedVersion
        {
            get => _selectedVersion;
            set { _selectedVersion = value; OnPropertyChanged(); }
        }

        // ── Create / Edit panel ───────────────────────────────────────────────

        private bool _showEditor;
        public bool ShowEditor
        {
            get => _showEditor;
            set { _showEditor = value; OnPropertyChanged(); }
        }

        private bool _isEditing;   // true = new version of existing; false = new subject
        public bool IsEditing
        {
            get => _isEditing;
            set { _isEditing = value; OnPropertyChanged(); OnPropertyChanged(nameof(EditorTitle)); }
        }

        public string EditorTitle => IsEditing
            ? $"New version — {EditorSubjectName}"
            : "Register new schema";

        private string _editorSubjectName = string.Empty;
        public string EditorSubjectName
        {
            get => _editorSubjectName;
            set { _editorSubjectName = value; OnPropertyChanged(); OnPropertyChanged(nameof(EditorTitle)); }
        }

        private string _editorSchema = string.Empty;
        public string EditorSchema
        {
            get => _editorSchema;
            set { _editorSchema = value; OnPropertyChanged(); }
        }

        private SchemaType _editorSchemaType = SchemaType.AVRO;
        public SchemaType EditorSchemaType
        {
            get => _editorSchemaType;
            set { _editorSchemaType = value; OnPropertyChanged(); }
        }

        public IEnumerable<SchemaType> SchemaTypes { get; } =
            Enum.GetValues<SchemaType>();

        // ── Compatibility panel ───────────────────────────────────────────────

        private CompatibilityLevel _selectedCompatibility;
        public CompatibilityLevel SelectedCompatibility
        {
            get => _selectedCompatibility;
            set { _selectedCompatibility = value; OnPropertyChanged(); }
        }

        public IEnumerable<CompatibilityLevel> CompatibilityLevels { get; } =
            Enum.GetValues<CompatibilityLevel>();

        // ── Commands ──────────────────────────────────────────────────────────

        public ICommand RefreshCommand { get; }
        public ICommand OpenCreateCommand { get; }
        public ICommand OpenEditCommand { get; }
        public ICommand SaveSchemaCommand { get; }
        public ICommand CancelEditorCommand { get; }
        public ICommand DeleteSubjectCommand { get; }
        public ICommand DeleteVersionCommand { get; }
        public ICommand SaveCompatibilityCommand { get; }
        public ICommand CopySchemaCommand { get; }
        public ICommand FormatSchemaCommand { get; }

        // ── Constructor ───────────────────────────────────────────────────────

        public SchemaRegistryViewModel()
        {
            RefreshCommand        = new AsyncRelayCommand(LoadSubjectsAsync);
            OpenCreateCommand     = new RelayCommand(_ => OpenCreate());
            OpenEditCommand       = new RelayCommand(_ => OpenEdit(), _ => HasSelection);
            SaveSchemaCommand     = new AsyncRelayCommand(SaveSchemaAsync);
            CancelEditorCommand   = new RelayCommand(_ => ShowEditor = false);
            DeleteSubjectCommand  = new AsyncRelayCommand(DeleteSubjectAsync, () => HasSelection);
            DeleteVersionCommand  = new AsyncRelayCommand(DeleteVersionAsync, () => SelectedVersion != null);
            SaveCompatibilityCommand = new AsyncRelayCommand(SaveCompatibilityAsync, () => HasSelection);
            CopySchemaCommand     = new RelayCommand(_ => CopySchema(), _ => SelectedSubject != null);
            FormatSchemaCommand   = new RelayCommand(_ => FormatSchema());
        }

        // ── Initialise with cluster config ────────────────────────────────────

        public void Initialise(KafkaCluster cluster)
        {
            if (string.IsNullOrWhiteSpace(cluster.SchemaRegistryUrl))
            {
                IsConfigured = false;
                StatusMessage = "No Schema Registry URL configured for this cluster.";
                return;
            }

            IsConfigured = true;
            _service = new SchemaRegistryService(
                cluster.SchemaRegistryUrl,
                cluster.SchemaRegistryUsername,
                cluster.SchemaRegistryPassword);

            _ = LoadSubjectsAsync();
        }

        // ── Data loading ──────────────────────────────────────────────────────

        private async Task LoadSubjectsAsync()
        {
            if (_service == null) return;
            IsLoading = true;
            StatusMessage = "Loading schemas…";
            try
            {
                var subjects = await _service.GetAllSubjectsAsync();
                Subjects.Clear();
                foreach (var s in subjects.OrderBy(s => s.Name))
                    Subjects.Add(s);
                ApplyFilter();
                StatusMessage = $"{Subjects.Count} subject(s) loaded.";
            }
            catch (Exception ex)
            {
                StatusMessage = $"Error: {ex.Message}";
            }
            finally
            {
                IsLoading = false;
            }
        }

        private async Task LoadVersionsAsync()
        {
            Versions.Clear();
            SelectedVersion = null;
            if (_service == null || SelectedSubject == null) return;

            try
            {
                var versionNumbers = await _service.GetVersionsAsync(SelectedSubject.Name);
                var tasks = versionNumbers.Select(v =>
                    _service.GetSchemaVersionAsync(SelectedSubject.Name, v.ToString()));
                var versions = await Task.WhenAll(tasks);
                foreach (var v in versions.OrderByDescending(v => v.Version))
                    Versions.Add(v);

                SelectedCompatibility = SelectedSubject.Compatibility;
            }
            catch (Exception ex)
            {
                StatusMessage = $"Failed to load versions: {ex.Message}";
            }
        }

        // ── Filter ────────────────────────────────────────────────────────────

        private void ApplyFilter()
        {
            FilteredSubjects.Clear();
            var query = SearchText.Trim().ToLowerInvariant();
            foreach (var s in Subjects)
                if (string.IsNullOrEmpty(query) || s.Name.ToLowerInvariant().Contains(query))
                    FilteredSubjects.Add(s);
        }

        // ── Create / Edit ─────────────────────────────────────────────────────

        private void OpenCreate()
        {
            IsEditing = false;
            EditorSubjectName = string.Empty;
            EditorSchema = GetSchemaTemplate(SchemaType.AVRO);
            EditorSchemaType = SchemaType.AVRO;
            ShowEditor = true;
        }

        private void OpenEdit()
        {
            if (SelectedSubject == null) return;
            IsEditing = true;
            EditorSubjectName = SelectedSubject.Name;
            EditorSchema = SelectedSubject.LatestSchema ?? string.Empty;
            EditorSchemaType = SelectedSubject.SchemaType;
            ShowEditor = true;
        }

        private async Task SaveSchemaAsync()
        {
            if (_service == null) return;
            if (string.IsNullOrWhiteSpace(EditorSubjectName))
            {
                StatusMessage = "Subject name cannot be empty.";
                return;
            }

            IsLoading = true;
            try
            {
                var id = await _service.RegisterSchemaAsync(
                    EditorSubjectName, EditorSchema, EditorSchemaType);
                ShowEditor = false;
                StatusMessage = $"Schema registered (id={id}).";
                await LoadSubjectsAsync();
            }
            catch (Exception ex)
            {
                StatusMessage = $"Registration failed: {ex.Message}";
            }
            finally
            {
                IsLoading = false;
            }
        }

        // ── Delete ────────────────────────────────────────────────────────────

        private async Task DeleteSubjectAsync()
        {
            if (_service == null || SelectedSubject == null) return;

            var confirm = MessageBox.Show(
                $"Delete all versions of '{SelectedSubject.Name}'?",
                "Confirm delete", MessageBoxButton.YesNo, MessageBoxImage.Warning);
            if (confirm != MessageBoxResult.Yes) return;

            IsLoading = true;
            try
            {
                await _service.DeleteSubjectAsync(SelectedSubject.Name);
                StatusMessage = $"'{SelectedSubject.Name}' deleted.";
                await LoadSubjectsAsync();
            }
            catch (Exception ex)
            {
                StatusMessage = $"Delete failed: {ex.Message}";
            }
            finally
            {
                IsLoading = false;
            }
        }

        private async Task DeleteVersionAsync()
        {
            if (_service == null || SelectedSubject == null || SelectedVersion == null) return;

            var confirm = MessageBox.Show(
                $"Delete version {SelectedVersion.Version} of '{SelectedSubject.Name}'?",
                "Confirm delete", MessageBoxButton.YesNo, MessageBoxImage.Warning);
            if (confirm != MessageBoxResult.Yes) return;

            IsLoading = true;
            try
            {
                await _service.DeleteVersionAsync(SelectedSubject.Name, SelectedVersion.Version);
                StatusMessage = $"Version {SelectedVersion.Version} deleted.";
                await LoadVersionsAsync();
            }
            catch (Exception ex)
            {
                StatusMessage = $"Delete failed: {ex.Message}";
            }
            finally
            {
                IsLoading = false;
            }
        }

        // ── Compatibility ─────────────────────────────────────────────────────

        private async Task SaveCompatibilityAsync()
        {
            if (_service == null || SelectedSubject == null) return;
            IsLoading = true;
            try
            {
                await _service.SetSubjectCompatibilityAsync(
                    SelectedSubject.Name, SelectedCompatibility);
                SelectedSubject.Compatibility = SelectedCompatibility;
                StatusMessage = $"Compatibility set to {SelectedCompatibility}.";
            }
            catch (Exception ex)
            {
                StatusMessage = $"Failed: {ex.Message}";
            }
            finally
            {
                IsLoading = false;
            }
        }

        // ── Helpers ───────────────────────────────────────────────────────────

        private void CopySchema()
        {
            if (SelectedSubject?.LatestSchema != null)
                Clipboard.SetText(SelectedSubject.LatestSchema);
        }

        private void FormatSchema()
        {
            if (string.IsNullOrWhiteSpace(EditorSchema)) return;
            try
            {
                using var doc = System.Text.Json.JsonDocument.Parse(EditorSchema);
                EditorSchema = System.Text.Json.JsonSerializer.Serialize(
                    doc.RootElement,
                    new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
            }
            catch
            {
                StatusMessage = "Schema is not valid JSON — cannot format.";
            }
        }

        private static string GetSchemaTemplate(SchemaType type) => type switch
        {
            SchemaType.AVRO => """
                {
                  "type": "record",
                  "name": "MyRecord",
                  "namespace": "com.example",
                  "fields": [
                    { "name": "id", "type": "long" },
                    { "name": "name", "type": "string" }
                  ]
                }
                """,
            SchemaType.JSON => """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "title": "MyRecord",
                  "type": "object",
                  "properties": {
                    "id": { "type": "integer" },
                    "name": { "type": "string" }
                  },
                  "required": ["id", "name"]
                }
                """,
            SchemaType.PROTOBUF => """
                syntax = "proto3";
                package com.example;

                message MyRecord {
                  int64 id = 1;
                  string name = 2;
                }
                """,
            _ => string.Empty
        };
    }
}
