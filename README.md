# Schema Registry — Integration Guide

## Files to add to your project

```
Models/SchemaRegistryModels.cs        — Model classes + internal REST response shapes
Services/SchemaRegistryService.cs     — HttpClient wrapper for Confluent Schema Registry API
ViewModels/SchemaRegistryViewModel.cs — Full MVVM ViewModel
Views/SchemaRegistryView.xaml         — WPF UI (dark-themed, matches your DarkTheme.xaml)
Views/SchemaRegistryView.xaml.cs      — Code-behind (trivial)
```

---

## 1. Models/KafkaCluster.cs

Ensure your cluster model has these properties:

```csharp
public string? SchemaRegistryUrl { get; set; }
public string? SchemaRegistryUsername { get; set; }
public string? SchemaRegistryPassword { get; set; }
```

If you already have `SchemaRegistryUrl` but named differently, update the
`Initialise()` call in `SchemaRegistryViewModel`.

---

## 2. AddClusterDialog — add Schema Registry fields

In your `AddClusterDialog.xaml`, add input fields for the three properties above,
e.g.:

```xml
<TextBlock Text="Schema Registry URL" .../>
<TextBox Text="{Binding SchemaRegistryUrl}" .../>

<TextBlock Text="Username (optional)" .../>
<TextBox Text="{Binding SchemaRegistryUsername}" .../>

<TextBlock Text="Password (optional)" .../>
<PasswordBox .../>   <!-- bind via converter or code-behind -->
```

---

## 3. MainViewModel — wire up SchemaRegistryViewModel

```csharp
public SchemaRegistryViewModel SchemaRegistryViewModel { get; } = new();

// Call this whenever SelectedCluster changes:
private void OnSelectedClusterChanged(KafkaCluster cluster)
{
    // existing wiring for brokers/topics/consumers...
    SchemaRegistryViewModel.Initialise(cluster);
}
```

---

## 4. MainWindow.xaml — bind the view

Find the TabItem or content area where `SchemaRegistryView` is hosted and set its DataContext:

```xml
<TabItem Header="Schema Registry">
    <views:SchemaRegistryView
        DataContext="{Binding SchemaRegistryViewModel}"/>
</TabItem>
```

---

## 5. Converters needed in App.xaml / DarkTheme.xaml

The XAML uses these converters (you likely already have them):

| Key | Type | Description |
|-----|------|-------------|
| `BoolToVisibilityConverter` | `BooleanToVisibilityConverter` | true → Visible |
| `BoolToVisibilityInverter` | custom | true → Collapsed |
| `BoolInverter` | custom | negates a bool |

If `BoolToVisibilityInverter` or `BoolInverter` are missing, add them to your
`Converters/` folder:

```csharp
// BoolToVisibilityInverter
public class BoolToVisibilityInverter : IValueConverter
{
    public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        => (bool)value ? Visibility.Collapsed : Visibility.Visible;
    public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        => throw new NotImplementedException();
}

// BoolInverter
public class BoolInverter : IValueConverter
{
    public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        => !(bool)value;
    public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        => !(bool)value;
}
```

And register in App.xaml or DarkTheme.xaml:

```xml
<converters:BoolToVisibilityInverter x:Key="BoolToVisibilityInverter"/>
<converters:BoolInverter x:Key="BoolInverter"/>
```

---

## Features implemented

- ✅ List all subjects with schema type, latest version, compatibility level
- ✅ Search/filter subjects
- ✅ View latest schema content (syntax-highlighted Consolas font)
- ✅ Version history with per-version delete
- ✅ Register new schema (Avro / JSON Schema / Protobuf) with template
- ✅ Add new version to existing subject
- ✅ Format JSON schema (pretty-print)
- ✅ Copy schema to clipboard
- ✅ Delete subject (all versions)
- ✅ View and update per-subject compatibility level
- ✅ Basic Auth support (username/password per cluster)
- ✅ Graceful "not configured" state when URL is empty
- ✅ Loading indicator + status bar messages

## Not yet implemented (future work)

- Schema content syntax highlighting (requires AvalonEdit or similar)
- Schema compatibility pre-check before registering
- Protobuf-aware display
- Permanent hard-delete (`?permanent=true`)
