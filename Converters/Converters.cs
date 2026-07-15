using System.Globalization;
using System.Windows;
using System.Windows.Data;
using System.Windows.Media;

namespace KafkaUI
{
    // Null → Visible (collapsed when not null)
    public class NullToVisibilityConverter : IValueConverter
    {
        public static readonly NullToVisibilityConverter Instance = new();
        public object Convert(object? v, Type t, object? p, CultureInfo c) => v == null ? Visibility.Visible : Visibility.Collapsed;
        public object ConvertBack(object? v, Type t, object? p, CultureInfo c) => throw new NotImplementedException();
    }

    // Not-null → Visible
    public class NotNullToVisibilityConverter : IValueConverter
    {
        public static readonly NotNullToVisibilityConverter Instance = new();
        public object Convert(object? v, Type t, object? p, CultureInfo c) => v != null ? Visibility.Visible : Visibility.Collapsed;
        public object ConvertBack(object? v, Type t, object? p, CultureInfo c) => throw new NotImplementedException();
    }

    // Bool → Visibility
    public class BoolToVisibilityConverter : IValueConverter
    {
        public static readonly BoolToVisibilityConverter Instance = new();
        public object Convert(object? v, Type t, object? p, CultureInfo c) => v is true ? Visibility.Visible : Visibility.Collapsed;
        public object ConvertBack(object? v, Type t, object? p, CultureInfo c) => v is Visibility.Visible;
    }

    // Inverse Bool → Visibility
    public class InverseBoolToVisibilityConverter : IValueConverter
    {
        public static readonly InverseBoolToVisibilityConverter Instance = new();
        public object Convert(object? v, Type t, object? p, CultureInfo c) => v is false ? Visibility.Visible : Visibility.Collapsed;
        public object ConvertBack(object? v, Type t, object? p, CultureInfo c) => throw new NotImplementedException();
    }

    // Inverse bool
    public class InverseBoolConverter : IValueConverter
    {
        public static readonly InverseBoolConverter Instance = new();
        public object Convert(object? v, Type t, object? p, CultureInfo c) => v is bool b ? !b : true;
        public object ConvertBack(object? v, Type t, object? p, CultureInfo c) => throw new NotImplementedException();
    }

    // Bool → "Yes"/"No"
    public class BoolToYesNoConverter : IValueConverter
    {
        public static readonly BoolToYesNoConverter Instance = new();
        public object Convert(object? v, Type t, object? p, CultureInfo c) => v is true ? "Yes" : "No";
        public object ConvertBack(object? v, Type t, object? p, CultureInfo c) => throw new NotImplementedException();
    }

    // Bool → background brush (green/dark for controller)
    public class BoolToBrushConverter : IValueConverter
    {
        public static readonly BoolToBrushConverter Instance = new();
        public object Convert(object? v, Type t, object? p, CultureInfo c)
            => v is true ? new SolidColorBrush(Color.FromRgb(0x1A, 0x3A, 0x2A)) : new SolidColorBrush(Color.FromRgb(0x2A, 0x2A, 0x3A));
        public object ConvertBack(object? v, Type t, object? p, CultureInfo c) => throw new NotImplementedException();
    }

    // Bool → text brush
    public class BoolToTextBrushConverter : IValueConverter
    {
        public static readonly BoolToTextBrushConverter Instance = new();
        public object Convert(object? v, Type t, object? p, CultureInfo c)
            => v is true ? new SolidColorBrush(Color.FromRgb(0x2D, 0xD4, 0xA5)) : new SolidColorBrush(Color.FromRgb(0x8B, 0x93, 0xC4));
        public object ConvertBack(object? v, Type t, object? p, CultureInfo c) => throw new NotImplementedException();
    }

    // Bool → string with static variants
    public class BoolToStringConverter : IValueConverter
    {
        private readonly string _trueVal;
        private readonly string _falseVal;

        public BoolToStringConverter(string trueVal, string falseVal) { _trueVal = trueVal; _falseVal = falseVal; }

        public static readonly BoolToStringConverter TestConnection = new("Testing...", "Test Connection");

        public object Convert(object? v, Type t, object? p, CultureInfo c) => v is true ? _trueVal : _falseVal;
        public object ConvertBack(object? v, Type t, object? p, CultureInfo c) => throw new NotImplementedException();
    }

    // String starts with "✓" or "✗"
    public class StringStartsWithConverter : IValueConverter
    {
        private readonly string _prefix;
        public StringStartsWithConverter(string prefix) { _prefix = prefix; }
        public static readonly StringStartsWithConverter Success = new("✓");
        public static readonly StringStartsWithConverter Failure = new("✗");
        public object Convert(object? v, Type t, object? p, CultureInfo c) => v is string s && s.StartsWith(_prefix);
        public object ConvertBack(object? v, Type t, object? p, CultureInfo c) => throw new NotImplementedException();
    }

    // Status message → foreground brush based on prefix character
    public class StatusMessageColorConverter : IValueConverter
    {
        public static readonly StatusMessageColorConverter Instance = new();
        public object Convert(object? v, Type t, object? p, CultureInfo c)
        {
            if (v is string s)
            {
                if (s.StartsWith("✓")) return new SolidColorBrush(Color.FromRgb(0x2D, 0xD4, 0xA5));
                if (s.StartsWith("✗")) return new SolidColorBrush(Color.FromRgb(0xEF, 0x53, 0x50));
            }
            return new SolidColorBrush(Color.FromRgb(0x8B, 0x93, 0xC4));
        }
        public object ConvertBack(object? v, Type t, object? p, CultureInfo c) => throw new NotImplementedException();
    }

    // Converts a tab index to Visibility — visible when index matches ConverterParameter
    public class IndexToVisibilityConverter : IValueConverter
    {
        public static readonly IndexToVisibilityConverter Instance = new();
        public object Convert(object? v, Type t, object? p, CultureInfo c)
        {
            if (v is int idx && p is string param && int.TryParse(param, out int target))
                return idx == target ? Visibility.Visible : Visibility.Collapsed;
            return Visibility.Collapsed;
        }
        public object ConvertBack(object? v, Type t, object? p, CultureInfo c) => throw new NotImplementedException();
    }
}
