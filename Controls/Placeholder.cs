using System.Windows;
using System.Windows.Controls;

namespace KafkaUI
{
    /// <summary>
    /// Attached property that provides placeholder/watermark text for a WPF TextBox.
    /// Usage: local:Placeholder.Text="Type here..."
    /// </summary>
    public static class Placeholder
    {
        public static readonly DependencyProperty TextProperty =
            DependencyProperty.RegisterAttached(
                "Text",
                typeof(string),
                typeof(Placeholder),
                new PropertyMetadata(string.Empty, OnPlaceholderChanged));

        public static string GetText(DependencyObject obj) => (string)obj.GetValue(TextProperty);
        public static void SetText(DependencyObject obj, string value) => obj.SetValue(TextProperty, value);

        private static void OnPlaceholderChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            if (d is not TextBox tb) return;
            tb.Loaded -= TextBox_Loaded;
            tb.Loaded += TextBox_Loaded;
            tb.TextChanged -= TextBox_TextChanged;
            tb.TextChanged += TextBox_TextChanged;
            UpdatePlaceholder(tb);
        }

        private static void TextBox_Loaded(object sender, RoutedEventArgs e) => UpdatePlaceholder((TextBox)sender);
        private static void TextBox_TextChanged(object sender, TextChangedEventArgs e) => UpdatePlaceholder((TextBox)sender);

        private static void UpdatePlaceholder(TextBox tb)
        {
            var placeholder = GetText(tb);
            if (string.IsNullOrEmpty(placeholder)) return;

            // Find or create the placeholder TextBlock in the visual tree via Tag
            if (tb.Tag is TextBlock hint)
            {
                hint.Visibility = string.IsNullOrEmpty(tb.Text) ? Visibility.Visible : Visibility.Collapsed;
            }
        }
    }
}
