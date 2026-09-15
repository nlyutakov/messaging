using System.Text;

namespace Messaging.Core.Naming;

public sealed class DefaultMessageNameFormatter : IMessageNameFormatter
{
    public string Format<TMessage>()
        where TMessage : class
    {
        return Format(typeof(TMessage));
    }

    public string Format(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        var name = messageType.Name;

        if (name.EndsWith("Command", StringComparison.Ordinal))
        {
            name = name[..^"Command".Length];
        }

        return ToKebabCase(name);
    }

    private static string ToKebabCase(string value)
    {
        var builder = new StringBuilder();

        for (var i = 0; i < value.Length; i++)
        {
            var current = value[i];

            if (char.IsUpper(current) && i > 0)
            {
                builder.Append('-');
            }

            builder.Append(char.ToLowerInvariant(current));
        }

        return builder.ToString();
    }
}