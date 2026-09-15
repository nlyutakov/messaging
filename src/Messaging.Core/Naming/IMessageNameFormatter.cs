namespace Messaging.Core.Naming;

public interface IMessageNameFormatter
{
    string Format<TMessage>()
        where TMessage : class;

    string Format(Type messageType);
}