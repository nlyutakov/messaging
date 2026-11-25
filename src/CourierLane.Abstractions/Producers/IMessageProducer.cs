namespace Messaging.Abstractions.Producers;

public interface IMessageProducer
{
    Task SendAsync<TCommand>(string destination, TCommand command, CancellationToken cancellationToken = default);
    Task SendAsync<TCommand>(string destination, TCommand command, SendOptions options, CancellationToken cancellationToken) =>
        throw new NotSupportedException("This producer does not support custom message metadata.");
    Task<TResponse> RequestAsync<TRequest, TResponse>(string destination, TRequest request, RequestOptions? options = null, CancellationToken cancellationToken = default);
}

public class RequestOptions
{
    public TimeSpan? Timeout { get; init; }
    public SendOptions? Metadata { get; init; }
}
