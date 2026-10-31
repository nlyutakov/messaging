namespace Messaging.Abstractions.Producers;

public interface IMessageProducer
{
    Task SendAsync<TCommand>(string destination, TCommand command, CancellationToken cancellationToken = default);
    Task<TResponse> RequestAsync<TRequest, TResponse>(string destination, TRequest request, RequestOptions? options = null, CancellationToken cancellationToken = default);
}

public class RequestOptions
{
    public TimeSpan? Timeout { get; init; }
}
