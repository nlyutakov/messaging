using System.Collections.Concurrent;
using System.Text.Json;
using Messaging.Abstractions.Producers;
using Messaging.RabbitMQ.Configuration;
using Messaging.RabbitMQ.Connections;
using Messaging.RabbitMQ.Rpc;
using Microsoft.Extensions.Logging;
using RabbitMQ.AMQP.Client;
namespace Messaging.RabbitMQ.Producers;
public class RabbitMessageProducer:IMessageProducer,IAsyncDisposable {
 private readonly RabbitConnectionManager connections;private readonly MessageTypeRegistry messageTypes;private readonly RabbitRpcClient rpcClient;private readonly ILogger<RabbitMessageProducer> logger;private readonly ConcurrentDictionary<string,Lazy<Task<IPublisher>>> publishers=new(StringComparer.Ordinal);private readonly JsonSerializerOptions jsonOptions=new(JsonSerializerDefaults.Web);private bool disposed;
 public RabbitMessageProducer(RabbitConnectionManager connections,MessageTypeRegistry messageTypes,RabbitRpcClient rpcClient,ILogger<RabbitMessageProducer> logger){this.connections=connections;this.messageTypes=messageTypes;this.rpcClient=rpcClient;this.logger=logger;}
 public async Task SendAsync<TCommand>(string destination,TCommand command,CancellationToken cancellationToken=default){ObjectDisposedException.ThrowIf(this.disposed,this);ArgumentException.ThrowIfNullOrWhiteSpace(destination);ArgumentNullException.ThrowIfNull(command);var typeId=this.messageTypes.GetMessageTypeId<TCommand>();var payload=JsonSerializer.SerializeToUtf8Bytes(command,this.jsonOptions);var message=new AmqpMessage(payload).Property("message-type",typeId).ContentType("application/json").MessageId(Guid.NewGuid().ToString("N")).Durable(true);var publisher=await GetPublisherAsync(destination);var result=await publisher.PublishAsync(message,cancellationToken);if(result.Outcome.State!=OutcomeState.Accepted)throw new InvalidOperationException($"RabbitMQ did not accept '{typeId}' for '{destination}': {result.Outcome.State}; {result.Outcome.Error}");this.logger.LogInformation("Published {MessageType} to {Destination}",typeId,destination);}
 public Task<TResponse> RequestAsync<TRequest,TResponse>(string destination,TRequest request,RequestOptions? options=null,CancellationToken cancellationToken=default)=>this.rpcClient.RequestAsync<TRequest,TResponse>(destination,request,options,cancellationToken);
 private async Task<IPublisher> GetPublisherAsync(string destination){ObjectDisposedException.ThrowIf(this.disposed,this);var lazy=this.publishers.GetOrAdd(destination,q=>new Lazy<Task<IPublisher>>(()=>CreatePublisherAsync(q)));try{return await lazy.Value;}catch{this.publishers.TryRemove(new KeyValuePair<string,Lazy<Task<IPublisher>>>(destination,lazy));throw;}}
 private async Task<IPublisher> CreatePublisherAsync(string destination){var connection=await this.connections.GetConnectionAsync();return await connection.PublisherBuilder().Queue(destination).BuildAsync();}
 public async ValueTask DisposeAsync(){if(this.disposed)return;this.disposed=true;foreach(var lazy in this.publishers.Values){if(!lazy.IsValueCreated)continue;try{var publisher=await lazy.Value;await publisher.CloseAsync();publisher.Dispose();}catch(Exception ex){this.logger.LogWarning(ex,"Error closing publisher");}}this.publishers.Clear();}
}
