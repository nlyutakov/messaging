using Messaging.RabbitMQ.Configuration;
using Messaging.RabbitMQ.Connections;
using Messaging.RabbitMQ.Dispatching;
using Messaging.RabbitMQ.Retry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.AMQP.Client;
namespace Messaging.RabbitMQ.Consumers;
public class RabbitQueueConsumer {
 private readonly RabbitConnectionManager connections;private readonly IMessageDispatcher dispatcher;private readonly RabbitMessagingOptions options;private readonly ILogger<RabbitQueueConsumer> logger;private readonly ILoggerFactory loggerFactory;
 public RabbitQueueConsumer(RabbitConnectionManager connections,IMessageDispatcher dispatcher,IOptions<RabbitMessagingOptions> options,ILogger<RabbitQueueConsumer> logger,ILoggerFactory loggerFactory){this.connections=connections;this.dispatcher=dispatcher;this.options=options.Value;this.logger=logger;this.loggerFactory=loggerFactory;}
 public async Task<IConsumer> StartAsync(QueueRegistration queue,CancellationToken cancellationToken=default){var connection=await this.connections.GetConnectionAsync(cancellationToken);var credit=queue.Options.Credit??this.options.Consumer.Credit;var consumer=await connection.ConsumerBuilder().Queue(queue.Name).InitialCredits(credit).MessageHandler(async(context,message)=>{await ProcessMessageAsync(queue.Name,context,message,cancellationToken);}).BuildAndStartAsync(cancellationToken);this.logger.LogInformation("Consumer started for {Queue}",queue.Name);return consumer;}
 private async Task ProcessMessageAsync(string queueName,IContext context,IMessage message,CancellationToken cancellationToken){try{var messageTypeId=message.Property("message-type") as string;if(string.IsNullOrWhiteSpace(messageTypeId)){this.logger.LogWarning("Message without message-type on {Queue}",queueName);context.Discard();return;}var body=message.Body();if(body is null){this.logger.LogWarning("Message without body on {Queue}",queueName);context.Discard();return;}var retry=new RetryExecutor(this.options.Consumer.Retry,this.loggerFactory.CreateLogger<RetryExecutor>());await retry.ExecuteAsync(async ct=>{await this.dispatcher.DispatchAsync(queueName,messageTypeId,body,ct);},cancellationToken);context.Accept();}catch(OperationCanceledException)when(cancellationToken.IsCancellationRequested){context.Requeue();}catch(Exception ex){this.logger.LogError(ex,"Message processing failed on {Queue}; discarding to DLQ if configured",queueName);context.Discard();}}
}
