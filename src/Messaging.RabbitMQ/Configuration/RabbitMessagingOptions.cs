namespace Messaging.RabbitMQ.Configuration;
public class RabbitMessagingOptions {
 public const string SectionName="RabbitMessaging";
 public string Host {get;set;}="localhost"; public int Port {get;set;}=5672; public string Username {get;set;}="guest"; public string Password {get;set;}="guest"; public string VirtualHost {get;set;}="/";
 public RpcOptions Rpc {get;set;}=new(); public ConsumerOptions Consumer {get;set;}=new();
}
public class RpcOptions { public TimeSpan DefaultTimeout {get;set;}=TimeSpan.FromSeconds(30); }
public enum RabbitQueueType { Classic, Quorum }
public class ConsumerOptions {
 public bool AutoDeclare {get;set;}=true; public int Credit {get;set;}=50; public int MaxConcurrency {get;set;}=10; public RabbitQueueType QueueType {get;set;}=RabbitQueueType.Quorum;
 public RetryOptions Retry {get;set;}=new(); public DeadLetterOptions DeadLetter {get;set;}=new();
}
public class RetryOptions {public int MaxAttempts {get;set;}=3; public TimeSpan InitialDelay {get;set;}=TimeSpan.FromSeconds(2); public TimeSpan MaxDelay {get;set;}=TimeSpan.FromSeconds(30); public double BackoffMultiplier {get;set;}=2;}
public class DeadLetterOptions {public bool Enabled {get;set;}=true;public string Suffix {get;set;}=".dlq";}
public class QueueOptions {public RabbitQueueType? QueueType {get;set;} public int? MaxConcurrency {get;set;} public int? Credit {get;set;} }
