# Configuration

Pass the `RabbitMessaging` configuration section to `AddRabbitMessaging`.

| Setting | Default |
| --- | --- |
| Host, Port, VirtualHost | localhost, 5672, / |
| UseTls | false |
| Rpc.DefaultTimeout | 30 s |
| Consumer.AutoDeclare, QueueType | true, Quorum |
| Consumer.Credit, MaxConcurrency | 50, 10 |
| Consumer.Retry | 3 attempts, 2 s initial delay, 30 s maximum delay, multiplier 2 |
| Consumer.DeadLetter.Enabled, Suffix | true, .dlq |
| Consumer.EnableIdempotency | false |
| Idempotency.Provider | InMemory |
| Idempotency.ProcessingTimeout, RetentionPeriod | 5 min, 7 days |

Startup validation checks ports, timeouts, retry limits, and the configured provider key. Credit controls AMQP deliveries. `MaxConcurrency` limits active callbacks per queue. With `AutoDeclare=false`, the settings must match the existing topology. Broadcast requires automatic declaration. For TLS, configure the AMQPS port and a trusted certificate chain. [Deployment](deployment.md)
