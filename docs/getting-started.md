# Getting started

Install the .NET 10 SDK and start RabbitMQ 4.x with AMQP 1.0. Run these commands from the repository root.

```bash
dotnet build Messaging.slnx
(cd samples/Messaging.ConnectionTest && dotnet run --no-build)
# In a separate terminal
(cd samples/Messaging.ProducerTest && dotnet run --no-build)
```

The worker receives a command and answers the RPC request. Run each sample from its project directory so it loads the intended `appsettings.json`. [Local NuGet installation](deployment.md)
