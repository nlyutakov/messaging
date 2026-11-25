# Architecture

`Messaging.RabbitMQ` handles transport, topology, and lifecycle. Applications use the contracts in `Messaging.Abstractions` and select a storage provider through DI. Management and outbox are separate packages.

Commands and durable event subscriptions can deliver at least once with durable topology. Broadcast reaches active subscribers. RPC uses a separate reply queue and transport correlation. [Detailed architecture](../ARCHITECTURE.md) · [Diagrams](diagrams/README.md)
