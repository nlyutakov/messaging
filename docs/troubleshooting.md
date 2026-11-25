# Troubleshooting

- **Connection refused** · Check the AMQP port and verify that RabbitMQ is listening. For AMQPS, check `UseTls`, the port, and certificate trust.
- **Queue precondition failure** · Compare the existing queue type, DLX, and arguments with the settings.
- **OptionsValidationException** · Check the provider key, its DI registration, and the matching configuration section.
- **RPC timeout** · Check the listener queue, type ID, and handler latency. The caller may miss a late reply.
- **Replay rejected** · Check the status, replay limit, and target allowlist. A temporary broadcast queue may already be gone.
- **Replay uncertain** · Check whether the original MessageId was processed before taking operator action.
- **Outbox backlog** · Check the destination and broker confirmations. Pending rows remain eligible for retry.
