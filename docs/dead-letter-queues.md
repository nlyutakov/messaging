# Dead-letter queues

With automatic declaration and dead-lettering enabled, each command queue gets a direct dead-letter exchange and a DLQ. Event subscriptions use a durable DLQ shared by the logical subscriber. A quorum source queue can add an application failure code. A classic source queue supplies the broker's reason.

Queue arguments are part of the topology. If an existing queue uses different arguments, use compatible settings or plan a migration. Without a configured DLQ, a terminal discard can remove the delivery. [DLQ Management](dlq-management.md)
