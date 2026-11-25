# DLQ Management

`Messaging.Management.Web` hosts the API and operator interface. Configure an HTTPS issuer, audience, persistent SQLite file, and allowed DLQ names. Enabling ingestion for a DLQ moves its messages into the archive.

```json
{
  "DeadLetterManagement": {
    "DatabasePath": "/var/lib/messaging/dlq.db",
    "Sources": { "orders.commands.v2.dlq": ["orders.commands.v2"] },
    "MaxReplayAttempts": 1
  }
}
```

`GET /api/dead-letters` supports `after`, `limit`, and `status`. `GET /api/dead-letters/counts` returns counts by status. `GET /api/dead-letters/{id}` requires payload access. `POST /api/dead-letters/{id}/replay`, `archive`, and `discard` require their respective permissions. For batch operations use `/api/dead-letters/batch/{action}` and an array of up to 20 IDs. An uncertain replay remains available for operator review. [Authorization and limitations](../ARCHITECTURE.md)
