# PostgreSQL provider

Select `PostgreSql` and call `AddPostgreSqlIdempotency(rabbit.GetSection("Idempotency:PostgreSql"))`. Supply `ConnectionString` and grant the permissions needed for startup migration. Use the same database and prefix for instances that share deduplication state.

The migration creates `messaging_idempotency_v1`. SQL operations check the owner token and expiration. Acquire operations trigger bounded cleanup. [Architecture](../ARCHITECTURE.md)
