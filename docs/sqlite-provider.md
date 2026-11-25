# SQLite provider

Select `Sqlite` and call `AddSqliteIdempotency(rabbit.GetSection("Idempotency:Sqlite"))`. Set `DatabasePath` to a persistent local file. The directory must allow creation of the database and WAL files.

SQLite serializes writes and is suitable for local deployments. For distributed workloads, use a shared provider. [PostgreSQL](postgresql-provider.md)
