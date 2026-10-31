# Messaging — reconstructed project (net10.0)

This archive reconstructs the source code from our Bulgarian ChatGPT conversation up through **Step 9.2: in-process exponential-backoff retry + DLQ**. It is **not an exact copy** of files on your work computer.

## Structure
- `src/Messaging.Abstractions`: handler interfaces, producer API, RPC response envelope and exceptions.
- `src/Messaging.RabbitMQ`: connection, topology, dispatch, producer, RPC, hosted consumers, retry.
- `samples/Messaging.ConnectionTest`: Worker listening to `orders.commands.v2` and `orders.queries`.
- `samples/Messaging.ProducerTest`: sends one command and one RPC request.

## Run
Requires .NET 10 SDK, RabbitMQ with AMQP 1.0 enabled (port 5672) and management UI optional.

```bash
dotnet restore Messaging.slnx
dotnet build Messaging.slnx
dotnet run --project samples/Messaging.ConnectionTest
dotnet run --project samples/Messaging.ProducerTest
```

The Worker deliberately throws on commands to test 3 attempts (2s and 4s waits) and DLQ `orders.commands.v2.dlq`; RPC succeeds.

## Important limitations / follow-up
- Dependency versions use floating `1.*` / `10.*` because the exact installed versions were not shared. Pin them to the working versions from the original machine before production. **The archive could not be compiled in this environment** (dotnet SDK not installed), so check API compatibility locally.
- For existing queues, DLX arguments can conflict with prior declarations. Test with new queues or manage dead-lettering via RabbitMQ policies.
- `Discard()` without configured DLQ may lose messages.
- Retry is in-process, not broker-delayed; crashes reset the attempt counter. Handlers should be idempotent.
- The consumer currently does not enforce `MaxConcurrency`, perform robust connection recovery, or classify permanent vs transient errors.
- RPC requester timeout is 10 minutes internally; per-request timeout starts after requester acquisition, and cancellation/pending cleanup needs review.
- The current dispatcher resolves handlers by interface; registering different handlers for the same CLR command type across queues needs keyed/type-specific resolution.
- Use secrets/environment variables for production credentials; `guest/guest` is local-only.

## Resume point
Next planned step: **9.3 — classify retryable vs permanent failures** (e.g. invalid JSON -> DLQ directly; transient DB error -> retry).
