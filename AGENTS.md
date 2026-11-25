# Messaging — инструкции за Codex

**Current product direction:** use `CourierLane.*` for NuGet package IDs. Write user-facing documentation, diagram labels, the offline guide, and the Management Web UI in English. This supersedes the earlier Bulgarian documentation requirements in the historical handoff below. Public publishing remains deferred.

## Мисия и начин на работа
Разработи преизползваема, лека .NET 10 библиотека за микросървисна комуникация чрез RabbitMQ, с Commands, RPC, Events, retry, DLQ, idempotency и transactional outbox. Крайната цел е release v1.0.0, модулни NuGet пакети, документация на български, визуални схеми и работещи примери.

Работна среда: Linux, code-server, GitHub. Repository: `/mnt/storage/Programming/Projects/Messaging`. Преди промени прочети solution-а, csproj, реалния код, конфигурацията и git status. Този файл е handoff от обсъжданията, а не проверен snapshot на repository. Реалният код е източник на истина за текущата имплементация; при конфликт документирай разликата. Не презаписвай чужди промени. Не рефакторирай работещи компоненти без причина. Не измисляй API на RabbitMQ клиента; провери инсталираната версия и компилирай. Работи на малки проверими етапи, изпълнявай build/test след всеки, обновявай ROADMAP.md, ARCHITECTURE.md и CHANGELOG.md. Не твърди, че тест е преминал, ако не е изпълнен. При липсваща инфраструктура отбележи blocker и предостави команда за проверка. Не публикувай пакети, не push-вай и не извършвай разрушителни операции без разрешение. Не включвай credentials в git. Използвай разумни обратими defaults; документирай решенията и питай при промяна на публичния API или съществена архитектурна дилема.

## C# стил (задължителен)
- Private fields без `_`, достъп чрез `this.field`.
- Не използвай `sealed` за класове.
- Параметрите на конструктори и методи на един ред.
- Винаги фигурни скоби при if/else/while/for/foreach, включително едноредови тела.
- Async API с CancellationToken; nullable reference types; ясни имена; минимални зависимости.
- Запази съществуващите namespaces и публични договори, освен при обоснована и документирана промяна.

## Текуща структура
- `src/Messaging.Abstractions`
- `src/Messaging.RabbitMQ`
- `samples/Messaging.ConnectionTest` — Worker за commands + RPC
- `samples/Messaging.ProducerTest` — producer

Съществуващи публични договори:
```csharp
Task SendAsync<TCommand>(string destination, TCommand command, CancellationToken cancellationToken = default);
Task<TResponse> RequestAsync<TRequest, TResponse>(string destination, TRequest request, RequestOptions? options = null, CancellationToken cancellationToken = default);
// ICommandHandler<TCommand>.HandleAsync(TCommand, CancellationToken)
// IRequestHandler<TRequest,TResponse>.HandleAsync(TRequest, CancellationToken)
```
Провери точните сигнатури в кода. DI API:
```csharp
builder.Services.AddRabbitMessaging(builder.Configuration.GetSection("RabbitMessaging"));
builder.Services.AddRabbitMessageContracts(contracts => contracts.Register<CreateOrderCommand>("orders.create.v1"));
builder.Services.AddRabbitListener(listener => listener.Listen("orders.commands.v2").Handle<CreateOrderCommand, CreateOrderHandler>("orders.create.v1"));
```
RPC queue: `orders.queries`. Command queue: `orders.commands.v2`.

## Съществуваща архитектура
- RabbitConnectionManager: singleton, AmqpEnvironment.Create и ConnectionSettingsBuilder (Host, Port, User, Password, VirtualHost).
- RabbitTopologyManager: queue declaration, quorum по подразбиране, direct DLX `<queue>.dlx`, DLQ `<queue>.dlq`, binding и dead-letter arguments. `IExchangeSpecification.Durable(true)` НЕ съществува в използвания API.
- RabbitMessageProducer: JSON (JsonSerializerDefaults.Web), AMQP property `message-type`, content type application/json, MessageId GUID N, Durable(true); cache на publishers чрез ConcurrentDictionary<string, Lazy<Task<IPublisher>>>; проверява OutcomeState.Accepted. Провери cleanup на failed cached tasks и disposal/reconnect.
- MessageDispatcher: dispatch по queue и message-type, DI scope на invocation, JSON deserialize, UnknownMessageTypeException.
- RabbitQueueConsumer: ConsumerBuilder().Queue(...).InitialCredits(...).MessageHandler(...).BuildAndStartAsync(...); Accept при успех, Discard при terminal failure, Requeue при cancellation. Провери реалната concurrency семантика, MaxConcurrency и backpressure, не приемай че credit е concurrency.
- MessageListenerService: hosted service, topology, commands consumers и RabbitRpcResponder.
- RabbitRpcClient/RabbitRpcResponder: requester/responder, RpcResponseEnvelope, RpcTimeoutExecutor, default timeout 30s. Провери timeout, cancellation, correlation, remote errors и lifecycle.
- RetryExecutor: exponential backoff, 3 attempts, 2s/4s delay по defaults, max 30s. IRetryExceptionClassifier и DefaultRetryExceptionClassifier: без retry за NonRetryableMessageException, UnknownMessageTypeException, JsonException. DI singleton classifier. Cancellation не е business failure. Не retry-вай permanent errors. Retry се изпълнява в рамките на lease.
- Namespaces: `Messaging.RabbitMQ.Configuration.RabbitMessagingOptions`, `Messaging.RabbitMQ.Options.IdempotencyOptions`, `Messaging.Abstractions.Exceptions.NonRetryableMessageException`, `Messaging.RabbitMQ.Dispatching.UnknownMessageTypeException`.
- Options се инжектират чрез IOptions<RabbitMessagingOptions>, не директно RabbitMessagingOptions (предишен DI проблем, вече решен).

## Конфигурация и defaults
```json
{
  "RabbitMessaging": {
    "Host": "localhost", "Port": 5672, "Username": "guest", "Password": "guest", "VirtualHost": "/",
    "Rpc": { "DefaultTimeout": "00:00:30" },
    "Consumer": {
      "AutoDeclare": true, "QueueType": "Quorum", "Credit": 50, "MaxConcurrency": 10,
      "Retry": { "MaxAttempts": 3, "InitialDelay": "00:00:02", "MaxDelay": "00:00:30", "BackoffMultiplier": 2 },
      "DeadLetter": { "Enabled": true, "Suffix": ".dlq" }, "EnableIdempotency": true
    },
    "Idempotency": { "Provider": "InMemory", "ProcessingTimeout": "00:05:00", "RetentionPeriod": "7.00:00:00" }
  }
}
```
EnableIdempotency е false по default в библиотеката, true само в sample конфигурацията за тестове. Guest credentials са локален пример, не production препоръка. Стартирай samples от техните директории, иначе content root може да е грешен и appsettings да не се зареди.

## Idempotency — завършена стъпка 9.4.2
В `Messaging.Abstractions.Idempotency`:
```csharp
public record MessageLease(string Key, string Token);
public record MessageAcquireResponse(MessageAcquireResult Result, MessageLease? Lease = null);
public enum MessageAcquireResult { Acquired, AlreadyCompleted, InProgress }
public interface IMessageDeduplicationStore
{
    Task<MessageAcquireResponse> TryAcquireAsync(string key, CancellationToken cancellationToken = default);
    Task<bool> RenewAsync(MessageLease lease, CancellationToken cancellationToken = default);
    Task<bool> MarkCompletedAsync(MessageLease lease, CancellationToken cancellationToken = default);
    Task ReleaseAsync(MessageLease lease, CancellationToken cancellationToken = default);
}
```
InMemoryMessageDeduplicationStore използва ConcurrentDictionary<string, MessageEntry>, immutable record с Token, State, ExpiresAt; atomic TryAdd/TryUpdate/TryRemove с expected entry. При expired entry допуска нов token; Renew/Complete само за текущ processing token, който не е изтекъл; Complete задава RetentionPeriod; Release премахва само собствен processing entry. DI: TryAddSingleton<IMessageDeduplicationStore, InMemoryMessageDeduplicationStore>(). Provider настройката засега НЕ избира реален provider.

Consumer ProcessWithIdempotencyAsync: ако е disabled или липсва messageId, dispatch с retry; иначе key `$"{queueName}:{messageId}"`; AlreadyCompleted -> log и return (Accept от caller); InProgress -> delay 500ms и повторен acquire; Acquired -> dispatch с retry -> MarkCompletedAsync -> return; при exception ReleaseAsync със CancellationToken.None и rethrow. Не връщай стария bool TryAcquire дизайн: InProgress НЕ е AlreadyCompleted и НЕ трябва да се ACK-ва. Не въвеждай ненужен IdempotencyProcessingResult enum.

Ограничения: няма automatic renewal; lease lost в момента може да попадне в generic Discard; expired entries се заменят при достъп, няма cleanup; InMemory не е shared и губи състояние при restart; queue key трябва да отчита namespace/vhost/subscriber identity при разширяване. Lease token не е MessageId. Не обещавай exactly-once: side effects и dedup store не са атомарни; cancellation е кооперативен. Business idempotency/transactional inbox може да е необходим.

### Ръчно потвърдени тестове от потребителя
- Worker и Producer работят, commands + RPC.
- Retry permanent exception: един опит, после Discard.
- Фиксиран MessageId `test-idempotency-lease-001`: един handler execution, следващ duplicate skipped.
- RetentionPeriod 15s: първи обработен, втори skipped, трети след изчакване обработен без рестарт на Worker.
- Direct store test с ProcessingTimeout 3s, delay 4s: Acquired, InProgress, Acquired, old Renew=False, old Complete=False, old Release не засяга новия, new Renew=True, new Complete=True, AlreadyCompleted.
- Concurrent test с 5s handler delay не доказва реална паралелност на callback-ите; добави автоматизиран конкурентен тест.
- Последният тест е с временен код в Worker Program.cs след host build и `return;` (CS0162 очакван). Провери дали е премахнат. Възстанови ProcessingTimeout 5min, RetentionPeriod 7 days, MessageId(Guid.NewGuid().ToString("N")) в RabbitMessageProducer.SendAsync, не в sample SendAsync call. 5s handler delay може да остане за следващ тест. Не приемай, че тези възстановявания вече са направени.

## Roadmap и acceptance criteria
### 9.4.3 — Automatic Lease Renewal (следващата задача)
Добави отделен MessageLeaseManager или еквивалентен малък компонент. Периодично RenewAsync през целия dispatch/retry/backoff, например на 1/3 от ProcessingTimeout с валидирани граници. Използвай linked CTS за handler и отделно контролирай lifecycle на renewal loop. При false или несигурен store failure прекрати обработката кооперативно, не Accept; изчакай loop-а и handler-а безопасно, не оставяй fire-and-forget. Координирай renewal и MarkCompleted, за да няма race с вече completed lease. При lease loss/infrastructure failure requeue с подходящ backoff, не DLQ като business error. Release само собствен token; bounded cleanup, не маскирай original exception. Разграничи shutdown, handler terminal failure, lease loss, store failure. Не допускай busy requeue loop. Ако handler игнорира cancellation, документирай ограниченията. Тестове: handler по-дълъг от lease, renew през retry, duplicate while processing, renewal false/exception, shutdown, completion race, stale owner, липса на ACK при загубен lease. Първо build/test на тази стъпка.

### 9.4.4 — Provider Selection & Validation
InMemory default; избор чрез Idempotency.Provider, explicit DI registration на provider extensions; fail-fast за unknown/missing/mismatched provider, без silent fallback. Validate positive timeouts, retention, retry и consumer settings. Запази core независим от Redis/SQL. Добави bounded InMemory expired-entry cleanup. Документирай behavior при disabled idempotency и липсващ message ID.

### 9.4.5 — Redis provider
Отделен `Messaging.Idempotency.Redis`, предложено `AddRedisIdempotency(...)`. Config: `Idempotency.Provider=Redis`, `Idempotency.Redis.ConnectionString`, `KeyPrefix=messaging:idempotency:`. Общи timeout/retention. Atomic acquire, compare-token renew/complete/release чрез Lua или еквивалентни атомарни операции; TTL, connection reuse, cancellation/timeouts, namespace isolation. Тестове с реален Redis, няколко Worker инстанции, restart, expiration, stale token. Документирай persistence/eviction/failover ограниченията. Не добавяй Redis dependency в RabbitMQ core.

### 10 — Event Publishing & Subscriptions
Универсален pub/sub, не само cache invalidation. Изрично изискване: broadcast към ВСЯКА активна абонирана инстанция, не само по един consumer на service. Topic exchange, event type routing key, отделна уникална queue на instance (предложено exclusive/auto-delete classic). Offline instances пропускат broadcast; документирай reconnect и lifecycle. Опционален CompetingConsumers режим с durable shared queue за logical subscriber. Broadcast default. Предложен API `producer.PublishAsync("business.events", new OrderCreatedEvent(orderId));`, `AddRabbitEventListener(events => events.Subscribe("business.events").WithDeliveryMode(EventDeliveryMode.Broadcast).Handle<OrderCreatedEvent, OrderCreatedHandler>("orders.created.v1"));` — финализирай след инспекция. Dedup scope включва subscriber/instance identity и при нужда handler identity; не блокирай други получатели. DLQ за ephemeral queues трябва да има ясна durable стратегия и replay към конкретен subscriber. Тест: 2+ инстанции получават всяка event; competing mode обработва от една.

### 11 — DLQ Management & Replay API
IDeadLetterManager: inspect/list payload, headers, metadata, failure reason когато е наличен, replay single/bulk, archive/discard, monitoring. Не използвай destructive browsing без изрично документирана семантика. RabbitMQ discard не записва автоматично app exception reason; избери безопасен failure metadata механизъм. Replay запазва MessageId, original routing/subscriber identity; publish confirm преди ACK на DLQ; документирай duplicate window и dedup implications; предотврати replay loops. Не rebroadcast-вай към всички при replay на единичен subscriber. Auth/authz, audit, redaction, paging, ограничени batch операции. REST API отделно от core.

### 12 — DLQ Management Web UI
Отделно приложение `Messaging.Management.Web`: списък, филтри, детайли, replay, archive/discard, потвърждение за разрушителни операции, статуси и monitoring. Без credentials/payload secrets в logs. UI използва management API.

### 13/14 — PostgreSQL и SQLite providers
Отделни `Messaging.Idempotency.PostgreSql` и `Messaging.Idempotency.Sqlite`. Същият contract и conformance tests като Redis/InMemory. Atomic conditional updates, unique key, token ownership, expiration, retention cleanup, migrations, concurrency. PostgreSQL за distributed workloads; SQLite за подходящи локални deployment-и, не обещавай distributed locking през произволен network filesystem. Без SQL dependencies в RabbitMQ core.

### 15 — SendOptions & Message Metadata
Caller-supplied MessageId (за outbox/replay), CorrelationId, custom headers; timestamp, source, causation ID; consistent metadata за commands/RPC/events. Backward-compatible overloads; не позволявай reserved headers да нарушават routing/type. Не използвай lease token като MessageId. Документирай contract versioning (`orders.create.v1`). Ако outbox/replay изисква metadata по-рано, реализирай минималния необходим API и отбележи промяната в roadmap.

### 16 — Transactional Outbox
Атомарен запис на business state + outbox record в една DB transaction, background publisher с confirm, stable MessageId, retries, concurrency-safe claiming, crash recovery, cleanup и observability. Не обещавай exactly-once; consumer idempotency остава нужна. Избери storage integration след преглед на архитектурата, без да налагаш ORM на core. Тестове за crash преди/след publish confirm, restart, duplicate delivery.

### 17 — Unit & Integration Tests
Добавяй тестове още при всяка стъпка; тук финализирай coverage. Unit tests за options, classifier, retry, dispatcher, lease lifecycle, metadata; shared provider conformance suite; integration tests с RabbitMQ, Redis, PostgreSQL, SQLite, multi-instance broadcast, RPC timeout/cancellation, DLQ replay, shutdown/reconnect, outbox crash scenarios. Използвай deterministic clocks когато е разумно, не flaky sleep-only tests. Docker Compose за локална инфраструктура; Testcontainers е възможен избор, не задължителен. Не използвай production ресурси.

### 18 — Logging, Metrics & OpenTelemetry
Structured logs, ActivitySource/Meter, trace propagation, message correlation, latency, throughput, retries, duplicate skips, lease losses, DLQ depth когато е достъпна, outbox backlog. Избягвай high-cardinality metric labels и sensitive payload logging. Health/readiness и graceful shutdown; провери reconnect, disposal, resource bounds, publisher cache, TLS и least-privilege credentials.

### 19 — Документация и визуални схеми
Документация на български с английска техническа терминология, versioned в GitHub. README quick start, installation, prerequisites, compatibility, packages, minimal working example. `docs/getting-started.md`, `architecture.md`, `configuration.md`, `commands.md`, `rpc.md`, `events.md`, `retry.md`, `idempotency.md`, `redis-provider.md`, `postgresql-provider.md`, `sqlite-provider.md`, `dead-letter-queues.md`, `dlq-management.md`, `transactional-outbox.md`, `troubleshooting.md`, `deployment.md`, `diagrams/`. За всяка функция: защо, как работи, конфигурация с defaults/validation, C# пример, ограничения, troubleshooting. Mermaid source + визуални exports при възможност: component/package dependency, command sequence, RPC, broadcast vs competing, retry/DLQ, lease state machine/renewal, provider architecture, replay, outbox. Схемите да съответстват на реалния код. Подготви и PDF ръководство като офлайн справочник. Поддържай ROADMAP.md, ARCHITECTURE.md (ADR при нужда), CHANGELOG.md от сега.

Работещи samples: CommandSample, RpcSample, EventSample, IdempotencySample, DeadLetterSample, Management.Web; може да запазиш текущите samples и да ги разшириш вместо ненужно дублиране. Docker Compose, инструкции за start/stop, sample configs без реални secrets. Провери всички документирани snippets срещу buildable samples.

### 20 — NuGet Packaging & CI/CD
Предварителни package IDs: Messaging.Abstractions, Messaging.RabbitMQ, Messaging.Idempotency.Redis, Messaging.Idempotency.PostgreSql, Messaging.Idempotency.Sqlite, Messaging.Management (и outbox пакет ако е обосновано). Провери уникалност преди публикуване; собствен prefix при нужда. .NET 10 target; не добавяй други target frameworks без причина. SemVer, package metadata, README, XML docs/IntelliSense, license (избор от собственика), repository URL, symbols/SourceLink при възможност. GitHub Actions restore/build/test/pack; publish workflow с explicit release trigger и secure credentials, без автоматично публично публикуване по време на разработката. Поддържай local feed и документирай NuGet.org / GitHub Packages. Провери install на packed packages в чист consumer project извън solution-а, transitive dependencies, DI и quick start.

### 21 — Release v1.0.0
Release само след реално преминали tests, review на public API, security/lifecycle review, проверени NuGet artifacts, документация и примери. Документирай известни ограничения, delivery guarantees (at-least-once при подходяща durable конфигурация, best-effort active-instance broadcast), compatibility и migration policy. Не публикувай без изрично разрешение на собственика.

## Първа задача за Codex
1. Инспектирай repository, git status, SDK, dependency versions, текущите имплементации и временния test code.
2. Направи baseline build/test и кратък gap report; не спирай за незначителни обратими решения.
3. Създай/актуализирай ROADMAP.md, ARCHITECTURE.md, CHANGELOG.md с реалното състояние.
4. Реализирай 9.4.3 и тестовете; продължи по roadmap последователно, когато проверките преминават. При blocker запиши точна причина, команди и оставаща работа; не отбелязвай непроверено като готово.
5. След всеки етап отчети променени файлове, build/test резултати, решения, рискове и следващата задача. Не променяй scope към друг message broker, framework или ненужна инфраструктура.
