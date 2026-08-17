# SOLUTION.md — Convin Webhook Ingestion Service Fix & Extension

## 1. What was broken, and why

| Symptom reported by Operations | Root Cause | Fix Implemented |
| :--- | :--- | :--- |
| **Duplicate call records & account stats drifting higher** | 1. `EventExists()` followed by separate inserts was a check-then-act race condition.<br>2. `events.event_id` lacked a `UNIQUE` constraint.<br>3. `events`, `calls`, and `account_stats` writes were not wrapped in a database transaction, allowing concurrent redeliveries of the same `event_id` to increment statistics multiple times. | Added `UNIQUE(event_id)` index in schema (`migrations/002_idempotency.sql` & `001_init.sql`). Wrapped ingestion in a single atomic Postgres transaction (`pgx.Tx`) with `INSERT ... ON CONFLICT (event_id) DO NOTHING`. If `RowsAffected == 0`, duplicate ingestion returns `200 OK` idempotently without double-counting. |
| **Recordings never marked processed (silent failure)** | 1. `processRecording` was spawned as a background goroutine passing `ctx` from `r.Context()`. As soon as the HTTP handler returned `200 OK`, Go's HTTP server canceled `r.Context()`, causing the subsequent database update to immediately fail with `context canceled`.<br>2. Errors from `processRecording` were swallowed (`// TODO: handle`) with zero logging. | Decoupled background recording processing from the HTTP request context by using a long-lived service background context. Added structured error logging via `s.log.Error(...)` on processing failures. |
| **In-flight work lost on deployment** | The server handled SIGTERM by calling `srv.Shutdown(ctx)`, which only drained HTTP connections. Asynchronous recording goroutines had no lifecycle tracking or synchronization, so when `main()` exited, connections closed and in-flight tasks were abruptly terminated. | Introduced `sync.WaitGroup` tracking and a dedicated `Service.Shutdown(ctx)` method called during graceful termination in `cmd/server/main.go`, allowing in-flight recording tasks to finish before exit. |
| **Data race in `stats.Cache`** | `Cache.Record()` mutated the map `c.m` and pointer values without holding `c.mu.Lock()`, causing data races and corrupted counts under concurrent load (`go test -race`). In addition, cold cache after restart returned 0 counts. | Acquired `c.mu.Lock()` in `Record()` and added `Set()`. Added cold-cache read-through fallback to Postgres in `Service.Stats()` so statistics remain accurate across service restarts. |

---

## 2. Why this deduplication strategy over alternatives

We considered three primary approaches to guarantee idempotent at-least-once webhook processing:

1. **Postgres Transactional Insert with Unique Constraint (Chosen Strategy)**:
   - **How it works**: An atomic transaction inserts the raw event with `INSERT INTO events ... ON CONFLICT (event_id) DO NOTHING`. If 0 rows are affected, it rolls back and returns early. If inserted, `calls` upsert and `account_stats` increment occur in the same transaction.
   - **Why chosen**: 
     - **Strict ACID Guarantees**: Guarantees zero phantom increments and exact consistency between raw events, call state, and account totals with no dual-write failure modes.
     - **Simplicity & Crash Safety**: If the app or network crashes midway, Postgres handles rollback automatically without orphaned locks or distributed state divergence.
     - **Zero State Drift**: Unlike external locks, the deduplication marker and the aggregate update commit together atomically.

2. **Redis `SETNX` / Distributed Locking (Alternative Considered)**:
   - **Trade-off**: Requires setting a lock/key with TTL (`SET event:evt_123 1 NX EX 86400`). If Redis crashes, loses a replica, or evicts the key under memory pressure, duplicate processing occurs. More importantly, two-phase distributed state between Redis and Postgres introduces distributed rollback failure modes (e.g. Redis key set, Postgres write fails, subsequent retries rejected as duplicates).

3. **Two-Tier (Redis Cache + Postgres DB Transaction) (Alternative Considered)**:
   - **Trade-off**: Fast-path deduplication in Redis before hitting Postgres. Highly effective for ultra-high throughput, but adds unnecessary architectural overhead for moderate workloads. Postgres transactions remain the single source of truth.

---

## 3. Scaling to 10,000 webhooks/second

To handle 10,000 webhooks/second sustainably without saturating Postgres connection pools or encountering row lock contention on `account_stats`:

1. **Decouple Ingestion from Processing via Distributed Message Queue (Kafka / AWS SQS / NATS JetStream)**:
   - The HTTP ingest tier becomes ultra-lightweight: validate JSON payload, push event to Kafka/NATS partitioned by `account_id`, and immediately respond with `202 Accepted` or `200 OK` (p99 latency < 5ms).
   - Ingestion nodes become completely stateless and horizontally scalable behind an API gateway / load balancer.

2. **Stream Processing & Batch Database Writes**:
   - Worker consumer groups pull events in micro-batches (e.g., 500–1,000 events/batch).
   - Use `COPY` or multi-row batch inserts (`INSERT INTO events (...) VALUES (...), (...)... ON CONFLICT DO NOTHING`) to maximize Postgres disk I/O throughput.

3. **Mitigate Account Stats Lock Contention (Buffer & Aggregate in Redis / Flink)**:
   - At 10k req/s, multiple webhooks for the same hot `account_id` will cause row-level lock contention on `account_stats`.
   - **Solution**: Increment running totals using Redis `HINCRBY` / Redis Streams or Flink tumbling/sliding windows in memory, and periodically flush consolidated deltas to Postgres `account_stats` asynchronously in bulk every few seconds.

4. **Dedicated Worker Pool for Recording Transcoding**:
   - Decouple recording download/transcoding into asynchronous background workers pulling from a dedicated Redis/SQS task queue with exponential backoff retries and Dead Letter Queues (DLQ).
