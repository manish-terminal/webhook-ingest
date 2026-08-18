# Changes Made & Defect Documentation

## Fix 1: Thread-Safety Race Condition in In-Memory Stats Cache

### 1. What Was Happening
When multiple webhook events arrived concurrently for an account, Go's race detector reported `WARNING: DATA RACE` on `internal/stats/cache.go`. Under high production traffic, concurrent requests caused stats counter corruption or process crashes with `fatal error: concurrent map writes`.

### 2. Why It Was Happening
In Go, standard `map[string]*AccountStats` is **not safe for concurrent reads and writes**. 

While the read method `Get()` correctly acquired a read lock (`c.mu.RLock()`), the write method `Record()` in [`internal/stats/cache.go`](file:///Users/manishgupta/Documents/webhook-ingest/webhook-ingest/internal/stats/cache.go#L39) was missing an exclusive write lock:

```go
// BEFORE (Buggy)
func (c *Cache) Record(accountID string, durationSec int) {
    s, ok := c.m[accountID] // ❌ Unsafe concurrent map access!
    if !ok {
        s = &AccountStats{}
        c.m[accountID] = s
    }
    s.CallCount++
    s.TotalDurationSec += int64(durationSec)
}
```

When 2 or more webhooks were ingested simultaneously, multiple goroutines executed `Record()` at the exact same microsecond, colliding on the underlying map structure.

### 3. How It Was Fixed
We acquired an exclusive write lock (`c.mu.Lock()`) and deferred its release (`defer c.mu.Unlock()`) at the start of `Record()` in [`internal/stats/cache.go`](file:///Users/manishgupta/Documents/webhook-ingest/webhook-ingest/internal/stats/cache.go#L41).

---

## Fix 2: Silent Failure of Audio Recording Background Processing

### 1. What Was Happening
Webhook calls landed successfully, but call recordings were **never marked processed** (`recording_processed` remained `FALSE` in PostgreSQL), and no errors were logged.

### 2. Why It Was Happening
In [`internal/httpapi/handler.go`](file:///Users/manishgupta/Documents/webhook-ingest/webhook-ingest/internal/httpapi/handler.go#L29), `postCallWebhook` passed the HTTP request context (`r.Context()`) to `svc.Ingest()`. 

In [`internal/ingest/service.go`](file:///Users/manishgupta/Documents/webhook-ingest/webhook-ingest/internal/ingest/service.go#L78), the service launched a background goroutine passing that same HTTP context:

```go
// BEFORE (Buggy)
if rec.RecordingURL != "" {
    go func() {
        if err := s.processRecording(ctx, rec); err != nil { // ❌ Canceled HTTP Context!
            // TODO: handle
        }
    }()
}
```

As soon as `postCallWebhook` completed and responded `200 OK` to the caller, Go's `net/http` server **automatically canceled `r.Context()`**.

When `processRecording()` woke up after sleeping for 50ms (`time.Sleep(50ms)`), it called `s.store.MarkRecordingProcessed(ctx, rec.CallID)`. Because `ctx` was already canceled, PostgreSQL rejected the SQL update with `context canceled`. Because line 79 was an empty `// TODO: handle` block, the error was silently swallowed.

### 3. How It Was Fixed
1. We used `context.WithoutCancel(ctx)` in [`internal/ingest/service.go:77`](file:///Users/manishgupta/Documents/webhook-ingest/webhook-ingest/internal/ingest/service.go#L77) to detach the background worker context from the HTTP request lifecycle:

```go
// AFTER (Fixed)
if rec.RecordingURL != "" {
    bgCtx := context.WithoutCancel(ctx)
    go func() {
        if err := s.processRecording(bgCtx, rec); err != nil {
            s.log.Error("recording processing failed", "event_id", rec.EventID, "call_id", rec.CallID, "err", err)
        }
    }()
}
```

2. We added structured error logging (`s.log.Error(...)`) so any background failures are properly reported.

---

## Fix 3: Duplicate Call Records & Account Call Count Drift (Idempotency Enforcement)

### 1. What Was Happening
Operations reported that duplicate call records were appearing in the dashboard and account call-counts were drifting higher than the actual number of unique calls.

### 2. Why It Was Happening
1. **Schema Defect**: `migrations/001_init.sql` indexed `event_id` but lacked a `UNIQUE` constraint on `events(event_id)`.
2. **Race Condition in `Ingest()`**: When identical `event_id` webhooks arrived concurrently, both executed `s.store.EventExists(event_id)`. Both received `false`, inserted duplicate events, and incremented `account_stats.call_count` twice.
3. **Call Count Drifting**: When multiple webhooks arrived for the *same* `call_id` (e.g., status retries or updates), `s.store.IncrementAccountStats()` incremented `account_stats.call_count` for every *event* rather than only for *new unique calls*.

### 3. How It Was Fixed
1. **New Migration** ([`migrations/002_unique_event_id.sql`](file:///Users/manishgupta/Documents/webhook-ingest/webhook-ingest/migrations/002_unique_event_id.sql)):
   Added a `UNIQUE` index on `events(event_id)`:
   ```sql
   CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id_unique ON events (event_id);
   ```

2. **Atomic DB Deduplication** ([`internal/store/store.go:76`](file:///Users/manishgupta/Documents/webhook-ingest/webhook-ingest/internal/store/store.go#L76)):
   Updated `InsertEvent` to use `ON CONFLICT (event_id) DO NOTHING` and return `(inserted bool, err error)`:
   ```go
   func (s *Store) InsertEvent(ctx context.Context, e Event) (bool, error) {
       tag, err := s.pool.Exec(ctx,
           `INSERT INTO events (event_id, call_id, account_id, payload)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (event_id) DO NOTHING`,
           e.EventID, e.CallID, e.AccountID, e.Payload)
       if err != nil { return false, err }
       return tag.RowsAffected() > 0, nil
   }
   ```

3. **New Call Detection** ([`internal/store/store.go:88`](file:///Users/manishgupta/Documents/webhook-ingest/webhook-ingest/internal/store/store.go#L88)):
   Updated `UpsertCall` to use PostgreSQL's `RETURNING (xmax = 0) AS is_new` idiom to detect whether a brand-new call row was created:
   ```go
   func (s *Store) UpsertCall(ctx context.Context, e Event) (bool, error) {
       var isNew bool
       err := s.pool.QueryRow(ctx,
           `INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
            VALUES ($1, $2, $3, $4, $5, now())
            ON CONFLICT (call_id) DO UPDATE SET ...
            RETURNING (xmax = 0) AS is_new`, ...).Scan(&isNew)
       return isNew, err
   }
   ```

4. **Service Wiring** ([`internal/ingest/service.go:64-77`](file:///Users/manishgupta/Documents/webhook-ingest/webhook-ingest/internal/ingest/service.go#L64-L77)):
   In `Ingest()`, if `InsertEvent` reports `!inserted`, the duplicate webhook is ignored immediately (`return nil`). `IncrementAccountStats` and `cache.Record` are ONLY executed if `isNewCall == true`.



