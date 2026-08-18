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
We acquired an exclusive write lock (`c.mu.Lock()`) and deferred its release (`defer c.mu.Unlock()`) at the start of `Record()`:
