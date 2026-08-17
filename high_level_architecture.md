# High-Level Architecture Specification: Distributed Delayed-Task & Throttling Engine

## System Overview

```
                                  +-----------------------+
                                  |   HTTP Ingestion API  |
                                  |     (POST /v1/tasks)  |
                                  +-----------+-----------+
                                              |
                                              v
                              +---------------+---------------+
                              |    Redis Stream / ZSET / DB   |
                              |   (Durable Persistence Layer) |
                              +---------------+---------------+
                                              |
                     +------------------------+------------------------+
                     |                                                 |
                     v                                                 v
        +----------------------------+                    +----------------------------+
        |       Worker Node 1        |                    |       Worker Node 2        |
        |  +----------------------+  |                    |  +----------------------+  |
        |  | In-Memory Timing     |  |                    |  | In-Memory Timing     |  |
        |  | Wheel (O(1) Ticks)   |  |                    |  | Wheel (O(1) Ticks)   |  |
        |  +----------+-----------+  |                    |  +----------+-----------+  |
        |             |              |                    |             |              |
        |             v              |                    |             v              |
        |  +----------------------+  |                    |  +----------------------+  |
        |  | Token Bucket Limiter |  |                    |  | Token Bucket Limiter |  |
        |  | (Per-Tenant Mutex)   |  |                    |  | (Per-Tenant Mutex)   |  |
        |  +----------+-----------+  |                    |  +----------+-----------+  |
        |             |              |                    |             |              |
        |             v              |                    |             v              |
        |  +----------------------+  |                    |  +----------------------+  |
        |  | Goroutine Worker     |  |                    |  | Goroutine Worker     |  |
        |  | Pool (Fixed Bound)   |  |                    |  | Pool (Fixed Bound)   |  |
        |  +----------------------+  |                    |  +----------------------+  |
        +--------------+-------------+                    +--------------+-------------+
                       |                                                 |
                       +------------------------+------------------------+
                                                |
                                                v
                              +-----------------+-----------------+
                              |       Redis Distributed Locks     |
                              |  (SETNX Visibility Leases / DLQ)  |
                              +-----------------------------------+
```

## Architectural Components

### 1. In-Memory Timing Wheel Scheduler
- **Design:** Ring buffer (array of slots representing discrete time buckets, e.g. 1-second ticks).
- **Complexity:** $O(1)$ task scheduling insertion and $O(1)$ slot expiration ticking.
- **Ticking:** A background Go `time.Ticker` advances the hand of the wheel at regular intervals, retrieving all tasks due in the active slot and passing them to the worker pipeline.

### 2. Multi-Tenant Token-Bucket Rate Limiter
- **Design:** Thread-safe per-tenant token buckets (`TokenBucket`) managed by a central `LimiterManager`.
- **Refill Mechanics:** Lazy fractional refill calculation based on elapsed time (`time.Since(lastRefill)`), eliminating the need for per-tenant background ticker goroutines.
- **Fairness:** Ensures noisy tenants who exceed their token rate get throttled without starving adjacent tenants.

### 3. Goroutine Worker Pool & Executor
- **Design:** Fixed number of worker goroutines consuming tasks from a bounded Go channel (`chan *Task`).
- **Execution:** Performs task dispatch (HTTP webhook or task payload execution), updates execution metadata, applies exponential backoff on retries, and enforces DLQ routing upon hitting `max_retries`.
- **Graceful Shutdown:** Implemented via `context.Context` cancellation and `sync.WaitGroup` tracking to ensure all in-flight tasks finish before the process terminates.

### 4. Distributed Visibility Leases & Recovery (Phase 3)
- Workers acquire task leases via Redis `SET key worker_id NX EX lease_ttl`.
- Expired visibility leases are reclaimed by watchdog processes to guarantee At-Least-Once execution across node failures.
