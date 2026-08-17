# Distributed Delayed-Task & Throttling Engine

A high-throughput, distributed delayed-task execution and rate-limiting engine in Go. The engine ingests deferred or immediate background jobs (e.g. webhooks, transactional processing, notifications, scheduled reconciliation) via an HTTP/gRPC API, buffers and schedules them using an in-memory hierarchical timing wheel, enforces multi-tenant concurrency limits using an atomic token-bucket rate limiter, and dispatches them across horizontally scaled worker nodes with distributed visibility leases in Redis to prevent double execution.

## Features & Core Architecture

- **At-Least-Once Execution**: Guarantees task execution with automated crash recovery and visibility lease timeouts.
- **Hierarchical In-Memory Timing Wheel**: $O(1)$ task scheduling and slot ticking for microsecond ingestion and execution triggers.
- **Atomic Multi-Tenant Token-Bucket Limiter**: Prevents noisy-neighbor starvation by enforcing per-tenant rate limits (burst capacity & refill rate).
- **Bounded Worker Pool**: Goroutine pool with strict concurrency limits, context cancellation, and zero-loss graceful shutdown (`sync.WaitGroup`).
- **Distributed Lease Management (Redis ZSET & SETNX)**: Multi-node visibility leases prevent task double-execution.
- **Dead-Letter Queue (DLQ) & Exponential Backoff**: Automated retries for failed tasks with routing to DLQ upon exceeding `max_retries`.

---

## Directory Structure

```text
.
├── cmd/
│   ├── engine/
│   │   └── main.go              # Core Engine entry point & local concurrency driver
│   └── loadtest/
│       └── main.go              # Concurrent traffic & benchmark load generator (Phase 4)
├── internal/
│   ├── api/                     # Ingestion API & router (Phase 2)
│   ├── config/                  # Engine configuration parser
│   ├── limiter/
│   │   ├── token_bucket.go      # Per-tenant token-bucket rate limiter logic
│   │   ├── manager.go           # Multi-tenant rate limiter registry
│   │   └── token_bucket_test.go # Concurrency & rate-limit unit tests
│   ├── models/
│   │   └── task.go              # Task struct, status transitions, validation logic
│   ├── queue/
│   │   ├── timing_wheel.go      # Circular ring buffer timing wheel scheduler
│   │   └── timing_wheel_test.go # Timing wheel unit tests
│   └── worker/
│       ├── executor.go          # Task execution & retry/limiter handling
│       ├── pool.go              # Bounded worker pool & graceful shutdown
│       └── pool_test.go         # Worker pool unit tests
├── high_level_architecture.md   # Architectural overview & design details
└── README.md
```

---

## Phase 1 Quickstart

### Running Unit Tests (with Race Detector)
```bash
go test -v -race ./...
```

### Running Local Engine Driver
```bash
go run cmd/engine/main.go
```

---

## Implementation Roadmap

- [x] **Phase 1: Local Concurrency & Core Pipeline** (Models, TokenBucket Limiter, TimingWheel, WorkerPool, Race-free tests)
- [ ] **Phase 2: Ingestion API & Redis Persistence** (`POST /v1/tasks`, Redis ZSET polling, graceful shutdown)
- [ ] **Phase 3: Distributed Locks & Fault Tolerance** (`SETNX` visibility leases, exponential backoff, DLQ, watchdog heartbeat)
- [ ] **Phase 4: Benchmarking & Load Testing** (`cmd/loadtest`, multi-node validation, performance profiling)
