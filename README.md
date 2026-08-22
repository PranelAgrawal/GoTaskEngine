# System Specification: Distributed Delayed-Task & Throttling Engine

**Target Stack:** Go 1.22+, Redis 7+, PostgreSQL (Optional for long-term state), Docker & Docker Compose  
**Architecture Style:** Distributed Worker Pool, Communicating Sequential Processes (CSP), Token-Bucket Rate Limiter, In-Memory Timing Wheel, Distributed Lease Coordination.

---

## Executive Summary & Objective

The goal of this project is to build a high-throughput, distributed delayed-task execution and rate-limiting engine in Go. The engine ingests deferred or immediate background jobs (such as webhooks, transactional processing, notifications, and scheduled reconciliation) via an HTTP/gRPC API, buffers and schedules them using an in-memory hierarchical timing wheel, enforces multi-tenant concurrency limits using an atomic token-bucket algorithm, and dispatches them across horizontally scaled worker nodes with distributed visibility leases in Redis to prevent double execution.

The system guarantees:

- **At-Least-Once Execution** with automated crash recovery.
- **Microsecond Ingestion Latency** via non-blocking asynchronous dispatch.
- **Fair Tenant Throttling** to prevent noisy-neighbor starvation.
- **Zero-Loss Graceful Termination** using Go `context.Context` and `sync.WaitGroup`.

---

## Core Functional Modules

### Module 1: Ingestion & API Layer

- **Endpoint:** `POST /v1/tasks`
  - Accepts JSON payload containing `tenant_id`, `execute_at` (or `delay_seconds`), `action`, `endpoint`, `payload`, `max_retries`, and `idempotency_key`.
- **Validation & Fast Ack:**
  - Validates schema and checks idempotency key against Redis to avoid duplicate scheduling.
  - Returns `202 Accepted` immediately with metadata `{ task_id, status: "SCHEDULED" }`.
  - Enqueues task metadata to durable storage (Redis `ZSET` or Streams).

### Module 2: In-Memory Timing Wheel Scheduler

- **Design:** Circular buffer (ring of time slots) representing seconds/minutes.
- **Mechanics:**
  - Background ticker (`time.NewTicker`) advances the wheel every tick interval (e.g., 100ms or 1s).
  - Fetches tasks allocated to the current slot in $O(1)$ time.
  - Pushes due tasks directly to the internal task channel `chan Task`.

### Module 3: Multi-Tenant Token-Bucket Rate Limiter

- **Design:** In-memory map of tenant rate limiters protected by concurrent locks or atomic CAS operations.
- **Behavior:**
  - Each tenant has an allocated capacity (burst) and refill rate (tokens/second).
  - When a task for tenant `T` is popped, the worker queries `limiter.Allow(tenantID)`.
  - **Allowed:** Worker proceeds with execution.
  - **Throttled:** Task is deferred back to the queue or delayed to prevent starvation of other tenants.

### Module 4: Goroutine Worker Pool & Execution

- **Design:** Fixed number of worker goroutines consuming from a buffered `chan Task`.
- **Execution Lifecycle:**
  1. Claim task lock in Redis via atomic `SET lock:task:<id> <node_id> NX EX <lease_seconds>`.
  2. Execute HTTP webhook or simulated work payload.
  3. On success: Mark task completed in Redis/DB and release lock.
  4. On failure: Increment retry count, apply exponential backoff (e.g., $2^{	ext{retry}} 	imes 1	ext{s}$), and re-enqueue.
  5. On exceeding `max_retries`: Route task to Dead-Letter Queue (DLQ).

### Module 5: Distributed Lease Recovery & Heartbeats

- **Heartbeat Thread:** Each node updates `SET node:heartbeat:<node_id> timestamp EX 10` periodically.
- **Reaper / Watchdog:** If a node crashes mid-execution, its visibility lease expires. A background reaper process detects orphaned tasks and puts them back into the scheduling pool.

### Module 6: Graceful Shutdown

- Catches `SIGINT` and `SIGTERM`.
- Closes the HTTP ingestion server, stops the timing wheel ticker, closes internal task channels, and waits on `sync.WaitGroup` until all in-flight workers complete their current jobs.

---

## Phased Implementation Roadmap

### Phase 1: Local Concurrency & Core Pipeline

- [ ] Initialize Go module and setup project structure.
- [ ] Define `Task` struct and state transitions (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `DLQ`).
- [ ] Implement `TokenBucket` rate limiter with atomic operations and unit tests.
- [ ] Implement the `TimingWheel` ring buffer scheduler.
- [ ] Build the in-memory bounded `WorkerPool` using Go channels and `sync.WaitGroup`.
- [ ] Verify thread safety with `go test -race ./...`.

### Phase 2: Ingestion API & Persistence

- [ ] Implement `POST /v1/tasks` handler with JSON schema validation.
- [ ] Integrate Redis client (`go-redis/v9`) for persisting scheduled tasks in a Sorted Set (`ZSET`).
- [ ] Implement the task poller that feeds Redis scheduled jobs into the local timing wheel.
- [ ] Implement graceful shutdown with signal trapping and context timeouts.

### Phase 3: Distributed Locking & Fault Tolerance

- [ ] Implement Redis-based distributed leases (`SETNX` with TTL) for worker task claiming.
- [ ] Implement exponential backoff retry policy and Dead-Letter Queue routing.
- [ ] Add node heartbeat mechanism and orphaned task reclamation.
- [ ] Dockerize the application and set up `docker-compose.yml` with 3 worker node replicas.

### Phase 4: Benchmarking & Load Testing

- [ ] Build `cmd/loadtest` capable of spawning 50+ concurrent goroutines pushing 5,000+ tasks/sec.
- [ ] Measure throughput (tasks/sec), p95/p99 execution latency, and lock contention under multi-node execution.
- [ ] Write integration test simulating node crashes (`docker kill`) to verify zero lost tasks.

---
