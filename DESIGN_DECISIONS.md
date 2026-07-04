# Design Decisions & Architectural Trade-offs

This document outlines key technical decisions, architectural trade-offs, and scaling limits chosen during the design and implementation of the Distributed Job Scheduler.

---

## 1. Collapse of Claimed and Running States

### Decision
In the database schema and job state machine, we deliberately collapsed the transitional `claimed` and execution `running` states into a single `running` state.

### Rationale
- **Zero Network/Handoff Hop:** Since the polling worker is a single async process, once it successfully claims a job via row-level locking, it immediately dispatches the task for execution using `asyncio.create_task`. There is no remote coordinator, network hop, or secondary scheduling delay.
- **Write Amplification Reduction:** Introducing a separate `claimed` state would require an extra database `UPDATE` write operation for every single job execution. Eliminating it saves 1 database roundtrip per execution.

---

## 2. Queue-Scoped Idempotency Keys

### Decision
Idempotency constraints are defined at the queue level rather than globally across the system. This is implemented via a composite unique index:
```sql
CREATE UNIQUE INDEX ix_jobs_queue_id_idempotency_key 
ON jobs (queue_id, idempotency_key) 
WHERE idempotency_key IS NOT NULL;
```

### Rationale
- **Cross-Queue Reuse:** This enables clients to use identical idempotency identifiers (e.g. standard request UUIDs) across different queues (e.g. `send-welcome-email` and `generate-invoice`) without name collisions.
- **Pre-flight checking:** The submission endpoint performs a fast pre-flight check and handles Postgres `IntegrityError` collisions to return `200 OK` with the existing job ID and the header `X-Idempotent-Replay: true`.

---

## 3. Partial Claim Index

### Decision
We replaced the global composite index on `(queue_id, status, run_at)` with a partial index:
```sql
CREATE INDEX ix_jobs_claim_lookup 
ON jobs (queue_id, status, run_at) 
WHERE status IN ('queued', 'scheduled');
```

### Rationale
- **Index Size Optimization:** Over time, the vast majority of jobs in a production system transition to terminal states (`completed`, `failed`, or `dead_letter`).
- **Performance Preservation:** Polling queries only ever search for jobs in `queued` or `scheduled` states. Excluding completed and dead-lettered jobs keeps the lookup index compact and entirely in memory, preventing lookup degradation as job history grows.

---

## 4. Job Dependency DAG Claiming Gating

### Decision
Job dependency constraint validation is folded directly into the database transaction's worker `CLAIM` query via a `NOT EXISTS` clause:
```sql
AND NOT EXISTS (
    SELECT 1
    FROM job_dependencies jd
    JOIN jobs dep ON jd.depends_on_job_id = dep.id
    WHERE jd.job_id = j.id
      AND dep.status != 'completed'
)
```

### Trade-off & Scaling Limit
- **Dependency Gating Safety:** This guarantees that a worker will never claim or lock a child job before all its parent jobs reach a `completed` state. Since the check is executed within the same atomic transaction that locks the target row (`FOR UPDATE SKIP LOCKED`), it is immune to race conditions.
- **Scan-past Backlog Overhead:** Candidate jobs are locked and evaluated in chronological `run_at` order. If a queue has a backlog of many blocked child jobs at the front (oldest `run_at`), the claim query will scan past them and evaluate the `NOT EXISTS` subquery for each, before reaching the first unblocked job.
- **Scope Limit:** For this project's scope, this overhead is a known and acceptable trade-off. In ultra-high scale systems, this can be optimized by maintaining a dynamic "unblocked" index or utilizing an event-driven graph solver.
