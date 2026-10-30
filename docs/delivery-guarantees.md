# NexusGrid Delivery Semantics and System Guarantees

## 1. Core Guarantee: At-Least-Once Delivery

NexusGrid provides **at-least-once message delivery** across the event ingestion pipeline, from client HTTP entry to durable stream storage and downstream columnar aggregation.

NexusGrid does **not** claim an end-to-end distributed ACID transaction spanning PostgreSQL, Kafka, Redis, ClickHouse, and Object Storage. Instead, it enforces reliability through idempotent writes, bounded deduplication, dead-letter routing, and reconciliation.

## 2. Ingestion-to-Log Delivery Protocol

1. **Ingress Validation**: Events arriving at `POST /v1/events` or `POST /v1/events:batch` are validated against the canonical JSON Schema draft-07. Malformed payloads or invalid ULIDs are rejected immediately with HTTP `400` / `422` and never reach Kafka.
2. **Idempotency Defense Window**:
   - Ingestion requests carrying an `Idempotency-Key` header acquire a distributed lock in Redis (`SET idempotency:{tenant_id}:{key} IN_PROGRESS NX PX 30000`).
   - If an in-progress operation is detected, HTTP `409 Conflict` is returned.
   - If completed, the cached HTTP `202 Accepted` response is returned with header `X-Cache-Lookup: HIT`.
3. **Durable Publication**: The event is partitioned and committed to Apache Kafka before the HTTP `202 Accepted` acknowledgement is returned to the client. If Kafka publish fails, an HTTP `503 Service Unavailable` is returned, signalling the client to retry.

## 3. Partitioning and Ordering Scope

- **Partition Key**: Events are partitioned by `hash(tenant_id + ":" + project_id)`.
- **Per-Project Total Ordering**: Total ordering is strictly guaranteed within a single `(tenant_id, project_id)` partition.
- **Cross-Project Concurrency**: Different projects and tenants are processed concurrently across different topic partitions.

## 4. Consumer Semantics and Idempotent Downstream Writes

- **Consumer Groups**: Stream workers operate in horizontally scalable consumer groups.
- **Batch Processing**: Workers buffer up to 5,000 events or 500ms before bulk writing to ClickHouse and archiving to S3.
- **Offset Commits**: Offsets are committed back to Kafka only **after** the downstream batch write completes successfully.
- **ReplacingMergeTree Deduplication**: Downstream ClickHouse storage utilizes `ReplacingMergeTree(received_at)` keyed by `(tenant_id, project_id, event_name, occurred_at, event_id)` to deduplicate multiple deliveries of the identical `event_id`.

## 5. Failure Classification & Dead-Letter Queue (DLQ)

Workers categorize processing failures into three distinct classes:

| Failure Type | Definition | Action |
|:---|:---|:---|
| **Retriable Failure** | Downstream network blip, temporary DB pool exhaustion, transient timeout | Route to backoff retry topics (`events.retry.10s`, `events.retry.1m`, `events.retry.5m`) with incremented `x-retry-count`. |
| **Non-Retriable / Poison** | Corrupted bytes, unparseable JSON, schema violation | Bypass retries; forward immediately to `nexusgrid.events.dlq` with failure headers and diagnostic error stack. |
| **Terminal Exhaustion** | Any event exceeding `x-retry-count >= 5` | Forward to `nexusgrid.events.dlq`. |

## 6. Dead-Letter Queue Replay Tooling

An operator CLI script (`scripts/replay-dlq.ts`) enables operators to:
1. Inspect queued dead-letter messages in `--dry-run` mode without acknowledging offsets.
2. Filter by `tenant_id`, `error_type`, or time range.
3. Resubmit patched messages back into the primary topic (`nexusgrid.events.raw`) with `--execute`.

## 7. Transactional Outbox for PostgreSQL Integration Events

When metadata events originate in PostgreSQL (such as tenant creation, API key rotation, or project configuration changes), they are written to the `outbox` table in the **same database transaction**. The `outbox-publisher` worker scans uncommitted outbox rows using `SELECT ... FOR UPDATE SKIP LOCKED` and publishes them to Kafka, ensuring zero integration event loss even during service crashes.

