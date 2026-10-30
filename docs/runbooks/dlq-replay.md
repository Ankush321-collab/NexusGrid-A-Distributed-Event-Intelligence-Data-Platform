# NexusGrid Operational Runbook: Dead-Letter Queue (DLQ) Inspection & Safe Replay

## 1. Overview and Alert Triggers

- **Alert Name**: `NexusGridHighDLQRate`
- **Condition**: Messages published to `nexusgrid.events.dlq` exceeds 5 msgs/min or DLQ total count > 100.
- **Root Causes**:
  - Downstream ClickHouse syntax error or changed column types.
  - Poison-pill message (malformed payload bypassing edge validation).
  - Prolonged downstream outage exhausting the 5 backoff retry stages.

## 2. Step 1: Inspect DLQ in Dry-Run Mode

Before taking any mutative action, inspect the dead-letter messages without acknowledging or re-publishing them:

```bash
# Dry run for all tenants
pnpm tsx scripts/replay-dlq.ts --dry-run

# Dry run filtered to a specific customer tenant
pnpm tsx scripts/replay-dlq.ts --dry-run --tenant tenant_acme
```

Verify output headers:
- `x-original-topic`
- `x-error-reason`
- `x-error-stack`
- `x-retry-count`

## 3. Step 2: Categorize and Resolve Root Cause

1. If the error is **Corrupt JSON / Poison Payload**:
   - Determine if payload can be patched or if it should be permanently discarded to cold archival.
2. If the error is **ClickHouse Schema Mismatch**:
   - Apply the corresponding ClickHouse migration first (e.g. `ALTER TABLE nexusgrid.events ADD COLUMN ...`).
3. If the error was a **Temporary Downstream Outage**:
   - Confirm downstream ClickHouse/Kafka health has fully recovered.

## 4. Step 3: Execute Controlled Replay

Once downstream systems are restored, resubmit the messages back into the primary log:

```bash
# Execute replay for the tenant
pnpm tsx scripts/replay-dlq.ts --execute --tenant tenant_acme
```

## 5. Step 4: Verification and Clean-up

1. Monitor consumer group lag: `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group nexusgrid-workers-v1`.
2. Confirm the replayed messages are successfully acknowledged and processed without bouncing back into `nexusgrid.events.dlq`.
3. Close the incident report.

