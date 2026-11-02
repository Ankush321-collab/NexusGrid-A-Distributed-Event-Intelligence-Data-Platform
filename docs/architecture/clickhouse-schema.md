# NexusGrid Phase 4: ClickHouse Columnar Storage Engine & Schema Architecture

## 1. Primary Events Table Design

```sql
CREATE TABLE IF NOT EXISTS nexusgrid.events (
    tenant_id LowCardinality(String),
    project_id LowCardinality(String),
    event_name LowCardinality(String),
    occurred_at DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    event_id String,
    schema_version UInt16,
    properties_string Map(String, String),
    properties_numeric Map(String, Float64),
    properties_boolean Map(String, UInt8),
    raw_payload String CODEC(ZSTD(3))
) ENGINE = ReplacingMergeTree(received_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (tenant_id, project_id, event_name, occurred_at, event_id)
TTL toDateTime(occurred_at) + INTERVAL 90 DAY;
```

### Architectural Decisions & Rationale:

1. **`LowCardinality(String)` on `tenant_id`, `project_id`, `event_name`**:
   - Dictionary encoding compresses repetitive strings into 1-2 byte integer keys.
   - Drastically reduces disk storage and RAM footprint during high-cardinality group-by queries.

2. **Monthly Partitioning (`PARTITION BY toYYYYMM(occurred_at)`)**:
   - Time-series queries typically target recent hours, days, or weeks.
   - Monthly partition chunks keep part counts optimal ($<300$ parts per table) while avoiding the ClickHouse `TOO_MANY_PARTS` error.

3. **Composite Primary Sorting Key (`ORDER BY (tenant_id, project_id, event_name, occurred_at, event_id)`)**:
   - Multi-tenant physical data layout: tenant data is physically contiguous on disk.
   - Primary index allows binary search down to 8192-row granular index marks without full-table scans.

4. **ReplacingMergeTree Engine (`ReplacingMergeTree(received_at)`)**:
   - Guarantees downstream idempotency: duplicate event deliveries with the identical primary key collapse during background merges or on explicit query `FINAL`.
   - The row with the largest `received_at` timestamp is preserved.

5. **Retention Policy (`TTL toDateTime(occurred_at) + INTERVAL 90 DAY`)**:
   - Automated hot-tier partition drop after 90 days.
   - Historical cold data is durably preserved in S3 / MinIO Parquet archives.

