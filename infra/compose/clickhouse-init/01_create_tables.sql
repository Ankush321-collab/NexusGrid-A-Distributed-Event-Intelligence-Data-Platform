-- NexusGrid Phase 4: ClickHouse Columnar Storage Engine DDL
-- Database & Events Table using ReplacingMergeTree with Monthly Partitioning and 90-Day TTL

CREATE DATABASE IF NOT EXISTS nexusgrid;

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

-- Pre-aggregated Materialized View for 1-Minute Service Metrics (Latency Percentiles & Error Rates)
CREATE TABLE IF NOT EXISTS nexusgrid.events_1m_aggregates (
    tenant_id LowCardinality(String),
    project_id LowCardinality(String),
    event_name LowCardinality(String),
    window_start DateTime,
    total_count UInt64,
    error_count UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (tenant_id, project_id, event_name, window_start);

CREATE MATERIALIZED VIEW IF NOT EXISTS nexusgrid.events_1m_mv
TO nexusgrid.events_1m_aggregates AS
SELECT
    tenant_id,
    project_id,
    event_name,
    toStartOfMinute(occurred_at) AS window_start,
    count() AS total_count,
    countIf(properties_numeric['status_code'] >= 500) AS error_count
FROM nexusgrid.events
GROUP BY tenant_id, project_id, event_name, window_start;

