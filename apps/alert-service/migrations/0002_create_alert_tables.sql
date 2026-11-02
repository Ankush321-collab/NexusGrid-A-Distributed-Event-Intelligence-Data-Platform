-- NexusGrid Phase 6: Alert Rules, Incidents & Notification Tables DDL

CREATE TABLE IF NOT EXISTS alert_rules (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    project_id VARCHAR(64) NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    metric_name VARCHAR(128) NOT NULL, -- e.g. error_rate, p95_latency_ms, event_count
    operator VARCHAR(8) NOT NULL, -- gt, gte, lt, lte, eq
    threshold FLOAT8 NOT NULL,
    window_minutes INT NOT NULL DEFAULT 5,
    consecutive_breaches_required INT NOT NULL DEFAULT 2, -- Hysteresis window
    cooldown_minutes INT NOT NULL DEFAULT 15,
    webhook_url TEXT NOT NULL,
    webhook_secret VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'OK', -- OK, PENDING, FIRING, MUTED
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alert_incidents (
    id BIGSERIAL PRIMARY KEY,
    rule_id VARCHAR(64) NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    state VARCHAR(32) NOT NULL, -- FIRING, RESOLVED
    current_value FLOAT8 NOT NULL,
    threshold FLOAT8 NOT NULL,
    dedupe_key VARCHAR(128) NOT NULL,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ NULL,
    notification_dispatched BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_alert_rules_tenant ON alert_rules (tenant_id, project_id);
CREATE INDEX IF NOT EXISTS idx_alert_incidents_rule ON alert_incidents (rule_id, state);
CREATE INDEX IF NOT EXISTS idx_alert_incidents_dedupe ON alert_incidents (dedupe_key);
