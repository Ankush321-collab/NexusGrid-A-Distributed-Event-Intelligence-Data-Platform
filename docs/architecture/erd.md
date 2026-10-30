# NexusGrid Phase 1 Entity Relationship Diagram & Metadata Architecture

## 1. Relational ER Diagram

```mermaid
erDiagram
    TENANTS ||--o{ USERS : owns
    TENANTS ||--o{ PROJECTS : provisions
    TENANTS ||--o{ API_KEYS : issues
    TENANTS ||--o{ AUDIT_LOGS : records
    PROJECTS ||--o{ API_KEYS : scopes

    TENANTS {
        string id PK "Unique slug (e.g. tenant_acme)"
        string name "Tenant display name"
        string status "ACTIVE | SUSPENDED | DELETED"
        timestamptz created_at
        timestamptz updated_at
    }

    USERS {
        string id PK "Unique user identifier"
        string tenant_id FK "References tenants.id"
        string email "User email"
        string role "OWNER | ADMIN | MEMBER | VIEWER"
        timestamptz created_at
    }

    PROJECTS {
        string id PK "Unique project identifier"
        string tenant_id FK "References tenants.id"
        string name "Project name"
        string environment "dev | staging | prod"
        timestamptz created_at
    }

    API_KEYS {
        string id PK "Key identifier"
        string tenant_id FK "References tenants.id"
        string project_id FK "References projects.id"
        string name "Human-readable key description"
        varchar key_prefix "First 8 hex chars of secret"
        string key_hash "Salted SHA-256 / Argon2id hash"
        text_array scopes "Array of scopes (e.g. events:write)"
        timestamptz expires_at "Nullable expiration timestamp"
        timestamptz revoked_at "Nullable revocation timestamp"
        timestamptz created_at
    }

    AUDIT_LOGS {
        bigserial id PK "Sequential ID for cursor pagination"
        string tenant_id FK "References tenants.id"
        string actor_id "User or key responsible for change"
        string action "Event action name"
        string resource_type "Entity modified"
        string resource_id "ID of modified entity"
        jsonb metadata "Arbitrary structured audit attributes"
        timestamptz timestamp
    }

    OUTBOX {
        bigserial id PK "Sequential ID"
        string aggregate_type "Entity type (e.g. api_key)"
        string aggregate_id "Entity ID"
        string event_type "Kafka message event type"
        jsonb payload "Event payload"
        jsonb headers "Transport headers"
        timestamptz created_at
        timestamptz processed_at "Nullable"
    }
```

## 2. Composite Index Rationale & Access Patterns

1. **`idx_api_keys_lookup (key_prefix, tenant_id)`**:
   - In high-throughput ingestion, every API request presents an API key (`ng_live_<prefix8>_<secret32>`).
   - The gateway parses `prefix8` in O(1) time and performs an index lookup on `(key_prefix, tenant_id)`.
   - Result: Index Scan directly finds candidate hash without table scan.
2. **`idx_audit_logs_tenant_cursor (tenant_id, id DESC)`**:
   - Audit logs are read using cursor-based pagination: `SELECT * FROM audit_logs WHERE tenant_id = $1 AND id < $cursor ORDER BY id DESC LIMIT $limit`.
   - A single composite B-Tree traversal serves both the tenant equality filter and descending sort without intermediate sort buffers.
3. **`idx_outbox_unprocessed (id) WHERE processed_at IS NULL`**:
   - Outbox worker queries only unprocessed rows using `SELECT ... FOR UPDATE SKIP LOCKED`.
   - Partial index ensures only pending records are indexed, minimizing index bloat.

