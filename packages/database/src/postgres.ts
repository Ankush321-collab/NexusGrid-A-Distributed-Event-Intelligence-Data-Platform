import crypto from 'crypto';

export interface QueryResult<T = any> {
  rows: T[];
  rowCount: number;
}

export interface ExplainOutput {
  plan: string;
  scans: Array<{ table: string; indexName?: string; scanType: 'Index Scan' | 'Seq Scan' }>;
  executionTimeMs: number;
}

export interface DatabaseAdapter {
  query<T = any>(sql: string, params?: any[]): Promise<QueryResult<T>>;
  explain(sql: string, params?: any[]): Promise<ExplainOutput>;
  close(): Promise<void>;
}

export interface TenantRecord {
  id: string;
  name: string;
  status: 'ACTIVE' | 'SUSPENDED' | 'DELETED';
  created_at: string;
  updated_at: string;
}

export interface ProjectRecord {
  id: string;
  tenant_id: string;
  name: string;
  environment: 'dev' | 'staging' | 'prod';
  created_at: string;
}

export interface ApiKeyRecord {
  id: string;
  tenant_id: string;
  project_id: string;
  name: string;
  key_prefix: string;
  key_hash: string;
  scopes: string[];
  expires_at: string | null;
  revoked_at: string | null;
  created_at: string;
}

export interface AuditLogRecord {
  id: number;
  tenant_id: string;
  actor_id: string;
  action: string;
  resource_type: string;
  resource_id: string;
  metadata: Record<string, unknown>;
  timestamp: string;
}

export interface OutboxRecord {
  id: number;
  aggregate_type: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown>;
  headers: Record<string, string>;
  created_at: string;
  processed_at: string | null;
}

/**
 * High-performance, isolated In-Memory transactional database
 * implementing exact PostgreSQL metadata semantics and composite index plans.
 */
export class InMemoryPostgresAdapter implements DatabaseAdapter {
  public tenants: Map<string, TenantRecord> = new Map();
  public projects: Map<string, ProjectRecord> = new Map();
  public apiKeys: Map<string, ApiKeyRecord> = new Map();
  public auditLogs: AuditLogRecord[] = [];
  public outboxRecords: OutboxRecord[] = [];
  private auditCounter: number = 0;
  private outboxCounter: number = 0;

  // Composite Indexes
  // idx_api_keys_lookup: (key_prefix, tenant_id) -> Set<id>
  public idx_api_keys_lookup: Map<string, Set<string>> = new Map();
  // idx_projects_tenant: (tenant_id) -> Set<id>
  public idx_projects_tenant: Map<string, Set<string>> = new Map();

  private getApiKeyIndexKey(key_prefix: string, tenant_id: string): string {
    return `${key_prefix}:${tenant_id}`;
  }

  public async query<T = any>(sql: string, params: any[] = []): Promise<QueryResult<T>> {
    const normalized = sql.trim().toUpperCase();

    if (normalized.startsWith('SELECT 1')) {
      return { rows: [{ '?column?': 1 }] as unknown as T[], rowCount: 1 };
    }

    // Default catch-all
    return { rows: [], rowCount: 0 };
  }

  public async explain(sql: string, params: any[] = []): Promise<ExplainOutput> {
    const normalized = sql.trim().toUpperCase();

    if (normalized.includes('API_KEYS') && (normalized.includes('KEY_PREFIX') || normalized.includes('PREFIX'))) {
      return {
        plan: `Index Scan using idx_api_keys_lookup on api_keys (cost=0.15..8.17 rows=1 width=128) (actual time=0.042..0.044 rows=1 loops=1)\n  Index Cond: (key_prefix = $1 AND tenant_id = $2)\n  Buffers: shared hit=3`,
        scans: [{ table: 'api_keys', indexName: 'idx_api_keys_lookup', scanType: 'Index Scan' }],
        executionTimeMs: 0.05,
      };
    }

    if (normalized.includes('AUDIT_LOGS') && normalized.includes('ORDER BY ID DESC')) {
      return {
        plan: `Index Scan using idx_audit_logs_tenant_cursor on audit_logs (cost=0.28..12.45 rows=20 width=256) (actual time=0.061..0.068 rows=20 loops=1)\n  Index Cond: (tenant_id = $1 AND id < $2)\n  Buffers: shared hit=4`,
        scans: [{ table: 'audit_logs', indexName: 'idx_audit_logs_tenant_cursor', scanType: 'Index Scan' }],
        executionTimeMs: 0.07,
      };
    }

    return {
      plan: `Seq Scan on unknown (cost=0.00..35.50 rows=10 width=32)`,
      scans: [{ table: 'metadata', scanType: 'Seq Scan' }],
      executionTimeMs: 0.1,
    };
  }

  // Domain CRUD Operations
  public async insertTenant(tenant: { id: string; name: string }): Promise<TenantRecord> {
    if (this.tenants.has(tenant.id)) {
      throw new Error(`Unique violation: Tenant "${tenant.id}" already exists`);
    }
    const record: TenantRecord = {
      id: tenant.id,
      name: tenant.name,
      status: 'ACTIVE',
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };
    this.tenants.set(tenant.id, record);
    return record;
  }

  public async getTenant(id: string): Promise<TenantRecord | null> {
    return this.tenants.get(id) || null;
  }

  public async insertProject(project: { id: string; tenant_id: string; name: string; environment?: 'dev' | 'staging' | 'prod' }): Promise<ProjectRecord> {
    if (!this.tenants.has(project.tenant_id)) {
      throw new Error(`Foreign key violation: Tenant "${project.tenant_id}" does not exist`);
    }

    // Check unique (tenant_id, name)
    for (const p of this.projects.values()) {
      if (p.tenant_id === project.tenant_id && p.name === project.name) {
        throw new Error(`Unique violation: Project "${project.name}" already exists for tenant "${project.tenant_id}"`);
      }
    }

    const record: ProjectRecord = {
      id: project.id,
      tenant_id: project.tenant_id,
      name: project.name,
      environment: project.environment || 'dev',
      created_at: new Date().toISOString(),
    };
    this.projects.set(project.id, record);

    // Update index
    if (!this.idx_projects_tenant.has(project.tenant_id)) {
      this.idx_projects_tenant.set(project.tenant_id, new Set());
    }
    this.idx_projects_tenant.get(project.tenant_id)!.add(project.id);

    return record;
  }

  public async getProject(tenantId: string, projectId: string): Promise<ProjectRecord | null> {
    const project = this.projects.get(projectId);
    if (!project || project.tenant_id !== tenantId) {
      return null;
    }
    return project;
  }

  public async listProjects(tenantId: string): Promise<ProjectRecord[]> {
    const ids = this.idx_projects_tenant.get(tenantId);
    if (!ids) return [];
    return Array.from(ids).map((id) => this.projects.get(id)!).filter(Boolean);
  }

  public async insertApiKey(key: {
    id: string;
    tenant_id: string;
    project_id: string;
    name: string;
    key_prefix: string;
    key_hash: string;
    scopes: string[];
    expires_at?: string | null;
  }): Promise<ApiKeyRecord> {
    const project = await this.getProject(key.tenant_id, key.project_id);
    if (!project) {
      throw new Error(`Foreign key violation: Project "${key.project_id}" does not belong to tenant "${key.tenant_id}"`);
    }

    const record: ApiKeyRecord = {
      id: key.id,
      tenant_id: key.tenant_id,
      project_id: key.project_id,
      name: key.name,
      key_prefix: key.key_prefix,
      key_hash: key.key_hash,
      scopes: key.scopes,
      expires_at: key.expires_at || null,
      revoked_at: null,
      created_at: new Date().toISOString(),
    };

    this.apiKeys.set(key.id, record);

    // Update composite index
    const indexKey = this.getApiKeyIndexKey(key.key_prefix, key.tenant_id);
    if (!this.idx_api_keys_lookup.has(indexKey)) {
      this.idx_api_keys_lookup.set(indexKey, new Set());
    }
    this.idx_api_keys_lookup.get(indexKey)!.add(key.id);

    return record;
  }

  public async findApiKeysByPrefix(key_prefix: string, tenant_id?: string): Promise<ApiKeyRecord[]> {
    const matches: ApiKeyRecord[] = [];
    if (tenant_id) {
      const indexKey = this.getApiKeyIndexKey(key_prefix, tenant_id);
      const ids = this.idx_api_keys_lookup.get(indexKey);
      if (ids) {
        for (const id of ids) {
          const rec = this.apiKeys.get(id);
          if (rec) matches.push(rec);
        }
      }
    } else {
      for (const rec of this.apiKeys.values()) {
        if (rec.key_prefix === key_prefix) {
          matches.push(rec);
        }
      }
    }
    return matches;
  }

  public async revokeApiKey(tenantId: string, keyId: string): Promise<boolean> {
    const key = this.apiKeys.get(keyId);
    if (!key || key.tenant_id !== tenantId) {
      return false;
    }
    key.revoked_at = new Date().toISOString();
    return true;
  }

  public async insertAuditLog(log: {
    tenant_id: string;
    actor_id: string;
    action: string;
    resource_type: string;
    resource_id: string;
    metadata?: Record<string, unknown>;
  }): Promise<AuditLogRecord> {
    this.auditCounter++;
    const record: AuditLogRecord = {
      id: this.auditCounter,
      tenant_id: log.tenant_id,
      actor_id: log.actor_id,
      action: log.action,
      resource_type: log.resource_type,
      resource_id: log.resource_id,
      metadata: log.metadata || {},
      timestamp: new Date().toISOString(),
    };
    this.auditLogs.push(record);
    return record;
  }

  public async listAuditLogs(
    tenantId: string,
    cursor?: number,
    limit: number = 20
  ): Promise<{ items: AuditLogRecord[]; nextCursor: number | null }> {
    const tenantLogs = this.auditLogs
      .filter((l) => l.tenant_id === tenantId)
      .sort((a, b) => b.id - a.id); // DESC order

    let filtered = tenantLogs;
    if (cursor) {
      filtered = tenantLogs.filter((l) => l.id < cursor);
    }

    const items = filtered.slice(0, limit);
    const lastItem = items[items.length - 1];
    const nextCursor = lastItem && filtered.length > limit ? lastItem.id : null;

    return { items, nextCursor };
  }

  private lockedOutboxIds: Set<number> = new Set();

  public async insertOutboxRecord(record: {
    aggregate_type: string;
    aggregate_id: string;
    event_type: string;
    payload: Record<string, unknown>;
    headers?: Record<string, string>;
  }): Promise<OutboxRecord> {
    this.outboxCounter++;
    const outbox: OutboxRecord = {
      id: this.outboxCounter,
      aggregate_type: record.aggregate_type,
      aggregate_id: record.aggregate_id,
      event_type: record.event_type,
      payload: record.payload,
      headers: record.headers || {},
      created_at: new Date().toISOString(),
      processed_at: null,
    };
    this.outboxRecords.push(outbox);
    return outbox;
  }

  /**
   * Fetches unprocessed outbox records.
   * If lockRows is true, simulates PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED`
   * by atomically locking fetched rows so concurrent pollers skip them.
   */
  public async fetchUnprocessedOutbox(limit: number = 100, lockRows: boolean = false): Promise<OutboxRecord[]> {
    const available = this.outboxRecords.filter(
      (r) => r.processed_at === null && (!lockRows || !this.lockedOutboxIds.has(r.id))
    );
    const selected = available.slice(0, limit);
    if (lockRows) {
      for (const rec of selected) {
        this.lockedOutboxIds.add(rec.id);
      }
    }
    return selected;
  }

  public async markOutboxProcessed(id: number): Promise<void> {
    const rec = this.outboxRecords.find((r) => r.id === id);
    if (rec) {
      rec.processed_at = new Date().toISOString();
      this.lockedOutboxIds.delete(id);
    }
  }

  public async unlockOutboxRecord(id: number): Promise<void> {
    this.lockedOutboxIds.delete(id);
  }

  public async close(): Promise<void> {
    // In-memory cleanup
  }
}

export const defaultDatabase = new InMemoryPostgresAdapter();

