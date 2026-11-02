import type { EventEnvelope } from '@nexusgrid/contracts';

export interface ClickHouseEventRecord {
  tenant_id: string;
  project_id: string;
  event_name: string;
  occurred_at: string;
  received_at: string;
  event_id: string;
  schema_version: number;
  properties_string: Record<string, string>;
  properties_numeric: Record<string, number>;
  properties_boolean: Record<string, number>;
  raw_payload: string;
}

export interface QueryFilter {
  field: string;
  op: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'in';
  value: any;
}

export interface StructuredQueryParams {
  tenant_id: string;
  project_id: string;
  start_time: string;
  end_time: string;
  event_names?: string[];
  filters?: QueryFilter[];
  group_by?: string[];
  metrics?: string[]; // e.g. ["count()", "avg(latency_ms)", "p95(latency_ms)"]
  limit?: number;
  final?: boolean;
}

export interface QueryExecutionResult {
  data: any[];
  meta: {
    rowsScanned: number;
    partitionsScanned: string[];
    elapsedMs: number;
    fromCache?: boolean;
  };
}

export interface ClickHouseClientInterface {
  insertBatch(records: ClickHouseEventRecord[]): Promise<number>;
  query(params: StructuredQueryParams): Promise<QueryExecutionResult>;
  optimizeTableFinal(): Promise<number>;
  applyRetentionTtl(retentionDays?: number): Promise<number>;
  count(tenantId: string, projectId?: string): Promise<number>;
  close(): Promise<void>;
}

export class InMemoryClickHouseAdapter implements ClickHouseClientInterface {
  // Key: Partition string "YYYYMM" -> Array of records
  private partitions: Map<string, ClickHouseEventRecord[]> = new Map();
  public insertCallsCount: number = 0;
  public totalInsertedRows: number = 0;

  private getPartitionKey(occurredAtIso: string): string {
    const d = new Date(occurredAtIso);
    const yyyy = d.getUTCFullYear();
    const mm = (d.getUTCMonth() + 1).toString().padStart(2, '0');
    return `${yyyy}${mm}`;
  }

  public async insertBatch(records: ClickHouseEventRecord[]): Promise<number> {
    if (records.length === 0) return 0;
    this.insertCallsCount++;

    for (const record of records) {
      const partKey = this.getPartitionKey(record.occurred_at);
      if (!this.partitions.has(partKey)) {
        this.partitions.set(partKey, []);
      }
      this.partitions.get(partKey)!.push(record);
      this.totalInsertedRows++;
    }

    return records.length;
  }

  /**
   * Helper to map a canonical EventEnvelope into a typed ClickHouseEventRecord
   */
  public static envelopeToClickHouseRecord(event: EventEnvelope): ClickHouseEventRecord {
    const properties_string: Record<string, string> = {};
    const properties_numeric: Record<string, number> = {};
    const properties_boolean: Record<string, number> = {};

    if (event.properties && typeof event.properties === 'object') {
      for (const [k, v] of Object.entries(event.properties)) {
        if (typeof v === 'number') {
          properties_numeric[k] = v;
        } else if (typeof v === 'boolean') {
          properties_boolean[k] = v ? 1 : 0;
        } else if (v !== null && v !== undefined) {
          properties_string[k] = String(v);
        }
      }
    }

    return {
      tenant_id: event.tenant_id,
      project_id: event.project_id,
      event_name: event.event_name,
      occurred_at: event.occurred_at,
      received_at: event.received_at || new Date().toISOString(),
      event_id: event.event_id,
      schema_version: event.schema_version,
      properties_string,
      properties_numeric,
      properties_boolean,
      raw_payload: JSON.stringify(event),
    };
  }

  /**
   * Simulates ReplacingMergeTree deduplication (OPTIMIZE TABLE events FINAL)
   * Collapses duplicate event IDs within (tenant_id, project_id, event_name, occurred_at, event_id),
   * retaining only the latest received_at record.
   */
  public async optimizeTableFinal(): Promise<number> {
    let deduplicatedCount = 0;

    for (const [partKey, rows] of this.partitions.entries()) {
      const map = new Map<string, ClickHouseEventRecord>();
      for (const row of rows) {
        const dedupeKey = `${row.tenant_id}:${row.project_id}:${row.event_name}:${row.occurred_at}:${row.event_id}`;
        const existing = map.get(dedupeKey);
        if (!existing || new Date(row.received_at) >= new Date(existing.received_at)) {
          map.set(dedupeKey, row);
        }
      }
      const newRows = Array.from(map.values());
      deduplicatedCount += rows.length - newRows.length;
      this.partitions.set(partKey, newRows);
    }

    return deduplicatedCount;
  }

  /**
   * Simulates ClickHouse TTL toDateTime(occurred_at) + INTERVAL 90 DAY
   */
  public async applyRetentionTtl(retentionDays: number = 90): Promise<number> {
    const cutoff = Date.now() - retentionDays * 24 * 3600 * 1000;
    let pruned = 0;

    for (const [partKey, rows] of this.partitions.entries()) {
      const retained = rows.filter((r) => Date.parse(r.occurred_at) >= cutoff);
      pruned += rows.length - retained.length;
      this.partitions.set(partKey, retained);
    }

    return pruned;
  }

  /**
   * Executes structured analytical query with partition pruning, multi-tenant isolation,
   * filter evaluation, aggregations, and percentiles.
   */
  public async query(params: StructuredQueryParams): Promise<QueryExecutionResult> {
    const startTime = Date.now();
    const startMs = Date.parse(params.start_time);
    const endMs = Date.parse(params.end_time);

    // 1. Partition Pruning: Calculate which monthly partitions match the time range
    const matchingPartitions: string[] = [];
    const cur = new Date(startMs);
    const end = new Date(endMs);
    while (cur <= end || (cur.getUTCFullYear() === end.getUTCFullYear() && cur.getUTCMonth() === end.getUTCMonth())) {
      const yyyy = cur.getUTCFullYear();
      const mm = (cur.getUTCMonth() + 1).toString().padStart(2, '0');
      const key = `${yyyy}${mm}`;
      if (!matchingPartitions.includes(key)) {
        matchingPartitions.push(key);
      }
      cur.setUTCMonth(cur.getUTCMonth() + 1);
    }

    // 2. Scan only matching partitions
    let candidateRows: ClickHouseEventRecord[] = [];
    for (const partKey of matchingPartitions) {
      const partRows = this.partitions.get(partKey) || [];
      candidateRows.push(...partRows);
    }

    const rowsScanned = candidateRows.length;

    // 3. Multi-Tenant and Project Isolation Filter
    candidateRows = candidateRows.filter(
      (r) => r.tenant_id === params.tenant_id && r.project_id === params.project_id
    );

    // 4. Time Window Filter
    candidateRows = candidateRows.filter((r) => {
      const t = Date.parse(r.occurred_at);
      return t >= startMs && t <= endMs;
    });

    // 5. Event Names Filter
    if (params.event_names && params.event_names.length > 0) {
      const names = new Set(params.event_names);
      candidateRows = candidateRows.filter((r) => names.has(r.event_name));
    }

    // 6. Custom Filters
    if (params.filters && params.filters.length > 0) {
      for (const f of params.filters) {
        candidateRows = candidateRows.filter((r) => {
          let val: any;
          if (f.field.startsWith('properties_numeric.')) {
            const key = f.field.replace('properties_numeric.', '');
            val = r.properties_numeric[key];
          } else if (f.field.startsWith('properties_string.')) {
            const key = f.field.replace('properties_string.', '');
            val = r.properties_string[key];
          } else {
            val = (r as any)[f.field];
          }

          if (val === undefined || val === null) return false;

          switch (f.op) {
            case 'eq': return val === f.value;
            case 'neq': return val !== f.value;
            case 'gt': return val > f.value;
            case 'gte': return val >= f.value;
            case 'lt': return val < f.value;
            case 'lte': return val <= f.value;
            case 'in': return Array.isArray(f.value) && f.value.includes(val);
            default: return true;
          }
        });
      }
    }

    // 7. ReplacingMergeTree FINAL deduplication if requested
    if (params.final) {
      const dedupeMap = new Map<string, ClickHouseEventRecord>();
      for (const r of candidateRows) {
        const k = `${r.tenant_id}:${r.project_id}:${r.event_name}:${r.occurred_at}:${r.event_id}`;
        const existing = dedupeMap.get(k);
        if (!existing || new Date(r.received_at) >= new Date(existing.received_at)) {
          dedupeMap.set(k, r);
        }
      }
      candidateRows = Array.from(dedupeMap.values());
    }

    // 8. Aggregation / Group By Execution
    let resultData: any[];

    if (params.group_by && params.group_by.length > 0) {
      const groups = new Map<string, ClickHouseEventRecord[]>();
      for (const r of candidateRows) {
        const groupKey = params.group_by.map((g) => (r as any)[g] || '').join(':::');
        if (!groups.has(groupKey)) {
          groups.set(groupKey, []);
        }
        groups.get(groupKey)!.push(r);
      }

      resultData = [];
      for (const [key, gRows] of groups.entries()) {
        const item: Record<string, any> = {};
        const keyParts = key.split(':::');
        params.group_by.forEach((g, idx) => {
          item[g] = keyParts[idx];
        });

        // Compute metrics
        item['count'] = gRows.length;
        if (params.metrics) {
          for (const m of params.metrics) {
            if (m.startsWith('avg(') && m.endsWith(')')) {
              const field = m.slice(4, -1);
              const sum = gRows.reduce((acc, r) => acc + (r.properties_numeric[field] || 0), 0);
              item[m] = gRows.length > 0 ? sum / gRows.length : 0;
            } else if (m.startsWith('p95(') && m.endsWith(')')) {
              const field = m.slice(4, -1);
              const vals = gRows.map((r) => r.properties_numeric[field] || 0).sort((a, b) => a - b);
              const idx = Math.floor(vals.length * 0.95);
              item[m] = vals[idx] ?? 0;
            }
          }
        }
        resultData.push(item);
      }
    } else if (params.metrics && params.metrics.length > 0) {
      // Single global aggregate
      const item: Record<string, any> = { count: candidateRows.length };
      for (const m of params.metrics) {
        if (m.startsWith('avg(') && m.endsWith(')')) {
          const field = m.slice(4, -1);
          const sum = candidateRows.reduce((acc, r) => acc + (r.properties_numeric[field] || 0), 0);
          item[m] = candidateRows.length > 0 ? sum / candidateRows.length : 0;
        } else if (m.startsWith('p95(') && m.endsWith(')')) {
          const field = m.slice(4, -1);
          const vals = candidateRows.map((r) => r.properties_numeric[field] || 0).sort((a, b) => a - b);
          const idx = Math.floor(vals.length * 0.95);
          item[m] = vals[idx] ?? 0;
        }
      }
      resultData = [item];
    } else {
      resultData = candidateRows.slice(0, params.limit || 100);
    }

    if (params.limit && resultData.length > params.limit) {
      resultData = resultData.slice(0, params.limit);
    }

    return {
      data: resultData,
      meta: {
        rowsScanned,
        partitionsScanned: matchingPartitions,
        elapsedMs: Date.now() - startTime,
      },
    };
  }

  public async count(tenantId: string, projectId?: string): Promise<number> {
    let total = 0;
    for (const rows of this.partitions.values()) {
      for (const r of rows) {
        if (r.tenant_id === tenantId && (!projectId || r.project_id === projectId)) {
          total++;
        }
      }
    }
    return total;
  }

  public async close(): Promise<void> {
    this.partitions.clear();
  }
}

export const defaultClickHouse = new InMemoryClickHouseAdapter();
