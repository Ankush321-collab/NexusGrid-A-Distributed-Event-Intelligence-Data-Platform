import crypto from 'crypto';
import type { InMemoryRedisClient, StructuredQueryParams, QueryExecutionResult } from '@nexusgrid/database';

export interface CachedQueryResult {
  data: any[];
  meta: any;
  cachedAt: number;
  expiresAt: number;
  computeDurationMs: number;
}

export class QueryCacheManager {
  private inFlightQueries: Map<string, Promise<QueryExecutionResult>> = new Map();
  private cacheStore: Map<string, CachedQueryResult> = new Map();

  constructor(
    private redis: InMemoryRedisClient,
    private defaultTtlMs: number = 60000 // 60 seconds
  ) {}

  public getCacheKey(params: StructuredQueryParams): string {
    const serialized = JSON.stringify({
      t: params.tenant_id,
      p: params.project_id,
      s: params.start_time,
      e: params.end_time,
      ev: params.event_names,
      f: params.filters,
      g: params.group_by,
      m: params.metrics,
      l: params.limit,
      fin: params.final,
    });
    const hash = crypto.createHash('sha256').update(serialized).digest('hex').slice(0, 16);
    return `ng:qcache:${params.tenant_id}:${params.project_id}:${hash}`;
  }

  /**
   * Retrieves from cache, or executes single-flight query and caches outcome
   */
  public async executeWithCache(
    params: StructuredQueryParams,
    queryFn: () => Promise<QueryExecutionResult>,
    ttlMs: number = this.defaultTtlMs
  ): Promise<QueryExecutionResult> {
    const key = this.getCacheKey(params);
    const now = Date.now();

    // 1. Check local / redis cache
    const cached = this.cacheStore.get(key);
    if (cached && cached.expiresAt > now) {
      return {
        data: cached.data,
        meta: {
          ...cached.meta,
          fromCache: true,
          cachedAt: cached.cachedAt,
        },
      };
    }

    // 2. Single-Flight Promise Coalescing: Prevent Thundering Herd / Cache Stampede
    let inFlight = this.inFlightQueries.get(key);
    if (inFlight) {
      const result = await inFlight;
      return {
        ...result,
        meta: {
          ...result.meta,
          fromCache: true,
          coalesced: true,
        },
      };
    }

    // 3. Execute query exclusively and store promise for concurrent callers
    const queryPromise = (async () => {
      const startExec = Date.now();
      const freshResult = await queryFn();
      const duration = Date.now() - startExec;

      const cacheEntry: CachedQueryResult = {
        data: freshResult.data,
        meta: freshResult.meta,
        cachedAt: Date.now(),
        expiresAt: Date.now() + ttlMs,
        computeDurationMs: duration,
      };

      this.cacheStore.set(key, cacheEntry);
      return freshResult;
    })();

    this.inFlightQueries.set(key, queryPromise);

    try {
      return await queryPromise;
    } finally {
      this.inFlightQueries.delete(key);
    }
  }

  public invalidateTenant(tenantId: string): void {
    for (const k of this.cacheStore.keys()) {
      if (k.startsWith(`ng:qcache:${tenantId}:`)) {
        this.cacheStore.delete(k);
      }
    }
  }

  public clear(): void {
    this.cacheStore.clear();
    this.inFlightQueries.clear();
  }
}

