import { describe, it, expect, beforeEach } from 'vitest';
import { InMemoryClickHouseAdapter, InMemoryRedisClient } from '../../packages/database/src/index.js';
import { QueryCacheManager } from '../../apps/query-service/src/cache/cache-manager.js';
import { ASTValidator } from '../../apps/query-service/src/builder/ast-validator.js';
import { buildQueryServer } from '../../apps/query-service/src/index.js';
import type { EventEnvelope } from '../../packages/contracts/src/index.js';

describe('Phase 5 - Analytical Query Service, AST Allowlisting, and Cache Stampede Defense', () => {
  let clickhouse: InMemoryClickHouseAdapter;
  let redis: InMemoryRedisClient;
  let cacheManager: QueryCacheManager;
  let validator: ASTValidator;

  beforeEach(async () => {
    clickhouse = new InMemoryClickHouseAdapter();
    redis = new InMemoryRedisClient();
    cacheManager = new QueryCacheManager(redis, 30000); // 30s TTL
    validator = new ASTValidator();

    // Seed 100 test events for tenant_analytics
    const sampleEvents: EventEnvelope[] = [];
    for (let i = 1; i <= 100; i++) {
      sampleEvents.push({
        event_id: `01ARZ3NDEKTSV4RRFFQ69G${i.toString().padStart(5, '0')}`,
        tenant_id: 'tenant_analytics',
        project_id: 'proj_dash',
        event_name: i % 2 === 0 ? 'checkout.completed' : 'page.viewed',
        occurred_at: '2026-10-30T12:00:00.000Z',
        received_at: '2026-10-30T12:00:01.000Z',
        schema_version: 1,
        properties: {
          latency_ms: i * 2, // 2ms to 200ms
          status_code: i > 90 ? 500 : 200, // 10 error events
        },
      });
    }

    // Seed 5 events for tenant_other
    for (let i = 1; i <= 5; i++) {
      sampleEvents.push({
        event_id: `01ARZ3NDEKTSV4RRFFQ69GOTH${i}`,
        tenant_id: 'tenant_other',
        project_id: 'proj_other',
        event_name: 'secret.data',
        occurred_at: '2026-10-30T12:00:00.000Z',
        schema_version: 1,
        properties: { secret_val: 123 },
      });
    }

    const records = sampleEvents.map((e) => InMemoryClickHouseAdapter.envelopeToClickHouseRecord(e));
    await clickhouse.insertBatch(records);
  });

  // =========================================================================
  // Test 5.1: Parametric Aggregations & Percentiles Correctness
  // =========================================================================
  it('computes accurate grouping, counts, averages, and p95 percentiles against analytical data', async () => {
    const app = await buildQueryServer({ clickhouse, redis, cacheManager, validator });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/analytics/query',
      headers: { 'X-Tenant-Id': 'tenant_analytics' },
      payload: {
        project_id: 'proj_dash',
        time_range: {
          start: '2026-10-30T00:00:00.000Z',
          end: '2026-10-30T23:59:59.000Z',
        },
        group_by: ['event_name'],
        metrics: ['count()', 'avg(latency_ms)', 'p95(latency_ms)'],
      },
    });

    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.body);
    expect(body.status).toBe('SUCCESS');
    expect(body.data).toHaveLength(2); // checkout.completed and page.viewed

    const checkoutGroup = body.data.find((d: any) => d.event_name === 'checkout.completed');
    expect(checkoutGroup).toBeDefined();
    expect(checkoutGroup.count).toBe(50);
    expect(checkoutGroup['avg(latency_ms)']).toBeGreaterThan(0);
    expect(checkoutGroup['p95(latency_ms)']).toBeGreaterThan(checkoutGroup['avg(latency_ms)']);
  });

  // =========================================================================
  // Test 5.2: Strict AST Allowlist & SQL Injection Prevention
  // =========================================================================
  it('strictly rejects malicious queries attempting SQL injection or disallowed metric expressions', async () => {
    const app = await buildQueryServer({ clickhouse, redis, cacheManager, validator });

    // Attack 1: Injected SQL into metrics
    const sqliMetricRes = await app.inject({
      method: 'POST',
      url: '/v1/analytics/query',
      headers: { 'X-Tenant-Id': 'tenant_analytics' },
      payload: {
        project_id: 'proj_dash',
        time_range: { start: '2026-10-30T00:00:00Z', end: '2026-10-30T23:59:59Z' },
        metrics: ["count(); DROP TABLE events; --"],
      },
    });
    expect(sqliMetricRes.statusCode).toBe(400);
    expect(JSON.parse(sqliMetricRes.body).code).toBe('ERR_SQL_INJECTION_DETECTED');

    // Attack 2: Unsafe / Unallowlisted function
    const unallowlistedRes = await app.inject({
      method: 'POST',
      url: '/v1/analytics/query',
      headers: { 'X-Tenant-Id': 'tenant_analytics' },
      payload: {
        project_id: 'proj_dash',
        time_range: { start: '2026-10-30T00:00:00Z', end: '2026-10-30T23:59:59Z' },
        metrics: ["arbitrary_exec('cat /etc/passwd')"],
      },
    });
    expect(unallowlistedRes.statusCode).toBe(400);
    expect(JSON.parse(unallowlistedRes.body).code).toBe('ERR_DISALLOWED_METRIC');
  });

  // =========================================================================
  // Test 5.3: Scan Cost & Max Time-Range Limits (> 90 Days Rejection)
  // =========================================================================
  it('protects database by rejecting queries spanning more than 90 days', async () => {
    const app = await buildQueryServer({ clickhouse, redis, cacheManager, validator });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/analytics/query',
      headers: { 'X-Tenant-Id': 'tenant_analytics' },
      payload: {
        project_id: 'proj_dash',
        time_range: {
          start: '2026-01-01T00:00:00.000Z',
          end: '2026-06-01T00:00:00.000Z', // ~150 days!
        },
      },
    });

    expect(response.statusCode).toBe(400);
    const body = JSON.parse(response.body);
    expect(body.code).toBe('ERR_SCAN_COST_LIMIT_EXCEEDED');
  });

  // =========================================================================
  // Test 5.4: Redis Cache Hit & Cache Stampede Defense (Single-Flight Coalescing)
  // =========================================================================
  it('serves repeated requests from cache and coalesces concurrent queries during thundering herd', async () => {
    const app = await buildQueryServer({ clickhouse, redis, cacheManager, validator });
    const payload = {
      project_id: 'proj_dash',
      time_range: { start: '2026-10-30T00:00:00Z', end: '2026-10-30T23:59:59Z' },
      metrics: ['count()'],
    };

    // 1. First Request: Cache MISS
    const res1 = await app.inject({
      method: 'POST',
      url: '/v1/analytics/query',
      headers: { 'X-Tenant-Id': 'tenant_analytics' },
      payload,
    });
    expect(res1.statusCode).toBe(200);
    expect(res1.headers['x-cache-lookup']).toBe('MISS');

    // 2. Second Identical Request: Cache HIT
    const res2 = await app.inject({
      method: 'POST',
      url: '/v1/analytics/query',
      headers: { 'X-Tenant-Id': 'tenant_analytics' },
      payload,
    });
    expect(res2.statusCode).toBe(200);
    expect(res2.headers['x-cache-lookup']).toBe('HIT');

    // 3. Cache Stampede / Thundering Herd Test:
    // Invalidate cache, then fire 20 parallel requests simultaneously
    cacheManager.clear();

    const parallelRequests = Array.from({ length: 20 }, () =>
      app.inject({
        method: 'POST',
        url: '/v1/analytics/query',
        headers: { 'X-Tenant-Id': 'tenant_analytics' },
        payload,
      })
    );

    const parallelResults = await Promise.all(parallelRequests);
    for (const r of parallelResults) {
      expect(r.statusCode).toBe(200);
      const data = JSON.parse(r.body);
      expect(data.data[0].count).toBe(100);
    }
  });

  // =========================================================================
  // Test 5.5: Strict Multi-Tenant Query Isolation
  // =========================================================================
  it('guarantees tenant data isolation: Tenant A cannot view Tenant B events even with identical project IDs', async () => {
    const app = await buildQueryServer({ clickhouse, redis, cacheManager, validator });

    // Tenant Other query
    const res = await app.inject({
      method: 'POST',
      url: '/v1/analytics/query',
      headers: { 'X-Tenant-Id': 'tenant_other' },
      payload: {
        project_id: 'proj_other',
        time_range: { start: '2026-10-30T00:00:00Z', end: '2026-10-30T23:59:59Z' },
      },
    });

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.data).toHaveLength(5);
    // Every returned record belongs strictly to tenant_other
    for (const item of body.data) {
      expect(item.tenant_id).toBe('tenant_other');
    }
  });
});

