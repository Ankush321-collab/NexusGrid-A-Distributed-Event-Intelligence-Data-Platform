import { describe, it, expect, beforeEach } from 'vitest';
import {
  InMemoryClickHouseAdapter,
  InMemoryRedisClient,
  type ClickHouseEventRecord,
} from '../../packages/database/src/index.js';
import { QueryCacheManager } from '../../apps/query-service/src/cache/cache-manager.js';
import { ASTValidator } from '../../apps/query-service/src/builder/ast-validator.js';
import { buildQueryServer } from '../../apps/query-service/src/index.js';
import { ClickHouseBulkSink } from '../../workers/event-processor/src/sink/clickhouse-sink.js';
import type { EventEnvelope } from '../../packages/contracts/src/index.js';

describe('Hard-Level Test Type 4: OLAP Analytical Engine, ReplacingMergeTree Deduplication & Cache Stampede Defense', () => {
  let clickhouse: InMemoryClickHouseAdapter;
  let redis: InMemoryRedisClient;
  let cacheManager: QueryCacheManager;
  let validator: ASTValidator;

  beforeEach(() => {
    clickhouse = new InMemoryClickHouseAdapter();
    redis = new InMemoryRedisClient();
    cacheManager = new QueryCacheManager(redis, 60000);
    validator = new ASTValidator(90);
  });

  const createEvent = (
    id: string,
    version: number = 1,
    props: Record<string, any> = {},
    occurredAt: string = '2026-10-30T12:00:00.000Z',
    tenant: string = 'tenant_stress_olap',
    project: string = 'proj_analytics'
  ): EventEnvelope => ({
    event_id: id,
    tenant_id: tenant,
    project_id: project,
    event_name: 'payment.processed',
    occurred_at: occurredAt,
    received_at: new Date().toISOString(),
    schema_version: version,
    properties: {
      amount: 100 * version,
      status: 'SUCCESS',
      ...props,
    },
  });

  // =========================================================================
  // Test 4.1: ReplacingMergeTree Out-of-Order Version Deduplication Drill
  // =========================================================================
  it('deduplicates 500 out-of-order version updates across 100 unique ULIDs, strictly retaining highest schema_version', async () => {
    const sink = new ClickHouseBulkSink(clickhouse, {
      batchSize: 50,
      minFlushBatchSize: 10,
      enableAsyncInsert: true,
    });

    const totalUlids = 100;
    const versionsPerUlid = 5;
    const allEvents: EventEnvelope[] = [];

    // Create 5 versions for each of the 100 ULIDs
    for (let u = 1; u <= totalUlids; u++) {
      const ulid = `01ARZ3NDEKTSV4RRFFQ69G${u.toString().padStart(5, '0')}`;
      for (let v = 1; v <= versionsPerUlid; v++) {
        allEvents.push(createEvent(ulid, v, { updateIndex: v, finalPayload: v === 5 }));
      }
    }

    // Shuffle events deterministically to simulate concurrent out-of-order network arrival
    allEvents.sort((a, b) => {
      const hA = (parseInt(a.event_id.slice(-4), 10) * 31 + a.schema_version) % 100;
      const hB = (parseInt(b.event_id.slice(-4), 10) * 31 + b.schema_version) % 100;
      return hA - hB;
    });

    // Ingest all 500 events through ClickHouseBulkSink
    await sink.pushBatch(allEvents);
    await sink.flush(true);

    // Initial pre-compaction state: 500 raw rows inserted
    expect(clickhouse.totalInsertedRows).toBe(500);

    // Run Analytical Compaction (ReplacingMergeTree FINAL / optimizeTableFinal)
    const compactedRows = await clickhouse.optimizeTableFinal();

    // 400 duplicate rows purged; exactly 100 unique ULID records remain
    expect(compactedRows).toBe(400);
    expect(await clickhouse.count('tenant_stress_olap')).toBe(100);

    // Verify every single surviving record has schema_version === 5
    const queryRes = await clickhouse.query({
      tenant_id: 'tenant_stress_olap',
      project_id: 'proj_analytics',
      start_time: '2026-10-01T00:00:00.000Z',
      end_time: '2026-11-01T00:00:00.000Z',
      metrics: ['count()', 'avg(amount)'],
      final: true,
    });

    expect(queryRes.data).toHaveLength(1);
    expect(queryRes.data[0].count).toBe(100);
    // Each v=5 has amount = 500, so average must be exactly 500
    expect(queryRes.data[0]['avg(amount)']).toBe(500);
  });

  // =========================================================================
  // Test 4.2: Thundering Herd / Cache Stampede Singleflight Defense (200 concurrent requests)
  // =========================================================================
  it('coalesces 200 concurrent identical OLAP requests into a single database execution via singleflight', async () => {
    // Seed analytical database with 200 records
    const sampleRecords: ClickHouseEventRecord[] = [];
    for (let i = 1; i <= 200; i++) {
      const evt = createEvent(
        `01ARZ3NDEKTSV4RRFFQ69H${i.toString().padStart(5, '0')}`,
        1,
        { latency_ms: i * 5, status_code: 200 },
        '2026-10-30T10:00:00.000Z'
      );
      sampleRecords.push(InMemoryClickHouseAdapter.envelopeToClickHouseRecord(evt));
    }
    await clickhouse.insertBatch(sampleRecords);

    // Spy on clickhouse.query to track actual execution count
    let databaseQueryExecutions = 0;
    const originalQuery = clickhouse.query.bind(clickhouse);
    clickhouse.query = async (params) => {
      databaseQueryExecutions++;
      // Simulate non-trivial database analytical query latency
      await new Promise((r) => setTimeout(r, 25));
      return originalQuery(params);
    };

    const app = await buildQueryServer({ clickhouse, redis, cacheManager, validator });

    const concurrentCallers = 200;
    const queryPayload = {
      project_id: 'proj_analytics',
      time_range: {
        start: '2026-10-30T00:00:00.000Z',
        end: '2026-10-30T23:59:59.000Z',
      },
      metrics: ['count()', 'avg(latency_ms)', 'p95(latency_ms)'],
      group_by: ['event_name'],
    };

    // Dispatch 200 requests simultaneously in the same event-loop tick
    const requests = Array.from({ length: concurrentCallers }, () =>
      app.inject({
        method: 'POST',
        url: '/v1/analytics/query',
        headers: { 'X-Tenant-Id': 'tenant_stress_olap' },
        payload: queryPayload,
      })
    );

    const responses = await Promise.all(requests);

    // All 200 callers must receive HTTP 200 OK
    expect(responses).toHaveLength(200);
    for (const res of responses) {
      expect(res.statusCode).toBe(200);
      const json = res.json();
      expect(json.data).toHaveLength(1);
      expect(json.data[0].count).toBe(200);
      expect(json.data[0]['avg(latency_ms)']).toBeCloseTo(502.5, 1);
    }

    // Crucial Singleflight Assertion:
    // Despite 200 parallel incoming requests, ClickHouse was executed strictly ONCE!
    // 199 requests were coalesced into the in-flight singleflight promise!
    expect(databaseQueryExecutions).toBe(1);

    // Subsequent 201st request is served directly from Redis cache
    const cachedResponse = await app.inject({
      method: 'POST',
      url: '/v1/analytics/query',
      headers: { 'X-Tenant-Id': 'tenant_stress_olap' },
      payload: queryPayload,
    });
    expect(cachedResponse.statusCode).toBe(200);
    expect(cachedResponse.json().meta.fromCache).toBe(true);
    // Database query count must remain 1
    expect(databaseQueryExecutions).toBe(1);
  });

  // =========================================================================
  // Test 4.3: Adversarial SQL Injection, Tainted Input & Cross-Tenant Exfiltration Fuzzing
  // =========================================================================
  it('blocks diverse SQL injection attack vectors and guarantees strict tenant isolation boundaries', async () => {
    // Seed secret records for victim tenant
    const victimRecords: ClickHouseEventRecord[] = [
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(
        createEvent('01ARZ3NDEKTSV4RRFFQ69VIC01', 1, { balance: 999999 }, '2026-10-30T12:00:00.000Z', 'tenant_victim', 'proj_secret')
      ),
    ];
    await clickhouse.insertBatch(victimRecords);

    const app = await buildQueryServer({ clickhouse, redis, cacheManager, validator });

    // Attack Vectors
    const attackPayloads = [
      // 1. Tainted project_id SQL escape
      {
        project_id: "proj_analytics' OR '1'='1' --",
        time_range: { start: '2026-10-01T00:00:00.000Z', end: '2026-10-31T00:00:00.000Z' },
      },
      // 2. Drop table statement injection
      {
        project_id: 'proj_analytics',
        time_range: { start: '2026-10-01T00:00:00.000Z', end: '2026-10-31T00:00:00.000Z' },
        filters: [{ field: "amount'; DROP TABLE events_stream; --", op: 'eq', value: 100 }],
      },
      // 3. Union based cross-tenant exfiltration
      {
        project_id: 'proj_analytics',
        time_range: { start: '2026-10-01T00:00:00.000Z', end: '2026-10-31T00:00:00.000Z' },
        metrics: ["count() UNION SELECT raw_payload FROM events_stream WHERE tenant_id = 'tenant_victim' --"],
      },
      // 4. Disallowed ClickHouse reflection function
      {
        project_id: 'proj_analytics',
        time_range: { start: '2026-10-01T00:00:00.000Z', end: '2026-10-31T00:00:00.000Z' },
        metrics: ['toTypeName(schema_version)'],
      },
      // 5. Exceeded scan window (365 days requested, max 90 permitted)
      {
        project_id: 'proj_analytics',
        time_range: { start: '2025-01-01T00:00:00.000Z', end: '2026-01-01T00:00:00.000Z' },
        metrics: ['count()'],
      },
    ];

    for (const attack of attackPayloads) {
      const res = await app.inject({
        method: 'POST',
        url: '/v1/analytics/query',
        headers: { 'X-Tenant-Id': 'tenant_attacker' },
        payload: attack,
      });

      // Must be rejected with 400 Bad Request
      expect(res.statusCode).toBe(400);
      const errJson = res.json();
      expect(errJson.status).toBe(400);
      expect(errJson.title).toBeDefined();
    }

    // Attacker sends valid query trying to see victim's data
    const exfilRes = await app.inject({
      method: 'POST',
      url: '/v1/analytics/query',
      headers: { 'X-Tenant-Id': 'tenant_attacker' },
      payload: {
        project_id: 'proj_secret',
        time_range: { start: '2026-10-01T00:00:00.000Z', end: '2026-10-31T00:00:00.000Z' },
        metrics: ['count()'],
      },
    });

    expect(exfilRes.statusCode).toBe(200);
    // Attacker gets exactly 0 rows - zero data leak!
    expect(exfilRes.json().data[0].count).toBe(0);
  });

  // =========================================================================
  // Test 4.4: High-Cardinality Partition Pruning & Time-Bucket Verification
  // =========================================================================
  it('scans strictly target monthly partitions and computes hourly time buckets accurately', async () => {
    // Seed events across 4 distinct monthly partitions: 2026-07, 2026-08, 2026-09, 2026-10
    const multiMonthEvents: ClickHouseEventRecord[] = [
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(
        createEvent('01ARZ3NDEKTSV4RRFFQ69M0701', 1, { val: 10 }, '2026-07-15T12:00:00.000Z')
      ),
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(
        createEvent('01ARZ3NDEKTSV4RRFFQ69M0801', 1, { val: 20 }, '2026-08-15T12:00:00.000Z')
      ),
      // 2 events in 2026-09
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(
        createEvent('01ARZ3NDEKTSV4RRFFQ69M0901', 1, { val: 30 }, '2026-09-10T08:00:00.000Z')
      ),
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(
        createEvent('01ARZ3NDEKTSV4RRFFQ69M0902', 1, { val: 70 }, '2026-09-10T08:30:00.000Z')
      ),
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(
        createEvent('01ARZ3NDEKTSV4RRFFQ69M1001', 1, { val: 40 }, '2026-10-05T12:00:00.000Z')
      ),
    ];
    await clickhouse.insertBatch(multiMonthEvents);

    // Query specifically for 2026-09-10
    const queryRes = await clickhouse.query({
      tenant_id: 'tenant_stress_olap',
      project_id: 'proj_analytics',
      start_time: '2026-09-10T00:00:00.000Z',
      end_time: '2026-09-10T23:59:59.000Z',
      metrics: ['count()', 'avg(val)', 'p95(val)'],
    });

    // Partition Pruning Assertion:
    // Only the '202609' partition was scanned! Partitions 202607, 202608, 202610 were pruned!
    expect(queryRes.meta.partitionsScanned).toEqual(['202609']);
    expect(queryRes.meta.rowsScanned).toBe(2);

    expect(queryRes.data).toHaveLength(1);
    expect(queryRes.data[0].count).toBe(2);
    expect(queryRes.data[0]['avg(val)']).toBe(50); // (30 + 70) / 2
    expect(queryRes.data[0]['p95(val)']).toBe(70);
  });
});
