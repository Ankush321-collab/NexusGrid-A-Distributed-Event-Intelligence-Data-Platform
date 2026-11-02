import { describe, it, expect, beforeEach } from 'vitest';
import { InMemoryClickHouseAdapter, type ClickHouseClientInterface } from '../../packages/database/src/index.js';
import { ClickHouseBulkSink } from '../../workers/event-processor/src/sink/clickhouse-sink.js';
import type { EventEnvelope } from '../../packages/contracts/src/index.js';

describe('Phase 4 - ClickHouse Analytical Storage, ReplacingMergeTree, and S3 Cold Archival', () => {
  let clickhouse: InMemoryClickHouseAdapter;
  let sink: ClickHouseBulkSink;

  beforeEach(() => {
    clickhouse = new InMemoryClickHouseAdapter();
    sink = new ClickHouseBulkSink(clickhouse, {
      batchSize: 50,
      flushIntervalMs: 100,
      enableS3Archive: true,
    });
  });

  const createEvent = (
    id: string,
    occurredAt: string = '2026-10-30T12:00:00.000Z',
    tenant: string = 'tenant_ch_test',
    project: string = 'proj_ch_test',
    props: Record<string, any> = {}
  ): EventEnvelope => ({
    event_id: id,
    tenant_id: tenant,
    project_id: project,
    event_name: 'api.request.completed',
    occurred_at: occurredAt,
    received_at: new Date().toISOString(),
    schema_version: 1,
    properties: { latency_ms: 45.5, status_code: 200, ...props },
  });

  // =========================================================================
  // Test 4.1: Bulk Micro-Batch Ingestion & Cold Storage S3 Archival
  // =========================================================================
  it('buffers events and executes atomic micro-batch writes to ClickHouse and cold S3 archival', async () => {
    const totalEvents = 100;
    const events: EventEnvelope[] = [];

    for (let i = 1; i <= totalEvents; i++) {
      events.push(createEvent(`01ARZ3NDEKTSV4RRFFQ69G${i.toString().padStart(5, '0')}`));
    }

    // Push all events to sink
    await sink.pushBatch(events);

    // Flush any pending records
    const flushRes = await sink.flush();

    expect(sink.totalFlushedToClickHouse).toBe(totalEvents);
    expect(sink.totalArchivedToS3).toBe(totalEvents);
    expect(sink.archivedBatches.length).toBeGreaterThanOrEqual(1);

    // Verify S3 Key format: s3://nexusgrid-archives/{tenant_id}/{YYYY}/{MM}/{DD}/events_{batch_id}.json
    const s3Batch = sink.archivedBatches[0];
    expect(s3Batch.s3Key.startsWith('s3://nexusgrid-archives/tenant_ch_test/')).toBe(true);
    expect(s3Batch.s3Key.endsWith('.json')).toBe(true);

    // Verify ClickHouse count
    const totalInDb = await clickhouse.count('tenant_ch_test', 'proj_ch_test');
    expect(totalInDb).toBe(totalEvents);
  });

  // =========================================================================
  // Test 4.2: ReplacingMergeTree Deduplication (FINAL Mechanics)
  // =========================================================================
  it('deduplicates duplicate event deliveries preserving the latest received timestamp on FINAL', async () => {
    const eventId = '01ARZ3NDEKTSV4RRFFQ69GDEDUP';
    const originalTime = '2026-10-30T12:00:00.000Z';

    // 1. Initial Delivery with received_at: T1
    const event1 = createEvent(eventId, originalTime, 'tenant_dedupe', 'proj_dedupe');
    event1.received_at = '2026-10-30T12:00:01.000Z';
    event1.properties = { attempt: 1, status_code: 500 };

    // 2. Duplicate Delivery with received_at: T2 (later)
    const event2 = createEvent(eventId, originalTime, 'tenant_dedupe', 'proj_dedupe');
    event2.received_at = '2026-10-30T12:00:05.000Z';
    event2.properties = { attempt: 2, status_code: 200 };

    await clickhouse.insertBatch([
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(event1),
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(event2),
    ]);

    // Before merge: table contains 2 rows
    expect(await clickhouse.count('tenant_dedupe', 'proj_dedupe')).toBe(2);

    // Execute OPTIMIZE TABLE FINAL (or query with final: true)
    const queryResult = await clickhouse.query({
      tenant_id: 'tenant_dedupe',
      project_id: 'proj_dedupe',
      start_time: '2026-10-30T00:00:00.000Z',
      end_time: '2026-10-30T23:59:59.000Z',
      final: true, // ReplacingMergeTree FINAL
    });

    // Exactly 1 row returned after deduplication
    expect(queryResult.data).toHaveLength(1);
    expect(queryResult.data[0].event_id).toBe(eventId);
    expect(queryResult.data[0].received_at).toBe('2026-10-30T12:00:05.000Z');
    expect(queryResult.data[0].properties_numeric.status_code).toBe(200);

    // Run physical table optimization
    const deduplicatedRows = await clickhouse.optimizeTableFinal();
    expect(deduplicatedRows).toBe(1);
    expect(await clickhouse.count('tenant_dedupe', 'proj_dedupe')).toBe(1);
  });

  // =========================================================================
  // Test 4.3: Strict Multi-Tenant Analytical Query Isolation
  // =========================================================================
  it('strictly isolates analytical query results by tenant_id and project_id boundaries', async () => {
    // Tenant Alpha
    const alphaEvt = createEvent('01ARZ3NDEKTSV4RRFFQ69GALPHA', '2026-10-30T12:00:00.000Z', 'tenant_alpha', 'proj_alpha');
    // Tenant Beta
    const betaEvt = createEvent('01ARZ3NDEKTSV4RRFFQ69GBETA', '2026-10-30T12:00:00.000Z', 'tenant_beta', 'proj_beta');

    await clickhouse.insertBatch([
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(alphaEvt),
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(betaEvt),
    ]);

    // Query Tenant Alpha
    const resAlpha = await clickhouse.query({
      tenant_id: 'tenant_alpha',
      project_id: 'proj_alpha',
      start_time: '2026-10-30T00:00:00.000Z',
      end_time: '2026-10-30T23:59:59.000Z',
    });

    expect(resAlpha.data).toHaveLength(1);
    expect(resAlpha.data[0].tenant_id).toBe('tenant_alpha');

    // Query Tenant Beta with Project Alpha (Wrong combination) -> Must return 0 rows!
    const resMismatched = await clickhouse.query({
      tenant_id: 'tenant_beta',
      project_id: 'proj_alpha',
      start_time: '2026-10-30T00:00:00.000Z',
      end_time: '2026-10-30T23:59:59.000Z',
    });

    expect(resMismatched.data).toHaveLength(0);
  });

  // =========================================================================
  // Test 4.4: Monthly Partition Pruning Verification
  // =========================================================================
  it('scans only matching monthly partitions when time-range filter is applied', async () => {
    // Event in January 2026 (Partition 202601)
    const janEvt = createEvent('01ARZ3NDEKTSV4RRFFQ69G0JAN', '2026-01-15T10:00:00.000Z', 'tenant_part', 'proj_part');
    // Event in February 2026 (Partition 202602)
    const febEvt = createEvent('01ARZ3NDEKTSV4RRFFQ69G0FEB', '2026-02-15T10:00:00.000Z', 'tenant_part', 'proj_part');
    // Event in March 2026 (Partition 202603)
    const marEvt = createEvent('01ARZ3NDEKTSV4RRFFQ69G0MAR', '2026-03-15T10:00:00.000Z', 'tenant_part', 'proj_part');

    await clickhouse.insertBatch([
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(janEvt),
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(febEvt),
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(marEvt),
    ]);

    // Query only February
    const febQuery = await clickhouse.query({
      tenant_id: 'tenant_part',
      project_id: 'proj_part',
      start_time: '2026-02-01T00:00:00.000Z',
      end_time: '2026-02-28T23:59:59.000Z',
    });

    expect(febQuery.data).toHaveLength(1);
    expect(febQuery.data[0].event_id).toBe(febEvt.event_id);

    // Verify Partition Pruning: only 202602 was scanned! 202601 and 202603 were pruned!
    expect(febQuery.meta.partitionsScanned).toEqual(['202602']);
    expect(febQuery.meta.rowsScanned).toBe(1);
  });

  // =========================================================================
  // Test 4.5: Retention TTL Policy Purging (90-Day Policy)
  // =========================================================================
  it('purges records older than 90 days according to the TTL retention expression', async () => {
    const ninetyOneDaysAgo = new Date(Date.now() - 91 * 24 * 3600 * 1000).toISOString();
    const tenDaysAgo = new Date(Date.now() - 10 * 24 * 3600 * 1000).toISOString();

    const oldEvt = createEvent('01ARZ3NDEKTSV4RRFFQ69GOLDD', ninetyOneDaysAgo, 'tenant_ttl', 'proj_ttl');
    const freshEvt = createEvent('01ARZ3NDEKTSV4RRFFQ69GFRESH', tenDaysAgo, 'tenant_ttl', 'proj_ttl');

    await clickhouse.insertBatch([
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(oldEvt),
      InMemoryClickHouseAdapter.envelopeToClickHouseRecord(freshEvt),
    ]);

    expect(await clickhouse.count('tenant_ttl', 'proj_ttl')).toBe(2);

    // Run 90-day retention TTL
    const purged = await clickhouse.applyRetentionTtl(90);
    expect(purged).toBe(1);

    // Old event purged, fresh event preserved
    expect(await clickhouse.count('tenant_ttl', 'proj_ttl')).toBe(1);
  });

  // =========================================================================
  // Test 4.6: Worker Re-queue on Transient ClickHouse Unavailability
  // =========================================================================
  it('re-queues in-flight batch without data loss if ClickHouse insert fails transiently', async () => {
    let shouldFail = true;
    const failingCh: ClickHouseClientInterface = {
      async insertBatch(records) {
        if (shouldFail) {
          throw new Error('TOO_MANY_PARTS: ClickHouse merge tree backpressure');
        }
        return records.length;
      },
      async query(p) { return { data: [], meta: { rowsScanned: 0, partitionsScanned: [], elapsedMs: 0 } }; },
      async optimizeTableFinal() { return 0; },
      async applyRetentionTtl() { return 0; },
      async count() { return 0; },
      async close() {},
    };

    const resilientSink = new ClickHouseBulkSink(failingCh, { batchSize: 10, flushIntervalMs: 1000 });
    const evt = createEvent('01ARZ3NDEKTSV4RRFFQ69GRECOV');
    await resilientSink.push(evt);

    // First flush fails with TOO_MANY_PARTS
    await expect(resilientSink.flush()).rejects.toThrow(/TOO_MANY_PARTS/);

    // In-flight event is safely retained in buffer!
    expect(resilientSink.getPendingBufferCount()).toBe(1);

    // Downstream recovers
    shouldFail = false;
    const retryRes = await resilientSink.flush();
    expect(retryRes.inserted).toBe(1);
    expect(resilientSink.getPendingBufferCount()).toBe(0);
  });
});
