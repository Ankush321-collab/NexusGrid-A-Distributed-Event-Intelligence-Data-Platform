import { describe, it, expect } from 'vitest';
import { KafkaProducer, InMemoryClickHouseAdapter } from '../../packages/database/src/index.js';
import { EventValidator } from '../../apps/ingestion-service/src/validators/event-validator.js';
import { ClickHouseBulkSink } from '../../workers/event-processor/src/sink/clickhouse-sink.js';
import type { EventEnvelope } from '../../packages/contracts/src/index.js';

describe('High-Throughput Load Benchmark (1,000,000 Events)', () => {
  const CROCKFORD_BASE32 = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
  function toCrockford(num: number, len: number = 4): string {
    let s = '';
    let n = num;
    while (n > 0) {
      s = CROCKFORD_BASE32[n % 32] + s;
      n = Math.floor(n / 32);
    }
    return s.padStart(len, '0');
  }

  const baseTime = Date.now();
  const sampleEvent = (idx: number): EventEnvelope => ({
    event_id: `01ARZ3NDEKTSV4RRFFQ69G${toCrockford(idx, 4)}`,
    tenant_id: `tenant_${idx % 100}`,
    project_id: `proj_${idx % 10}`,
    event_name: 'api.request.completed',
    occurred_at: new Date(baseTime - (idx % 86400) * 1000).toISOString(),
    schema_version: 1,
    properties: {
      latency_ms: (idx * 7) % 500,
      status_code: idx % 20 === 0 ? 500 : 200,
      user_id: `usr_${idx % 5000}`,
      order_id: `ord_${idx}`,
    },
  });

  // =========================================================================
  // Test 1: Kafka Virtual Salt Partitioning Throughput & Skew Defense (1M Events)
  // =========================================================================
  it('partitions 1,000,000 events at >150k events/sec with uniform distribution across 12 partitions', () => {
    const totalEvents = 1_000_000;
    const partitionCounts = new Array(12).fill(0);
    const producer = new KafkaProducer('nexusgrid.events.raw', 12);

    const start = performance.now();
    for (let i = 1; i <= totalEvents; i++) {
      const { partition } = producer.computePartition(
        `tenant_${i % 100}`,
        `proj_${i % 10}`,
        `usr_${i % 5000}`,
        undefined
      );
      partitionCounts[partition]++;
    }
    const durationS = (performance.now() - start) / 1000;
    const throughput = Math.round(totalEvents / durationS);

    // Throughput assertion: Must exceed 150,000 events/second
    expect(throughput).toBeGreaterThan(150_000);

    // Uniform distribution assertion:
    // Ideal per partition = 8.33%. With 1M events, each partition should receive between 7.0% and 9.5%
    for (let p = 0; p < 12; p++) {
      const percentage = (partitionCounts[p] / totalEvents) * 100;
      expect(percentage).toBeGreaterThan(7.0);
      expect(percentage).toBeLessThan(9.5);
    }
  });

  // =========================================================================
  // Test 2: ClickHouse Bulk Sink Streaming Throughput (1M Events)
  // =========================================================================
  it('streams 1,000,000 events into ClickHouse Bulk Sink at >60k events/sec in micro-batches', async () => {
    const totalEvents = 1_000_000;
    const chunkSize = 50_000;
    const clickhouse = new InMemoryClickHouseAdapter();
    const sink = new ClickHouseBulkSink(clickhouse, {
      batchSize: 10_000,
      minFlushBatchSize: 5_000,
      enableS3Archive: false,
    });

    const start = performance.now();
    for (let c = 0; c < totalEvents / chunkSize; c++) {
      const batch: EventEnvelope[] = [];
      for (let i = 1; i <= chunkSize; i++) {
        batch.push(sampleEvent(c * chunkSize + i));
      }
      await sink.pushBatch(batch);
    }
    await sink.flush(true);

    const durationS = (performance.now() - start) / 1000;
    const throughput = Math.round(totalEvents / durationS);

    // Throughput assertion: Must exceed 60,000 events/second
    expect(throughput).toBeGreaterThan(60_000);
    expect(clickhouse.totalInsertedRows).toBe(totalEvents);

    // Multi-tenant count integrity
    const tenant1Count = await clickhouse.count('tenant_1');
    expect(tenant1Count).toBe(10_000); // 1,000,000 / 100 tenants = 10,000 per tenant
  }, 30000);

  // =========================================================================
  // Test 3: Analytical OLAP Query Performance on 1M Ingested Events
  // =========================================================================
  it('executes complex analytical aggregations and percentiles over 1M records in under 300ms', async () => {
    const totalEvents = 1_000_000;
    const chunkSize = 100_000;
    const clickhouse = new InMemoryClickHouseAdapter();
    const sink = new ClickHouseBulkSink(clickhouse, {
      batchSize: 20_000,
      minFlushBatchSize: 10_000,
      enableS3Archive: false,
    });

    for (let c = 0; c < totalEvents / chunkSize; c++) {
      const batch: EventEnvelope[] = [];
      for (let i = 1; i <= chunkSize; i++) {
        batch.push(sampleEvent(c * chunkSize + i));
      }
      await sink.pushBatch(batch);
    }
    await sink.flush(true);

    // Query across tenant_1 with aggregations and percentiles
    const startQuery = performance.now();
    const res = await clickhouse.query({
      tenant_id: 'tenant_1',
      project_id: 'proj_1',
      start_time: new Date(baseTime - 86400 * 1000).toISOString(),
      end_time: new Date(baseTime + 1000).toISOString(),
      metrics: ['count()', 'avg(latency_ms)', 'p95(latency_ms)'],
    });
    const queryDurationMs = performance.now() - startQuery;

    // Scan performance assertion: Must execute in under 300ms
    expect(queryDurationMs).toBeLessThan(300);
    expect(res.meta.rowsScanned).toBe(totalEvents);
    expect(res.data).toHaveLength(1);
    expect(res.data[0].count).toBe(10_000);
    expect(res.data[0]['avg(latency_ms)']).toBeGreaterThan(0);
    expect(res.data[0]['p95(latency_ms)']).toBeGreaterThan(res.data[0]['avg(latency_ms)']);
  }, 30000);
});
