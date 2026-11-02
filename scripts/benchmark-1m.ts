import { KafkaProducer, InMemoryClickHouseAdapter } from '../packages/database/src/index.js';
import { EventValidator } from '../apps/ingestion-service/src/validators/event-validator.js';
import { ClickHouseBulkSink } from '../workers/event-processor/src/sink/clickhouse-sink.js';
import type { EventEnvelope } from '../packages/contracts/src/index.js';

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

async function run() {
  console.log('--- Starting 1 Million Event Throughput Benchmark ---');
  const totalEvents = 1_000_000;
  const chunkSize = 50_000;

  const validator = new EventValidator();
  const producer = new KafkaProducer('nexusgrid.events.raw', 12);
  const clickhouse = new InMemoryClickHouseAdapter();
  const sink = new ClickHouseBulkSink(clickhouse, {
    batchSize: 10_000,
    minFlushBatchSize: 5_000,
    enableS3Archive: false, // In benchmark focus on ingestion + DB
  });

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

  // Benchmark 1: Validation Throughput (100k sample for fast validation check)
  console.log('\n[1/4] Benchmarking Event Validation...');
  const valCount = 100_000;
  const startVal = performance.now();
  for (let i = 1; i <= valCount; i++) {
    const evt = sampleEvent(i);
    const res = validator.validateSingle(evt);
    if (!res.valid) throw new Error(`Validation failed on item ${i}`);
  }
  const valDurationS = (performance.now() - startVal) / 1000;
  const valThroughput = Math.round(valCount / valDurationS);
  console.log(`  -> Validated ${valCount.toLocaleString()} events in ${valDurationS.toFixed(2)}s (${valThroughput.toLocaleString()} events/sec)`);

  // Benchmark 2: Kafka Producer Virtual Salting Throughput
  console.log('\n[2/4] Benchmarking Kafka Virtual Salt Partitioning on 1,000,000 Events...');
  const partitionCounts = new Array(12).fill(0);
  const startKafka = performance.now();
  for (let i = 1; i <= totalEvents; i++) {
    const { partition } = producer.computePartition(
      `tenant_${i % 100}`,
      `proj_${i % 10}`,
      `usr_${i % 5000}`,
      undefined
    );
    partitionCounts[partition]++;
  }
  const kafkaDurationS = (performance.now() - startKafka) / 1000;
  const kafkaThroughput = Math.round(totalEvents / kafkaDurationS);
  console.log(`  -> Partitioned 1,000,000 events in ${kafkaDurationS.toFixed(2)}s (${kafkaThroughput.toLocaleString()} events/sec)`);
  console.log('  -> Partition distribution across 12 partitions:');
  partitionCounts.forEach((cnt, p) => {
    console.log(`     Partition ${p.toString().padStart(2, '0')}: ${cnt.toLocaleString()} events (${((cnt / totalEvents) * 100).toFixed(2)}%)`);
  });

  // Benchmark 3: ClickHouse Bulk Sink Streaming Throughput (1,000,000 Events)
  console.log('\n[3/4] Streaming 1,000,000 Events into ClickHouse Bulk Sink in chunks of ' + chunkSize.toLocaleString() + '...');
  const startCH = performance.now();
  let processed = 0;

  for (let c = 0; c < totalEvents / chunkSize; c++) {
    const chunkStart = performance.now();
    const batch: EventEnvelope[] = [];
    for (let i = 1; i <= chunkSize; i++) {
      const globalIdx = c * chunkSize + i;
      batch.push(sampleEvent(globalIdx));
    }
    await sink.pushBatch(batch);
    processed += chunkSize;
    const chunkDurationMs = performance.now() - chunkStart;
    const chunkThroughput = Math.round((chunkSize / chunkDurationMs) * 1000);
    const memMb = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
    if ((c + 1) % 4 === 0 || c === totalEvents / chunkSize - 1) {
      console.log(`  -> Processed ${processed.toLocaleString()} / ${totalEvents.toLocaleString()} events | Chunk: ${chunkThroughput.toLocaleString()} evt/sec | Heap: ${memMb} MB`);
    }
  }
  await sink.flush(true);
  const chDurationS = (performance.now() - startCH) / 1000;
  const chThroughput = Math.round(totalEvents / chDurationS);
  console.log(`  -> Completed 1,000,000 events ClickHouse sink in ${chDurationS.toFixed(2)}s (${chThroughput.toLocaleString()} events/sec)`);
  console.log(`  -> Total rows in ClickHouse: ${(await clickhouse.count('tenant_1')).toLocaleString()} for tenant_1`);

  // Benchmark 4: Analytical Query Execution on 1M dataset
  console.log('\n[4/4] Benchmarking OLAP Analytical Queries across 1,000,000 dataset...');
  const startQuery = performance.now();
  const qRes = await clickhouse.query({
    tenant_id: 'tenant_1',
    project_id: 'proj_1',
    start_time: new Date(baseTime - 86400 * 1000).toISOString(),
    end_time: new Date(baseTime + 1000).toISOString(),
    metrics: ['count()', 'avg(latency_ms)', 'p95(latency_ms)'],
  });
  const queryDurationMs = (performance.now() - startQuery).toFixed(2);
  console.log(`  -> Scanned rows: ${qRes.meta.rowsScanned.toLocaleString()}`);
  console.log(`  -> Query executed in ${queryDurationMs}ms`);
  console.log(`  -> Query data:`, qRes.data[0]);

  console.log('\n============================================================');
  console.log('               1M THROUGHPUT BENCHMARK SUMMARY               ');
  console.log('============================================================');
  console.log(` Total Events Processed:   1,000,000`);
  console.log(` Validation Throughput:    ${valThroughput.toLocaleString()} events/sec`);
  console.log(` Virtual Salt Partition:   ${kafkaThroughput.toLocaleString()} events/sec`);
  console.log(` Bulk Sink Throughput:     ${chThroughput.toLocaleString()} events/sec`);
  console.log(` End-to-End Elapsed Time:  ${(chDurationS + kafkaDurationS).toFixed(2)}s`);
  console.log('============================================================\n');
}

run().catch((err) => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});
