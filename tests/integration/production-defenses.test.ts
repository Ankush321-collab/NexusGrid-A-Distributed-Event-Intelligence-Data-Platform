import { describe, it, expect, beforeEach } from 'vitest';
import { KafkaProducer, InMemoryPostgresAdapter, InMemoryClickHouseAdapter } from '../../packages/database/src/index.js';
import { StreamEventProcessor } from '../../workers/event-processor/src/consumer/kafka-consumer.js';
import { ClickHouseBulkSink } from '../../workers/event-processor/src/sink/clickhouse-sink.js';
import { DLQReplayTool } from '../../scripts/replay-dlq.js';
import { OutboxPoller } from '../../workers/outbox-publisher/src/poller/outbox-poller.js';
import type { EventEnvelope } from '../../packages/contracts/src/index.js';

describe('Production Defenses & Critical Failure Mode Mitigations', () => {
  let producer: KafkaProducer;
  let db: InMemoryPostgresAdapter;
  let clickhouse: InMemoryClickHouseAdapter;

  beforeEach(() => {
    producer = new KafkaProducer('nexusgrid.events.raw', 12);
    db = new InMemoryPostgresAdapter();
    clickhouse = new InMemoryClickHouseAdapter();
  });

  const makeEvent = (id: string, properties: Record<string, unknown> = {}): EventEnvelope => ({
    event_id: id,
    tenant_id: 'tenant_mega_corp',
    project_id: 'proj_ecommerce',
    event_name: 'order.placed',
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    properties: {
      amount: 199.99,
      currency: 'USD',
      ...properties,
    },
  });

  // =========================================================================
  // Defense 1: Hot Tenant Partition Skew Mitigation
  // =========================================================================
  it('distributes high-volume tenant traffic across multiple virtual partitions using entity salting', async () => {
    const partitionsObserved = new Set<number>();

    // Mega-tenant publishing 60 events with diverse user_ids
    for (let i = 0; i < 60; i++) {
      const evt = makeEvent(`01ARZ3NDEKTSV4RRFFQ69G5F${i.toString().padStart(2, '0')}`, {
        user_id: `usr_${i}`,
      });
      const result = await producer.publish(evt, `cor_${i}`);
      partitionsObserved.add(result.partition);
    }

    // Instead of all 60 events hitting partition 0 or a single partition,
    // entity salting distributes across multiple distinct partitions
    expect(partitionsObserved.size).toBeGreaterThan(1);
    expect(partitionsObserved.size).toBeGreaterThanOrEqual(4);

    const rawMessages = producer.getMessages('nexusgrid.events.raw');
    expect(rawMessages).toHaveLength(60);
    expect(rawMessages[0].headers['x-partition-key']).toBeDefined();
    expect(rawMessages[0].headers['x-partition-key']).toContain('tenant_mega_corp:proj_ecommerce:usr_0');
  });

  // =========================================================================
  // Defense 2: Retry Storm Prevention on Delayed Retry Topics
  // =========================================================================
  it('pauses partition consumption on delayed retry topic until temporal backoff window expires', async () => {
    const worker = new StreamEventProcessor(
      producer,
      async () => {
        throw new Error('SERVICE_UNAVAILABLE: Downstream 3rd party API down');
      },
      undefined,
      { enforceRetryDelay: true }
    );

    const evt = makeEvent('01ARZ3NDEKTSV4RRFFQ69G5G01');
    const queueTime = Date.now();

    // Ingest into raw and attempt 1 (fails -> routes to retry.10s)
    await producer.publish(evt, 'cor_storm_1');
    await worker.processTopic('nexusgrid.events.raw');

    const retry10sMessages = producer.getMessages('nexusgrid.events.retry.10s');
    expect(retry10sMessages).toHaveLength(1);
    expect(retry10sMessages[0].headers['x-retry-count']).toBe('1');
    expect(retry10sMessages[0].headers['x-retry-timestamp']).toBeDefined();

    // Immediate poll at T = +100ms (10-second delay has NOT elapsed)
    await worker.processTopic('nexusgrid.events.retry.10s', 0, queueTime + 100);

    // Assert: Retry storm prevented!
    // The message was NOT processed again, committed offset did NOT advance, delayedCount incremented
    expect(worker.stats.delayedCount).toBe(1);
    expect(worker.getCommittedOffset('nexusgrid.events.retry.10s', 0)).toBe(0);
    expect(producer.getMessages('nexusgrid.events.retry.1m')).toHaveLength(0);

    // Poll at T = +11,000ms (10-second delay HAS elapsed)
    await worker.processTopic('nexusgrid.events.retry.10s', 0, queueTime + 11000);

    // Assert: Now it processes, fails again, and advances to retry.1m
    expect(worker.getCommittedOffset('nexusgrid.events.retry.10s', 0)).toBe(1);
    const retry1mMessages = producer.getMessages('nexusgrid.events.retry.1m');
    expect(retry1mMessages).toHaveLength(1);
    expect(retry1mMessages[0].headers['x-retry-count']).toBe('2');
  });

  // =========================================================================
  // Defense 3: ClickHouse "Too Many Parts" Buffer Defense & Async Insert
  // =========================================================================
  it('enforces write batch buffering thresholds, async insert flags, and backpressure limit', async () => {
    const sink = new ClickHouseBulkSink(clickhouse, {
      batchSize: 5,
      minFlushBatchSize: 5,
      maxBufferLimit: 10,
      enableAsyncInsert: true,
    });

    expect(sink.enableAsyncInsert).toBe(true);

    // Push 3 events: should buffer and NOT trigger insert yet (preventing micro-part fragmentation)
    for (let i = 1; i <= 3; i++) {
      expect(sink.canAcceptMore()).toBe(true);
      await sink.push(makeEvent(`01ARZ3NDEKTSV4RRFFQ69G5H0${i}`));
    }
    expect(sink.getPendingBufferCount()).toBe(3);
    expect(clickhouse.totalInsertedRows).toBe(0);

    // Push 2 more events -> reaches minFlushBatchSize = 5 -> triggers bulk flush
    await sink.push(makeEvent('01ARZ3NDEKTSV4RRFFQ69G5H04'));
    await sink.push(makeEvent('01ARZ3NDEKTSV4RRFFQ69G5H05'));

    expect(clickhouse.totalInsertedRows).toBe(5);
    expect(sink.getPendingBufferCount()).toBe(0);

    // Backpressure test: downstream ClickHouse has a slow in-flight batch
    let unblockInsert: () => void = () => {};
    const slowClient: any = {
      ...clickhouse,
      insertBatch: async (records: any[]) => {
        await new Promise<void>((resolve) => {
          unblockInsert = resolve;
        });
        return records.length;
      },
    };

    const slowSink = new ClickHouseBulkSink(slowClient, {
      batchSize: 4,
      minFlushBatchSize: 4,
      maxBufferLimit: 4,
    });

    // Pushing 4 events triggers flush, but insertBatch is blocked
    const flushPromise = slowSink.pushBatch([
      makeEvent('01ARZ3NDEKTSV4RRFFQ69G5H11'),
      makeEvent('01ARZ3NDEKTSV4RRFFQ69G5H12'),
      makeEvent('01ARZ3NDEKTSV4RRFFQ69G5H13'),
      makeEvent('01ARZ3NDEKTSV4RRFFQ69G5H14'),
    ]);

    // Yield macro-task to let push start the flush
    await new Promise((r) => setTimeout(r, 10));

    // While in-flight to ClickHouse, buffer limit is saturated -> backpressure signaled
    expect(slowSink.canAcceptMore()).toBe(false);

    // Unblock the slow insert
    unblockInsert();
    await flushPromise;

    // After flush completes, backpressure is relieved
    expect(slowSink.canAcceptMore()).toBe(true);
  });

  // =========================================================================
  // Defense 4: PostgreSQL Outbox Table Bloat Pruning & Adaptive Polling
  // =========================================================================
  it('prunes processed outbox entries beyond retention window and uses adaptive backoff', async () => {
    // Insert record 1: processed 10 days ago (expired)
    await db.insertOutboxRecord({
      aggregate_type: 'event',
      aggregate_id: 'agg_old',
      event_type: 'user.created',
      payload: { id: 'old', tenant_id: 'tenant_mega_corp', project_id: 'proj_ecommerce' },
    });
    const rows = await db.fetchUnprocessedOutbox(10);
    await db.markOutboxProcessed(rows[0].id);
    db.outboxRecords[0].processed_at = new Date(Date.now() - 10 * 86400000).toISOString();

    // Insert record 2: processed 1 day ago (recent, keep)
    await db.insertOutboxRecord({
      aggregate_type: 'event',
      aggregate_id: 'agg_recent',
      event_type: 'user.updated',
      payload: { id: 'recent', tenant_id: 'tenant_mega_corp', project_id: 'proj_ecommerce' },
    });
    const rows2 = await db.fetchUnprocessedOutbox(10);
    await db.markOutboxProcessed(rows2[0].id);
    db.outboxRecords[1].processed_at = new Date(Date.now() - 1 * 86400000).toISOString();

    // Insert record 3: unprocessed
    await db.insertOutboxRecord({
      aggregate_type: 'event',
      aggregate_id: 'agg_pending',
      event_type: 'user.active',
      payload: { id: 'pending', tenant_id: 'tenant_mega_corp', project_id: 'proj_ecommerce' },
    });

    expect(db.outboxRecords).toHaveLength(3);

    const poller = new OutboxPoller(db, producer, 50);

    // Prune records older than 7 days
    const prunedCount = await poller.pruneProcessed(7);
    expect(prunedCount).toBe(1);

    // Remaining table has only the recent processed record and pending record
    expect(db.outboxRecords).toHaveLength(2);
    expect(db.outboxRecords.some((r) => r.aggregate_id === 'agg_old')).toBe(false);
    expect(db.outboxRecords.some((r) => r.aggregate_id === 'agg_recent')).toBe(true);
    expect(db.outboxRecords.some((r) => r.aggregate_id === 'agg_pending')).toBe(true);

    // Adaptive backoff test:
    expect(poller.getCurrentIntervalMs()).toBe(50);
    // Poll unprocessed record 3
    const processed = await poller.pollAndPublishBatch(10);
    expect(processed).toBe(1);
    expect(poller.getCurrentIntervalMs()).toBe(50); // reset to base on activity

    // Empty poll -> backs off interval
    await poller.pollAndPublishBatch(10);
    expect(poller.getCurrentIntervalMs()).toBe(75);
    await poller.pollAndPublishBatch(10);
    expect(poller.getCurrentIntervalMs()).toBe(112);
  });

  // =========================================================================
  // Defense 5: DLQ Replay Circuit Breaker & Quarantine Routing
  // =========================================================================
  it('quarantines poisoned messages that repeatedly fail replaying to prevent infinite loops', async () => {
    const replayTool = new DLQReplayTool(producer);

    // Seed DLQ with a message that has already been replayed twice (exhausted)
    producer.getMessages('nexusgrid.events.dlq').push({
      key: 'tenant_mega_corp:proj_ecommerce',
      value: JSON.stringify({ corrupted: 'payload_without_ids' }),
      headers: {
        'x-event-id': '01ARZ3NDEKTSV4RRFFQ69POISON',
        'x-tenant-id': 'tenant_mega_corp',
        'x-project-id': 'proj_ecommerce',
        'x-schema-version': '1',
        'x-correlation-id': 'cor_poison_loop',
        'x-retry-count': '3',
        'x-replay-count': '2', // Replayed 2 times previously
        'x-timestamp': new Date().toISOString(),
      },
    });

    // Run DLQ replay in live mode (dryRun: false) with maxReplayAttempts: 2
    const report = await replayTool.runReplay({ dryRun: false, maxReplayAttempts: 2 });

    // Assert: Circuit breaker prevented replay to raw and quarantined the poison pill
    expect(report.replayed).toBe(0);
    expect(report.quarantined).toBe(1);

    // Raw topic received zero replays
    expect(producer.getMessages('nexusgrid.events.raw')).toHaveLength(0);

    // Quarantine topic holds the dead message with audit quarantine headers
    const quarantine = producer.getMessages('nexusgrid.events.quarantine');
    expect(quarantine).toHaveLength(1);
    expect(quarantine[0].headers['x-quarantine-reason']).toBe('MAX_REPLAY_EXCEEDED_CIRCUIT_BREAKER');
    expect(quarantine[0].headers['x-quarantined-at']).toBeDefined();
  });

  // =========================================================================
  // Defense 6: Graceful Shutdown Lifecycle
  // =========================================================================
  it('safely drains in-flight buffers and halts workers without dropped records during shutdown', async () => {
    const flushedBatches: EventEnvelope[][] = [];
    const worker = new StreamEventProcessor(producer, async (event) => {
      flushedBatches.push([event]);
    });

    // Ingest 3 events into raw topic
    for (let i = 1; i <= 3; i++) {
      await producer.publish(makeEvent(`01ARZ3NDEKTSV4RRFFQ69G5I0${i}`), `cor_shut_${i}`);
    }

    // Process topic partially
    await worker.processTopic('nexusgrid.events.raw');
    expect(worker.stats.processedCount).toBe(3);

    // Trigger graceful shutdown
    await worker.shutdown();
    expect(worker.isShuttingDown).toBe(true);

    // Subsequent process calls are safely ignored without throwing or dropping
    await worker.processTopic('nexusgrid.events.raw');
    expect(worker.stats.processedCount).toBe(3);
  });
});
