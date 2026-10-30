import { describe, it, expect, beforeEach } from 'vitest';
import { KafkaProducer } from '../../apps/ingestion-service/src/producer/kafka-producer.js';
import { StreamEventProcessor } from '../../workers/event-processor/src/consumer/kafka-consumer.js';
import { ErrorClassifier } from '../../workers/event-processor/src/router/error-classifier.js';
import { DLQReplayTool } from '../../scripts/replay-dlq.js';
import { OutboxPoller } from '../../workers/outbox-publisher/src/poller/outbox-poller.js';
import { InMemoryPostgresAdapter } from '../../packages/database/src/index.js';
import type { EventEnvelope, KafkaEventMessage } from '../../packages/contracts/src/index.js';

describe('Phase 3 - Kafka Streaming Backbone, Workers, Retries, DLQ, and Outbox', () => {
  let producer: KafkaProducer;
  let db: InMemoryPostgresAdapter;

  beforeEach(() => {
    producer = new KafkaProducer('nexusgrid.events.raw', 12);
    db = new InMemoryPostgresAdapter();
  });

  const makeValidEvent = (id: string): EventEnvelope => ({
    event_id: id,
    tenant_id: 'tenant_acme',
    project_id: 'proj_web',
    event_name: 'page.viewed',
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    properties: { url: '/home', browser: 'Chrome' },
  });

  it('stream worker processes valid event batch and advances committed offsets', async () => {
    const processedEvents: EventEnvelope[] = [];
    const worker = new StreamEventProcessor(producer, async (event) => {
      processedEvents.push(event);
    });

    // Publish 5 valid events
    for (let i = 1; i <= 5; i++) {
      const evt = makeValidEvent(`01ARZ3NDEKTSV4RRFFQ69G5FA${i}`);
      await producer.publish(evt, `cor_${i}`);
    }

    expect(producer.getMessages('nexusgrid.events.raw')).toHaveLength(5);

    // Worker processes topic
    await worker.processTopic('nexusgrid.events.raw');

    expect(processedEvents).toHaveLength(5);
    expect(worker.stats.processedCount).toBe(5);
    expect(worker.getCommittedOffset('nexusgrid.events.raw', 0)).toBe(5);
  });

  it('isolates poison message by routing to DLQ without stalling partition progress', async () => {
    const worker = new StreamEventProcessor(producer);

    // 1. Publish valid event
    const evt1 = makeValidEvent('01ARZ3NDEKTSV4RRFFQ69G5F10');
    await producer.publish(evt1, 'cor_1');

    // 2. Publish poison message (corrupted invalid JSON syntax)
    const poisonMessage: KafkaEventMessage = {
      key: 'tenant_acme:proj_web',
      value: '{ "malformed_json": missing_quotes_here! }',
      headers: {
        'x-event-id': 'poison_id_999',
        'x-tenant-id': 'tenant_acme',
        'x-project-id': 'proj_web',
        'x-schema-version': '1',
        'x-correlation-id': 'cor_poison',
        'x-retry-count': '0',
        'x-timestamp': new Date().toISOString(),
      },
    };
    producer.publishedMessages.get('nexusgrid.events.raw')!.push(poisonMessage);

    // 3. Publish second valid event
    const evt2 = makeValidEvent('01ARZ3NDEKTSV4RRFFQ69G5F20');
    await producer.publish(evt2, 'cor_2');

    // Worker processes topic
    await worker.processTopic('nexusgrid.events.raw');

    // Stats verification: 2 processed, 1 routed to DLQ
    expect(worker.stats.processedCount).toBe(2);
    expect(worker.stats.dlqCount).toBe(1);
    // Offset must still advance to 3 so partition is NOT stalled!
    expect(worker.getCommittedOffset('nexusgrid.events.raw', 0)).toBe(3);

    // Verify DLQ topic contents
    const dlqMessages = producer.getMessages('nexusgrid.events.dlq');
    expect(dlqMessages).toHaveLength(1);
    expect(dlqMessages[0].headers['x-event-id']).toBe('poison_id_999');
    expect(dlqMessages[0].headers['x-original-topic']).toBe('nexusgrid.events.raw');
    expect(dlqMessages[0].headers['x-error-reason']).toContain('Non-retriable poison');
  });

  it('routes retriable failures to retry topic with incremented retry count', async () => {
    // Process function that fails with a retriable network timeout
    const worker = new StreamEventProcessor(producer, async () => {
      throw new Error('Connection timeout to downstream ClickHouse cluster');
    });

    const evt = makeValidEvent('01ARZ3NDEKTSV4RRFFQ69G5F30');
    await producer.publish(evt, 'cor_retry_test');

    await worker.processTopic('nexusgrid.events.raw');

    expect(worker.stats.retriedCount).toBe(1);

    // Check message sent to retry.10s topic
    const retry10s = producer.getMessages('nexusgrid.events.retry.10s');
    expect(retry10s).toHaveLength(1);
    expect(retry10s[0].headers['x-retry-count']).toBe('1');
    expect(retry10s[0].headers['x-error-reason']).toContain('Retriable failure');
  });

  it('routes to DLQ when retry attempts are exhausted (retry_count >= 5)', async () => {
    const worker = new StreamEventProcessor(producer, async () => {
      throw new Error('Persistent network partition');
    });

    // Create message already retried 5 times
    const exhaustedMessage: KafkaEventMessage = {
      key: 'tenant_acme:proj_web',
      value: JSON.stringify(makeValidEvent('01ARZ3NDEKTSV4RRFFQ69G5F55')),
      headers: {
        'x-event-id': '01ARZ3NDEKTSV4RRFFQ69G5F55',
        'x-tenant-id': 'tenant_acme',
        'x-project-id': 'proj_web',
        'x-schema-version': '1',
        'x-correlation-id': 'cor_exhausted',
        'x-retry-count': '5',
        'x-timestamp': new Date().toISOString(),
      },
    };
    producer.publishedMessages.set('nexusgrid.events.raw', [exhaustedMessage]);

    await worker.processTopic('nexusgrid.events.raw');

    expect(worker.stats.dlqCount).toBe(1);
    const dlq = producer.getMessages('nexusgrid.events.dlq');
    expect(dlq).toHaveLength(1);
    expect(dlq[0].headers['x-error-reason']).toContain('Exhausted all 5 retries');
  });

  it('allows operators to inspect and replay DLQ messages safely using replay tool', async () => {
    const replayTool = new DLQReplayTool(producer);

    // Seed DLQ with 2 messages
    const validEvt = makeValidEvent('01ARZ3NDEKTSV4RRFFQ69G5FRE');
    const dlqMsg1: KafkaEventMessage = {
      key: 'tenant_acme:proj_web',
      value: JSON.stringify(validEvt),
      headers: {
        'x-event-id': validEvt.event_id,
        'x-tenant-id': 'tenant_acme',
        'x-project-id': 'proj_web',
        'x-schema-version': '1',
        'x-correlation-id': 'cor_dlq_1',
        'x-retry-count': '5',
        'x-original-topic': 'nexusgrid.events.raw',
        'x-error-reason': 'ClickHouse cluster temporarily down',
        'x-timestamp': new Date().toISOString(),
      },
    };
    producer.publishedMessages.set('nexusgrid.events.dlq', [dlqMsg1]);

    // 1. Dry Run Replay
    const dryRunReport = await replayTool.runReplay({ dryRun: true });
    expect(dryRunReport.scanned).toBe(1);
    expect(dryRunReport.eligible).toBe(1);
    expect(dryRunReport.replayed).toBe(0); // Dry-run does NOT modify!
    expect(producer.getMessages('nexusgrid.events.dlq')).toHaveLength(1);

    // 2. Real Execution Replay
    const execReport = await replayTool.runReplay({ dryRun: false });
    expect(execReport.replayed).toBe(1);
    // DLQ cleared of replayed event
    expect(producer.getMessages('nexusgrid.events.dlq')).toHaveLength(0);
    // Event returned to primary topic!
    const replayed = producer.getMessages('nexusgrid.events.raw');
    expect(replayed).toHaveLength(1);
    expect(replayed[0].headers['x-event-id']).toBe(validEvt.event_id);
  });

  it('outbox poller reads uncommitted PostgreSQL events and publishes to Kafka', async () => {
    const poller = new OutboxPoller(db, producer);

    // Insert 2 transactional outbox records in DB
    await db.insertOutboxRecord({
      aggregate_type: 'tenant',
      aggregate_id: 'tenant_omega',
      event_type: 'tenant.created',
      payload: { tenant_id: 'tenant_omega', name: 'Omega Tech' },
    });

    await db.insertOutboxRecord({
      aggregate_type: 'project',
      aggregate_id: 'proj_omega_api',
      event_type: 'project.created',
      payload: { tenant_id: 'tenant_omega', project_id: 'proj_omega_api' },
    });

    // Check pending count before poller
    const pendingBefore = await db.fetchUnprocessedOutbox();
    expect(pendingBefore).toHaveLength(2);

    // Run poll batch
    const published = await poller.pollAndPublishBatch();
    expect(published).toBe(2);

    // Check pending count after poller
    const pendingAfter = await db.fetchUnprocessedOutbox();
    expect(pendingAfter).toHaveLength(0);

    // Verify messages reached Kafka integration topic
    const integrationEvents = producer.getMessages('nexusgrid.events.integration');
    expect(integrationEvents).toHaveLength(2);
    expect(integrationEvents[0].headers['x-event-id']).toBeDefined();
  });
});
