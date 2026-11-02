import { describe, it, expect, beforeEach } from 'vitest';
import { KafkaProducer } from '../../apps/ingestion-service/src/producer/kafka-producer.js';
import { StreamEventProcessor } from '../../workers/event-processor/src/consumer/kafka-consumer.js';
import { DLQReplayTool } from '../../scripts/replay-dlq.js';
import type { EventEnvelope, KafkaEventMessage } from '../../packages/contracts/src/index.js';

describe('Hard-Level Test Type 3: Chaos, Fault Injection & Resilient Recovery Testing', () => {
  let producer: KafkaProducer;

  beforeEach(() => {
    producer = new KafkaProducer('nexusgrid.events.raw', 12);
  });

  const createEvent = (id: string, name: string = 'order.created'): EventEnvelope => ({
    event_id: id,
    tenant_id: 'tenant_chaos',
    project_id: 'proj_chaos',
    event_name: name,
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    properties: { trace: 'chaos_test' },
  });

  // =========================================================================
  // Test 3.1: Poison Pill Barrage Drill (Zero Partition Stall Guarantee)
  // =========================================================================
  it('survives an adversarial stream of 50 mixed poison-pills without worker crash or partition blockage', async () => {
    const processedEvents: EventEnvelope[] = [];
    const worker = new StreamEventProcessor(producer, async (event) => {
      processedEvents.push(event);
    });

    const topic = 'nexusgrid.events.raw';
    const totalMessages = 50;

    // Produce 50 messages with alternating poison pills:
    // Slots 0-9: Valid (10)
    // Slots 10-19: Corrupted unparseable bytes (10)
    // Slots 20-29: Invalid envelope shape (10)
    // Slots 30-39: Valid (10)
    // Slots 40-49: Malformed JSON syntax (10)
    for (let i = 0; i < totalMessages; i++) {
      if ((i >= 0 && i < 10) || (i >= 30 && i < 40)) {
        // Valid
        const evt = createEvent(`01ARZ3NDEKTSV4RRFFQ69G${i.toString().padStart(5, '0')}`);
        await producer.publish(evt, `cor_valid_${i}`);
      } else if (i >= 10 && i < 20) {
        // Corrupted JSON bytes
        producer.publishedMessages.get(topic)!.push({
          key: 'tenant_chaos:proj_chaos',
          value: '<<CORRUPTED_NON_JSON_BYTES_CHUNK>>',
          headers: {
            'x-event-id': `poison_raw_${i}`,
            'x-tenant-id': 'tenant_chaos',
            'x-project-id': 'proj_chaos',
            'x-schema-version': '1',
            'x-correlation-id': `cor_poison_${i}`,
            'x-retry-count': '0',
            'x-timestamp': new Date().toISOString(),
          },
        });
      } else if (i >= 20 && i < 30) {
        // Envelope missing required tenant_id
        producer.publishedMessages.get(topic)!.push({
          key: 'tenant_chaos:proj_chaos',
          value: JSON.stringify({ missing_tenant_and_event: true }),
          headers: {
            'x-event-id': `poison_shape_${i}`,
            'x-tenant-id': 'tenant_chaos',
            'x-project-id': 'proj_chaos',
            'x-schema-version': '1',
            'x-correlation-id': `cor_poison_${i}`,
            'x-retry-count': '0',
            'x-timestamp': new Date().toISOString(),
          },
        });
      } else {
        // Truncated JSON
        producer.publishedMessages.get(topic)!.push({
          key: 'tenant_chaos:proj_chaos',
          value: '{"event_id": "01ARZ3", "incomplete": ',
          headers: {
            'x-event-id': `poison_trunc_${i}`,
            'x-tenant-id': 'tenant_chaos',
            'x-project-id': 'proj_chaos',
            'x-schema-version': '1',
            'x-correlation-id': `cor_poison_${i}`,
            'x-retry-count': '0',
            'x-timestamp': new Date().toISOString(),
          },
        });
      }
    }

    expect(producer.getMessages(topic)).toHaveLength(totalMessages);

    // Stream worker processes entire stream
    await worker.processTopic(topic);

    // Chaos assertions:
    // 1. Worker never threw unhandled rejection
    // 2. Exactly 20 valid events were successfully processed
    expect(processedEvents).toHaveLength(20);
    expect(worker.stats.processedCount).toBe(20);

    // 3. Exactly 30 poison messages were routed to DLQ
    expect(worker.stats.dlqCount).toBe(30);
    const dlqMessages = producer.getMessages('nexusgrid.events.dlq');
    expect(dlqMessages).toHaveLength(30);

    // 4. Committed offset strictly reaches 50 -> Zero partition stall!
    expect(worker.getCommittedOffset(topic, 0)).toBe(50);
  });

  // =========================================================================
  // Test 3.2: Flapping Downstream Network Recovery Drill
  // =========================================================================
  it('traverses backoff retry stages during transient downstream flapping and recovers without hitting DLQ', async () => {
    let failureCount = 0;

    // Simulated flaky downstream service: fails 2 times, succeeds on 3rd attempt
    const flakyWorker = new StreamEventProcessor(producer, async () => {
      failureCount++;
      if (failureCount <= 2) {
        throw new Error('ETIMEDOUT: Connection pool exhausted');
      }
      // Succeeds on 3rd attempt
    });

    const topic = 'nexusgrid.events.raw';
    const event = createEvent('01ARZ3NDEKTSV4RRFFQ69G5FFL');

    // Attempt 1: Ingest into primary topic
    await producer.publish(event, 'cor_flaky_1');
    await flakyWorker.processTopic(topic);

    // Must be routed to retry.10s topic with retry count = 1
    const retry10s = producer.getMessages('nexusgrid.events.retry.10s');
    expect(retry10s).toHaveLength(1);
    expect(retry10s[0].headers['x-retry-count']).toBe('1');
    expect(flakyWorker.stats.retriedCount).toBe(1);

    // Attempt 2: Worker processes retry.10s
    await flakyWorker.processTopic('nexusgrid.events.retry.10s');
    const retry1m = producer.getMessages('nexusgrid.events.retry.1m');
    expect(retry1m).toHaveLength(1);
    expect(retry1m[0].headers['x-retry-count']).toBe('2');
    expect(flakyWorker.stats.retriedCount).toBe(2);

    // Attempt 3: Flaky service recovers! Worker processes retry.1m
    await flakyWorker.processTopic('nexusgrid.events.retry.1m');

    // Downstream succeeded: DLQ remains empty!
    expect(flakyWorker.stats.dlqCount).toBe(0);
    expect(producer.getMessages('nexusgrid.events.dlq')).toHaveLength(0);
  });

  // =========================================================================
  // Test 3.3: Worker Crash & State Resumption Drill
  // =========================================================================
  it('resumes exactly from committed offset after worker crash, ensuring zero duplicated processing', async () => {
    const topic = 'nexusgrid.events.raw';
    const totalEvents = 60;
    const crashOffset = 25;

    // Produce 60 events
    for (let i = 1; i <= totalEvents; i++) {
      const evt = createEvent(`01ARZ3NDEKTSV4RRFFQ69G${i.toString().padStart(5, '0')}`);
      await producer.publish(evt, `cor_${i}`);
    }

    const worker1Processed: string[] = [];
    const worker1 = new StreamEventProcessor(producer, async (event) => {
      worker1Processed.push(event.event_id);
    });

    // Worker 1 processes up to crashOffset (25 events) then crashes
    const messages = producer.getMessages(topic);
    for (let i = 0; i < crashOffset; i++) {
      await worker1.consumeRecord(messages[i], topic, 0, i + 1);
    }

    expect(worker1Processed).toHaveLength(crashOffset);
    expect(worker1.getCommittedOffset(topic, 0)).toBe(crashOffset);

    // SIMULATED SIGKILL / CRASH: Worker 1 terminates abruptly!
    // A new replacement worker boots up
    const worker2Processed: string[] = [];
    const worker2 = new StreamEventProcessor(producer, async (event) => {
      worker2Processed.push(event.event_id);
    });

    // Replacement worker inherits the committed offset from broker
    // (In our adapter, we sync the committed offset)
    (worker2 as any).committedOffsets = (worker1 as any).committedOffsets;

    // Worker 2 resumes processing the topic
    await worker2.processTopic(topic);

    // Assert Worker 2 only processed remaining events (25 to 60 = 35 events)
    expect(worker2Processed).toHaveLength(totalEvents - crashOffset);
    expect(worker2.getCommittedOffset(topic, 0)).toBe(totalEvents);

    // Verify zero overlap between Worker 1 and Worker 2!
    const set1 = new Set(worker1Processed);
    for (const id of worker2Processed) {
      expect(set1.has(id)).toBe(false);
    }
  });

  // =========================================================================
  // Test 3.4: Operator DLQ Surgical Patch & Safe Replay Drill
  // =========================================================================
  it('enables operators to inspect, apply surgical payload corrections, and replay DLQ messages safely', async () => {
    const replayTool = new DLQReplayTool(producer);

    // Seed DLQ with 3 poisoned messages (schema version missing)
    for (let i = 1; i <= 3; i++) {
      const brokenPayload = {
        event_id: `01ARZ3NDEKTSV4RRFFQ69GPAT${i}`,
        tenant_id: 'tenant_chaos',
        project_id: 'proj_chaos',
        event_name: 'sensor.reading',
        occurred_at: new Date().toISOString(),
        // missing schema_version and properties!
      };

      producer.publishedMessages.set('nexusgrid.events.dlq', [
        ...(producer.publishedMessages.get('nexusgrid.events.dlq') || []),
        {
          key: 'tenant_chaos:proj_chaos',
          value: JSON.stringify(brokenPayload),
          headers: {
            'x-event-id': brokenPayload.event_id,
            'x-tenant-id': 'tenant_chaos',
            'x-project-id': 'proj_chaos',
            'x-schema-version': '0',
            'x-correlation-id': `cor_dlq_${i}`,
            'x-retry-count': '5',
            'x-original-topic': 'nexusgrid.events.raw',
            'x-error-reason': 'Schema validation failed: missing properties',
            'x-timestamp': new Date().toISOString(),
          },
        },
      ]);
    }

    // Step 1: Operator runs Dry-Run to inspect DLQ
    const dryRun = await replayTool.runReplay({ dryRun: true, tenantId: 'tenant_chaos' });
    expect(dryRun.scanned).toBe(3);
    expect(dryRun.eligible).toBe(3);
    expect(dryRun.replayed).toBe(0); // Dry-run leaves DLQ unchanged!
    expect(producer.getMessages('nexusgrid.events.dlq')).toHaveLength(3);

    // Step 2: Operator applies surgical patch to the messages in DLQ
    const dlqMessages = producer.getMessages('nexusgrid.events.dlq');
    for (const msg of dlqMessages) {
      const parsed = JSON.parse(msg.value);
      parsed.schema_version = 1;
      parsed.properties = { sensor_id: 'temp_01', reading: 22.4 };
      msg.value = JSON.stringify(parsed);
    }

    // Step 3: Operator executes real replay
    const executeRun = await replayTool.runReplay({ dryRun: false, tenantId: 'tenant_chaos' });
    expect(executeRun.replayed).toBe(3);
    // DLQ cleared of replayed messages
    expect(producer.getMessages('nexusgrid.events.dlq')).toHaveLength(0);

    // Replayed messages returned to primary topic
    const replayed = producer.getMessages('nexusgrid.events.raw');
    expect(replayed).toHaveLength(3);

    // Step 4: Stream worker processes the replayed messages
    const replayedWorkerEvents: EventEnvelope[] = [];
    const streamWorker = new StreamEventProcessor(producer, async (event) => {
      replayedWorkerEvents.push(event);
    });

    await streamWorker.processTopic('nexusgrid.events.raw');

    // All 3 replayed events processed successfully without bouncing back!
    expect(replayedWorkerEvents).toHaveLength(3);
    expect(streamWorker.stats.dlqCount).toBe(0);
  });
});
