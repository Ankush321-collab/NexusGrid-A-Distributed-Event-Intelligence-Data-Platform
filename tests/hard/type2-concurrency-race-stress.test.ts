import { describe, it, expect, beforeEach } from 'vitest';
import { InMemoryRedisClient, InMemoryPostgresAdapter } from '../../packages/database/src/index.js';
import { KafkaProducer } from '../../apps/ingestion-service/src/producer/kafka-producer.js';
import { EventValidator } from '../../apps/ingestion-service/src/validators/event-validator.js';
import { buildIngestionServer } from '../../apps/ingestion-service/src/index.js';
import { OutboxPoller } from '../../workers/outbox-publisher/src/poller/outbox-poller.js';
import type { EventEnvelope } from '../../packages/contracts/src/index.js';

describe('Hard-Level Test Type 2: Extreme Concurrency, Idempotency Stampede & Burst Testing', () => {
  let redis: InMemoryRedisClient;
  let producer: KafkaProducer;
  let validator: EventValidator;
  let db: InMemoryPostgresAdapter;

  beforeEach(() => {
    redis = new InMemoryRedisClient();
    producer = new KafkaProducer('nexusgrid.events.raw', 12);
    validator = new EventValidator();
    db = new InMemoryPostgresAdapter();
  });

  const createEvent = (id: string, key?: string): EventEnvelope => ({
    event_id: id,
    tenant_id: 'tenant_concurrency',
    project_id: 'proj_stress',
    event_name: 'transaction.processed',
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    idempotency_key: key,
    properties: { amount: 99.99, currency: 'USD' },
  });

  // =========================================================================
  // Test 2.1: Thundering Herd / Idempotency Key Parallel Stampede (100 parallel requests)
  // =========================================================================
  it('guarantees exactly-once Kafka publication under 100 parallel concurrent requests with identical Idempotency-Key', async () => {
    const app = await buildIngestionServer({ redis, producer, validator });
    const sharedIdemKey = 'stampede_unique_lock_9901';
    const sampleEvent = createEvent('01ARZ3NDEKTSV4RRFFQ69G5FST', sharedIdemKey);

    const concurrentCount = 100;
    const promises: Promise<any>[] = [];

    // Dispatch 100 requests in the exact same event loop microtask
    for (let i = 0; i < concurrentCount; i++) {
      promises.push(
        app.inject({
          method: 'POST',
          url: '/v1/events',
          headers: {
            'X-Tenant-Id': 'tenant_concurrency',
            'Idempotency-Key': sharedIdemKey,
          },
          payload: sampleEvent,
        })
      );
    }

    const responses = await Promise.all(promises);

    let acceptedCount = 0;
    let conflictCount = 0;
    let cachedHitCount = 0;

    for (const res of responses) {
      if (res.statusCode === 202) {
        acceptedCount++;
        if (res.headers['x-cache-lookup'] === 'HIT') {
          cachedHitCount++;
        }
      } else if (res.statusCode === 409) {
        conflictCount++;
        const parsed = JSON.parse(res.body);
        expect(parsed.code).toBe('ERR_CONCURRENT_IDEMPOTENT_REQUEST');
      }
    }

    // Mathematical guarantees under concurrent race:
    // 1. Total responses must equal 100
    expect(acceptedCount + conflictCount).toBe(concurrentCount);

    // 2. Exactly 1 request initiated the original publication
    expect(acceptedCount).toBeGreaterThanOrEqual(1);

    // 3. Critically: Kafka received STRICTLY 1 published message! Zero duplicate publish!
    const published = producer.getMessages('nexusgrid.events.raw');
    expect(published).toHaveLength(1);
    expect(published[0].headers['x-event-id']).toBe(sampleEvent.event_id);
  });

  // =========================================================================
  // Test 2.2: High-Velocity Token Bucket Boundary Hammer
  // =========================================================================
  it('strictly enforces token bucket capacity and mathematical rejection ratio under a burst of 60 parallel requests', async () => {
    const bucketCapacity = 15;
    const app = await buildIngestionServer({
      redis,
      producer,
      validator,
      maxTokens: bucketCapacity,
      refillRate: 0.001, // No instant refill
    });

    const burstCount = 60;
    const burstPromises: Promise<any>[] = [];

    for (let i = 0; i < burstCount; i++) {
      const uniqueId = `01ARZ3NDEKTSV4RRFFQ69G5F${i.toString().padStart(2, '0')}`;
      burstPromises.push(
        app.inject({
          method: 'POST',
          url: '/v1/events',
          headers: { 'X-Tenant-Id': 'tenant_hammer' },
          payload: createEvent(uniqueId),
        })
      );
    }

    const responses = await Promise.all(burstPromises);

    const status202 = responses.filter((r) => r.statusCode === 202);
    const status429 = responses.filter((r) => r.statusCode === 429);

    // Assert exact token count distribution
    expect(status202.length).toBe(bucketCapacity);
    expect(status429.length).toBe(burstCount - bucketCapacity);

    // All 429 responses must include standard rate-limiting headers
    for (const throttled of status429) {
      expect(throttled.headers['retry-after']).toBeDefined();
      expect(throttled.headers['x-ratelimit-remaining']).toBe('0');
      const body = JSON.parse(throttled.body);
      expect(body.code).toBe('ERR_RATE_LIMIT_EXCEEDED');
    }
  });

  // =========================================================================
  // Test 2.3: Outbox Poller High-Concurrency Lock Contention Drill
  // =========================================================================
  it('prevents duplicate message publishing when 5 concurrent outbox pollers race on 100 pending records', async () => {
    const totalOutboxRecords = 100;

    // Seed 100 outbox records in DB
    for (let i = 1; i <= totalOutboxRecords; i++) {
      await db.insertOutboxRecord({
        aggregate_type: 'payment',
        aggregate_id: `pay_${i}`,
        event_type: 'payment.authorized',
        payload: { payment_id: `pay_${i}`, amount: i * 10 },
      });
    }

    expect((await db.fetchUnprocessedOutbox(totalOutboxRecords)).length).toBe(totalOutboxRecords);

    // Create 5 separate poller workers referencing the same DB and producer
    const pollers = [
      new OutboxPoller(db, producer),
      new OutboxPoller(db, producer),
      new OutboxPoller(db, producer),
      new OutboxPoller(db, producer),
      new OutboxPoller(db, producer),
    ];

    // Race all 5 pollers simultaneously in parallel
    const pollResults = await Promise.all(
      pollers.map((p) => p.pollAndPublishBatch(totalOutboxRecords))
    );

    const totalClaimed = pollResults.reduce((a, b) => a + b, 0);
    expect(totalClaimed).toBe(totalOutboxRecords);

    // Verify all 100 records processed
    const remainingPending = await db.fetchUnprocessedOutbox(totalOutboxRecords);
    expect(remainingPending).toHaveLength(0);

    // Kafka must have received exactly 100 integration events
    const published = producer.getMessages('nexusgrid.events.integration');
    expect(published).toHaveLength(totalOutboxRecords);

    // Verify uniqueness of published events (zero duplicate publish)
    const eventIds = new Set(published.map((m) => m.headers['x-event-id']));
    expect(eventIds.size).toBe(totalOutboxRecords);
  });

  // =========================================================================
  // Test 2.4: Exact Batch Limit Boundary Stress
  // =========================================================================
  it('strictly enforces exact batch boundaries at 500 events vs 501 events and 0 events', async () => {
    const app = await buildIngestionServer({ redis, producer, validator });

    // Case A: Exact maximum allowed batch (500 events) -> Must Succeed
    const batch500: EventEnvelope[] = [];
    for (let i = 0; i < 500; i++) {
      batch500.push(createEvent('01ARZ3NDEKTSV4RRFFQ69G5FAV'));
    }

    const res500 = await app.inject({
      method: 'POST',
      url: '/v1/events:batch',
      payload: { events: batch500 },
    });
    expect(res500.statusCode).toBe(202);
    const body500 = JSON.parse(res500.body);
    expect(body500.accepted).toBe(500);

    // Case B: Boundary violation by +1 (501 events) -> Must Reject with 413
    const batch501: EventEnvelope[] = [...batch500, createEvent('01ARZ3NDEKTSV4RRFFQ69G5FAV')];
    const res501 = await app.inject({
      method: 'POST',
      url: '/v1/events:batch',
      payload: { events: batch501 },
    });
    expect(res501.statusCode).toBe(413);
    const body501 = JSON.parse(res501.body);
    expect(body501.code).toBe('ERR_BATCH_LIMIT_EXCEEDED');

    // Case C: Empty events array (0 events) -> Must Reject with 400
    const resEmpty = await app.inject({
      method: 'POST',
      url: '/v1/events:batch',
      payload: { events: [] },
    });
    expect(resEmpty.statusCode).toBe(400);
    const bodyEmpty = JSON.parse(resEmpty.body);
    expect(bodyEmpty.code).toBe('ERR_EMPTY_BATCH');
  });
});

