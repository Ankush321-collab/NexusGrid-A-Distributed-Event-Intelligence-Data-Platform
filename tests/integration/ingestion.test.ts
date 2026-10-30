import { describe, it, expect, beforeEach } from 'vitest';
import { InMemoryRedisClient } from '../../packages/database/src/index.js';
import { EventValidator } from '../../apps/ingestion-service/src/validators/event-validator.js';
import { KafkaProducer } from '../../apps/ingestion-service/src/producer/kafka-producer.js';
import { buildIngestionServer } from '../../apps/ingestion-service/src/index.js';
import type { EventEnvelope } from '../../packages/contracts/src/index.js';

describe('Phase 2 - High-Throughput Ingestion Gateway, Validation, Rate Limiting & Idempotency', () => {
  let redis: InMemoryRedisClient;
  let producer: KafkaProducer;
  let validator: EventValidator;

  beforeEach(() => {
    redis = new InMemoryRedisClient();
    producer = new KafkaProducer('nexusgrid.events.raw', 12);
    validator = new EventValidator();
  });

  const createSampleEvent = (id: string = '01ARZ3NDEKTSV4RRFFQ69G5FAV'): EventEnvelope => ({
    event_id: id,
    tenant_id: 'tenant_acme',
    project_id: 'proj_web',
    event_name: 'button.clicked',
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    properties: { button_id: 'btn_signup', page: '/pricing' },
  });

  it('accepts valid single event and routes to deterministic Kafka partition', async () => {
    const app = await buildIngestionServer({ redis, producer, validator });
    const event = createSampleEvent();

    const response = await app.inject({
      method: 'POST',
      url: '/v1/events',
      payload: event,
    });

    expect(response.statusCode).toBe(202);
    const body = JSON.parse(response.body);
    expect(body.status).toBe('ACCEPTED');
    expect(body.event_id).toBe(event.event_id);
    expect(body.correlation_id).toBeDefined();
    expect(body.received_at).toBeDefined();
    expect(typeof body.partition).toBe('number');
    expect(body.partition).toBeGreaterThanOrEqual(0);
    expect(body.partition).toBeLessThan(12);

    // Verify message landed in Kafka producer
    const published = producer.getMessages();
    expect(published).toHaveLength(1);
    expect(published[0].key).toBe('tenant_acme:proj_web');
    expect(published[0].headers['x-event-id']).toBe(event.event_id);
  });

  it('rejects invalid event envelope missing mandatory fields with 400', async () => {
    const app = await buildIngestionServer({ redis, producer, validator });
    const invalidEvent = {
      event_id: 'bad_id', // Invalid ULID format
      tenant_id: 't1',
      // missing project_id, occurred_at, etc.
    };

    const response = await app.inject({
      method: 'POST',
      url: '/v1/events',
      payload: invalidEvent,
    });

    expect(response.statusCode).toBe(400);
    const body = JSON.parse(response.body);
    expect(body.code).toBe('ERR_SCHEMA_VALIDATION_FAILED');
  });

  it('rejects event with timestamp in distant future with 422', async () => {
    const app = await buildIngestionServer({ redis, producer, validator });
    const futureEvent = createSampleEvent();
    futureEvent.occurred_at = new Date(Date.now() + 5 * 3600 * 1000).toISOString(); // 5 hours in future

    const response = await app.inject({
      method: 'POST',
      url: '/v1/events',
      payload: futureEvent,
    });

    expect(response.statusCode).toBe(422);
    const body = JSON.parse(response.body);
    expect(body.code).toBe('ERR_TIMESTAMP_IN_FUTURE');
  });

  it('rejects batch exceeding 500 events limit with 413', async () => {
    const app = await buildIngestionServer({ redis, producer, validator });
    const oversizedEvents: EventEnvelope[] = [];
    for (let i = 0; i < 501; i++) {
      oversizedEvents.push(createSampleEvent('01ARZ3NDEKTSV4RRFFQ69G5FAV'));
    }

    const response = await app.inject({
      method: 'POST',
      url: '/v1/events:batch',
      payload: { events: oversizedEvents },
    });

    expect(response.statusCode).toBe(413);
    const body = JSON.parse(response.body);
    expect(body.code).toBe('ERR_BATCH_LIMIT_EXCEEDED');
  });

  it('handles partial batch acceptance when some events are malformed', async () => {
    const app = await buildIngestionServer({ redis, producer, validator });
    const valid1 = createSampleEvent('01ARZ3NDEKTSV4RRFFQ69G5FA1');
    const valid2 = createSampleEvent('01ARZ3NDEKTSV4RRFFQ69G5FA2');
    const invalid = { event_id: 'not_a_ulid' };

    const response = await app.inject({
      method: 'POST',
      url: '/v1/events:batch',
      payload: { events: [valid1, invalid, valid2] },
    });

    expect(response.statusCode).toBe(202);
    const body = JSON.parse(response.body);
    expect(body.status).toBe('PARTIAL');
    expect(body.total).toBe(3);
    expect(body.accepted).toBe(2);
    expect(body.rejected).toBe(1);
    expect(body.failures).toHaveLength(1);
    expect(body.failures[0].index).toBe(1);

    // Only 2 valid events published to Kafka
    expect(producer.getMessages()).toHaveLength(2);
  });

  it('guarantees idempotency: repeated request with same Idempotency-Key returns cached response', async () => {
    const app = await buildIngestionServer({ redis, producer, validator });
    const event = createSampleEvent();
    const idemKey = 'idem_checkout_unique_123';

    // First Request
    const res1 = await app.inject({
      method: 'POST',
      url: '/v1/events',
      headers: { 'Idempotency-Key': idemKey, 'X-Tenant-Id': 'tenant_acme' },
      payload: event,
    });

    expect(res1.statusCode).toBe(202);
    expect(res1.headers['x-cache-lookup']).toBeUndefined();
    const body1 = JSON.parse(res1.body);

    // Second Identical Request with same Idempotency-Key
    const res2 = await app.inject({
      method: 'POST',
      url: '/v1/events',
      headers: { 'Idempotency-Key': idemKey, 'X-Tenant-Id': 'tenant_acme' },
      payload: event,
    });

    expect(res2.statusCode).toBe(202);
    expect(res2.headers['x-cache-lookup']).toBe('HIT');
    const body2 = JSON.parse(res2.body);
    expect(body2.event_id).toBe(body1.event_id);
    expect(body2.correlation_id).toBe(body1.correlation_id);

    // Ensure event was only published ONCE to Kafka!
    expect(producer.getMessages()).toHaveLength(1);
  });

  it('enforces token-bucket rate limiting and returns 429 with Retry-After header', async () => {
    // Configure tight bucket: 3 max tokens, 0 refill
    const app = await buildIngestionServer({
      redis,
      producer,
      validator,
      maxTokens: 3,
      refillRate: 0.1,
    });

    const tenantHeaders = { 'X-Tenant-Id': 'tenant_limited' };

    // Request 1, 2, 3 should succeed
    for (let i = 0; i < 3; i++) {
      const res = await app.inject({
        method: 'POST',
        url: '/v1/events',
        headers: tenantHeaders,
        payload: createSampleEvent(),
      });
      expect(res.statusCode).toBe(202);
    }

    // Request 4 must be rate-limited!
    const limitedRes = await app.inject({
      method: 'POST',
      url: '/v1/events',
      headers: tenantHeaders,
      payload: createSampleEvent(),
    });

    expect(limitedRes.statusCode).toBe(429);
    expect(limitedRes.headers['retry-after']).toBeDefined();
    const limitedBody = JSON.parse(limitedRes.body);
    expect(limitedBody.code).toBe('ERR_RATE_LIMIT_EXCEEDED');
  });
});

