import { describe, it, expect, beforeEach } from 'vitest';
import { InMemoryPostgresAdapter, InMemoryRedisClient } from '../../packages/database/src/index.js';
import { AuthService } from '../../apps/auth-service/src/services/auth.service.js';
import { KafkaProducer } from '../../apps/ingestion-service/src/producer/kafka-producer.js';
import { EventValidator } from '../../apps/ingestion-service/src/validators/event-validator.js';
import { buildIngestionServer } from '../../apps/ingestion-service/src/index.js';
import { StreamEventProcessor } from '../../workers/event-processor/src/consumer/kafka-consumer.js';
import { OutboxPoller } from '../../workers/outbox-publisher/src/poller/outbox-poller.js';
import { DLQReplayTool } from '../../scripts/replay-dlq.js';
import type { EventEnvelope, KafkaEventMessage } from '../../packages/contracts/src/index.js';

describe('NexusGrid Complete End-to-End System Test (Phases 0 through 3)', () => {
  let db: InMemoryPostgresAdapter;
  let redis: InMemoryRedisClient;
  let producer: KafkaProducer;
  let authService: AuthService;
  let validator: EventValidator;
  let poller: OutboxPoller;

  beforeEach(() => {
    db = new InMemoryPostgresAdapter();
    redis = new InMemoryRedisClient();
    producer = new KafkaProducer('nexusgrid.events.raw', 12);
    authService = new AuthService(db);
    validator = new EventValidator();
    poller = new OutboxPoller(db, producer);
  });

  it('executes full multi-tenant lifecycle from tenant creation to ingestion, Kafka streaming, DLQ, and outbox', async () => {
    // ==========================================
    // STEP 1: Phase 1 — Provision Tenant, Project, and API Key
    // ==========================================
    const tenant = await authService.createTenant('tenant_hyperion', 'Hyperion Dynamics');
    expect(tenant.id).toBe('tenant_hyperion');

    const project = await authService.createProject('tenant_hyperion', 'proj_payments', 'Payments Ingestion', 'prod');
    expect(project.id).toBe('proj_payments');

    const keyResult = await authService.createApiKey({
      tenantId: 'tenant_hyperion',
      projectId: 'proj_payments',
      name: 'Hyperion Ingestion Key',
      scopes: ['events:write'],
    });

    expect(keyResult.plaintextKey.startsWith('ng_prod_')).toBe(true);

    // Verify Outbox populated with metadata integration events
    const pendingOutbox = await db.fetchUnprocessedOutbox();
    expect(pendingOutbox.length).toBeGreaterThan(0);

    // Poller synchronizes metadata outbox to Kafka
    const outboxSynced = await poller.pollAndPublishBatch();
    expect(outboxSynced).toBeGreaterThan(0);
    expect(await db.fetchUnprocessedOutbox()).toHaveLength(0);

    // ==========================================
    // STEP 2: Phase 2 — HTTP Ingestion via Fastify Gateway
    // ==========================================
    const ingestionApp = await buildIngestionServer({ redis, producer, validator });

    const clientEvent: EventEnvelope = {
      event_id: '01HZY000000000000000000001',
      tenant_id: 'tenant_hyperion',
      project_id: 'proj_payments',
      event_name: 'payment.settled',
      occurred_at: new Date().toISOString(),
      schema_version: 1,
      idempotency_key: 'idem_settle_hyp_9901',
      properties: {
        transaction_id: 'tx_788912',
        amount_cents: 125000,
        currency: 'USD',
      },
    };

    // 2a. Ingest with authenticated context
    const ingestRes = await ingestionApp.inject({
      method: 'POST',
      url: '/v1/events',
      headers: {
        'X-Tenant-Id': 'tenant_hyperion',
        'Idempotency-Key': 'idem_settle_hyp_9901',
      },
      payload: clientEvent,
    });

    expect(ingestRes.statusCode).toBe(202);
    const ingestBody = JSON.parse(ingestRes.body);
    expect(ingestBody.status).toBe('ACCEPTED');
    expect(ingestBody.event_id).toBe(clientEvent.event_id);
    expect(ingestBody.correlation_id).toBeDefined();

    // 2b. Repeat identical request -> Should hit idempotency cache
    const repeatRes = await ingestionApp.inject({
      method: 'POST',
      url: '/v1/events',
      headers: {
        'X-Tenant-Id': 'tenant_hyperion',
        'Idempotency-Key': 'idem_settle_hyp_9901',
      },
      payload: clientEvent,
    });
    expect(repeatRes.statusCode).toBe(202);
    expect(repeatRes.headers['x-cache-lookup']).toBe('HIT');

    // Kafka has received only 1 primary event (deduplicated!)
    expect(producer.getMessages('nexusgrid.events.raw')).toHaveLength(1);

    // ==========================================
    // STEP 3: Phase 3 — Stream Worker Processing
    // ==========================================
    const processedEvents: EventEnvelope[] = [];
    const worker = new StreamEventProcessor(producer, async (event) => {
      processedEvents.push(event);
    });

    await worker.processTopic('nexusgrid.events.raw');
    expect(processedEvents).toHaveLength(1);
    expect(processedEvents[0].event_id).toBe(clientEvent.event_id);
    expect(worker.stats.processedCount).toBe(1);

    // ==========================================
    // STEP 4: Phase 3 — Poison Message Injection & DLQ Isolation
    // ==========================================
    const poisonPill: KafkaEventMessage = {
      key: 'tenant_hyperion:proj_payments',
      value: '{ CORRUPT_BYTE_STREAM: true', // Malformed JSON
      headers: {
        'x-event-id': 'poison_001',
        'x-tenant-id': 'tenant_hyperion',
        'x-project-id': 'proj_payments',
        'x-schema-version': '1',
        'x-correlation-id': 'cor_poison_drill',
        'x-retry-count': '0',
        'x-timestamp': new Date().toISOString(),
      },
    };
    producer.publishedMessages.get('nexusgrid.events.raw')!.push(poisonPill);

    // Worker continues processing
    await worker.processTopic('nexusgrid.events.raw');
    expect(worker.stats.dlqCount).toBe(1);

    // Confirm message is in DLQ
    const dlq = producer.getMessages('nexusgrid.events.dlq');
    expect(dlq).toHaveLength(1);
    expect(dlq[0].headers['x-error-reason']).toContain('Non-retriable poison');

    // ==========================================
    // STEP 5: Phase 3 — Operator DLQ Replay
    // ==========================================
    // Replace the corrupt body with fixed payload
    const fixedEvent: EventEnvelope = {
      ...clientEvent,
      event_id: '01HZY000000000000000000002',
    };
    dlq[0].value = JSON.stringify(fixedEvent);

    const replayTool = new DLQReplayTool(producer);
    const replayReport = await replayTool.runReplay({ dryRun: false, tenantId: 'tenant_hyperion' });
    expect(replayReport.replayed).toBe(1);
    expect(producer.getMessages('nexusgrid.events.dlq')).toHaveLength(0);

    // Worker picks up replayed event
    await worker.processTopic('nexusgrid.events.raw');
    expect(processedEvents).toHaveLength(2);

    // ==========================================
    // STEP 6: Phase 1 — Transactional Audit Log Verification
    // ==========================================
    const auditLogs = await db.listAuditLogs('tenant_hyperion');
    expect(auditLogs.items.length).toBeGreaterThanOrEqual(2);
    const actions = auditLogs.items.map((i) => i.action);
    expect(actions).toContain('API_KEY_CREATED');
    expect(actions).toContain('PROJECT_CREATED');
  });
});

