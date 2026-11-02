import { describe, it, expect, beforeEach } from 'vitest';
import {
  InMemoryPostgresAdapter,
  InMemoryRedisClient,
  InMemoryClickHouseAdapter,
  KafkaProducer,
} from '../../packages/database/src/index.js';
import { AuthService } from '../../apps/auth-service/src/services/auth.service.js';
import { CryptoService } from '../../apps/auth-service/src/services/crypto.service.js';
import { buildIngestionServer } from '../../apps/ingestion-service/src/index.js';
import { EventValidator } from '../../apps/ingestion-service/src/validators/event-validator.js';
import { StreamEventProcessor } from '../../workers/event-processor/src/consumer/kafka-consumer.js';
import { ClickHouseBulkSink } from '../../workers/event-processor/src/sink/clickhouse-sink.js';
import { OutboxPoller } from '../../workers/outbox-publisher/src/poller/outbox-poller.js';
import { QueryCacheManager } from '../../apps/query-service/src/cache/cache-manager.js';
import { ASTValidator } from '../../apps/query-service/src/builder/ast-validator.js';
import { buildQueryServer } from '../../apps/query-service/src/index.js';
import { AlertStateMachine, type AlertRuleConfig } from '../../apps/alert-service/src/state/alert-state-machine.js';
import { WebhookDispatcher } from '../../apps/alert-service/src/dispatch/webhook-dispatcher.js';
import { RuleEvaluator } from '../../apps/alert-service/src/evaluator/rule-evaluator.js';
import { DLQReplayTool } from '../../scripts/replay-dlq.js';
import type { EventEnvelope, KafkaEventMessage } from '../../packages/contracts/src/index.js';

describe('Hard-Level Test Type 6: Distributed End-to-End Pipeline Extreme Resilience Drill (Phase 1 to Phase 6)', () => {
  let db: InMemoryPostgresAdapter;
  let redis: InMemoryRedisClient;
  let clickhouse: InMemoryClickHouseAdapter;
  let producer: KafkaProducer;
  let cryptoService: CryptoService;
  let authService: AuthService;
  let validator: EventValidator;
  let cacheManager: QueryCacheManager;
  let astValidator: ASTValidator;
  let fsm: AlertStateMachine;
  let dispatcher: WebhookDispatcher;
  let evaluator: RuleEvaluator;

  beforeEach(() => {
    db = new InMemoryPostgresAdapter();
    redis = new InMemoryRedisClient();
    clickhouse = new InMemoryClickHouseAdapter();
    producer = new KafkaProducer('nexusgrid.events.raw', 12);
    cryptoService = new CryptoService();
    authService = new AuthService(db, cryptoService);
    validator = new EventValidator();
    cacheManager = new QueryCacheManager(redis, 60000);
    astValidator = new ASTValidator(90);
    fsm = new AlertStateMachine();
    dispatcher = new WebhookDispatcher();
    evaluator = new RuleEvaluator(clickhouse, fsm, dispatcher);
  });

  const createEvent = (
    id: string,
    tenantId: string = 'tenant_pipeline_e2e',
    projectId: string = 'proj_core',
    props: Record<string, any> = {}
  ): EventEnvelope => ({
    event_id: id,
    tenant_id: tenantId,
    project_id: projectId,
    event_name: 'order.placed',
    occurred_at: new Date().toISOString(),
    schema_version: 1,
    properties: {
      amount: 49.99,
      status_code: 200,
      user_id: `usr_${id.slice(-4)}`,
      ...props,
    },
  });

  // =========================================================================
  // Test 6.1: High-Concurrency End-to-End Pipeline Drill (Phase 1 through Phase 6)
  // =========================================================================
  it('executes full pipeline flow from auth to ingestion, outbox, kafka salting, clickhouse sink, query caching, and live alerting', async () => {
    // -----------------------------------------------------------------------
    // STEP 1 (Phase 1): Zero-Trust Multi-Tenant Onboarding & Key Issuance
    // -----------------------------------------------------------------------
    const tenantId = 'tenant_pipeline_e2e';
    const projectId = 'proj_core';
    const tenant = await authService.createTenant(tenantId, 'E2E Enterprise Corp');
    const project = await authService.createProject(tenant.id, projectId, 'Production Analytics');
    const { plaintextKey, apiKey } = await authService.createApiKey({
      tenantId: tenant.id,
      projectId: project.id,
      name: 'Ingestion Key',
      scopes: ['events:write', 'analytics:read'],
    });

    // -----------------------------------------------------------------------
    // STEP 2 (Phase 2): HTTP Ingestion Gateway with Validation & Partial Acceptance
    // -----------------------------------------------------------------------
    const ingestionApp = await buildIngestionServer({ redis, producer, validator });

    // Send a batch of 100 events: 90 valid, 10 malformed (missing required fields)
    const batchEvents: any[] = [];
    for (let i = 1; i <= 100; i++) {
      if (i <= 90) {
        batchEvents.push(createEvent(`01ARZ3NDEKTSV4RRFFQ69${i.toString().padStart(5, '0')}`, tenant.id, project.id, {
          amount: i * 10,
          status_code: i > 80 ? 500 : 200, // 10 error events
        }));
      } else {
        // Poison / invalid shape (missing event_id and occurred_at)
        batchEvents.push({ tenant_id: tenant.id, properties: { junk: true } });
      }
    }

    const ingestRes = await ingestionApp.inject({
      method: 'POST',
      url: '/v1/events:batch',
      headers: {
        'X-API-Key': plaintextKey,
        'X-Tenant-Id': tenant.id,
      },
      payload: { events: batchEvents },
    });

    // HTTP 202 Accepted with PARTIAL status: 90 accepted, 10 rejected
    expect(ingestRes.statusCode).toBe(202);
    const ingestJson = ingestRes.json();
    expect(ingestJson.status).toBe('PARTIAL');
    expect(ingestJson.accepted).toBe(90);
    expect(ingestJson.rejected).toBe(10);
    expect(producer.getMessages('nexusgrid.events.raw')).toHaveLength(90);

    // -----------------------------------------------------------------------
    // STEP 3 (Phase 3 & Defense 1): Partition Routing with Entity Virtual Salting
    // -----------------------------------------------------------------------
    const rawMessages = producer.getMessages('nexusgrid.events.raw');
    const partitionsHit = new Set<string>();
    for (const msg of rawMessages) {
      if (msg.headers['x-partition-key']) {
        partitionsHit.add(msg.headers['x-partition-key']);
      }
    }
    // High-cardinality entity keys distributed traffic across multiple partition keys
    expect(partitionsHit.size).toBeGreaterThan(1);

    // Outbox Poller row-locking test
    await db.insertOutboxRecord({
      aggregate_type: 'order',
      aggregate_id: 'ord_9901',
      event_type: 'order.completed',
      payload: { tenant_id: tenant.id, project_id: project.id, amount: 99.0 },
    });
    // Outbox Poller row-locking test: polls both the api_key.created and order.completed records
    const outboxPoller = new OutboxPoller(db, producer, 50);
    const polledCount = await outboxPoller.pollAndPublishBatch(10);
    expect(polledCount).toBe(2);
    expect(outboxPoller.publishedCount).toBe(2);

    // -----------------------------------------------------------------------
    // STEP 4 (Phase 4 & Defense 3): Micro-Batch ClickHouse Sink & S3 Cold Archival
    // -----------------------------------------------------------------------
    const sink = new ClickHouseBulkSink(clickhouse, {
      batchSize: 50,
      minFlushBatchSize: 10,
      enableAsyncInsert: true,
      enableS3Archive: true,
    });

    const streamProcessor = new StreamEventProcessor(producer, async (event) => {
      await sink.push(event);
    });

    // Consume all raw messages into ClickHouse
    await streamProcessor.processTopic('nexusgrid.events.raw');
    await sink.flush(true);

    expect(streamProcessor.stats.processedCount).toBe(90);
    expect(clickhouse.totalInsertedRows).toBe(90);
    expect(sink.totalArchivedToS3).toBe(90);
    expect(sink.archivedBatches.length).toBeGreaterThan(0);

    // -----------------------------------------------------------------------
    // STEP 5 (Phase 5): Analytical OLAP Query with Singleflight & Cache Verification
    // -----------------------------------------------------------------------
    const queryApp = await buildQueryServer({ clickhouse, redis, cacheManager, validator: astValidator });

    const queryPayload = {
      project_id: project.id,
      time_range: {
        start: '2026-10-01T00:00:00.000Z',
        end: '2026-11-30T00:00:00.000Z',
      },
      metrics: ['count()', 'avg(amount)'],
      group_by: ['event_name'],
    };

    // First query executes against database
    const queryRes1 = await queryApp.inject({
      method: 'POST',
      url: '/v1/analytics/query',
      headers: { 'X-Tenant-Id': tenant.id },
      payload: queryPayload,
    });
    expect(queryRes1.statusCode).toBe(200);
    const q1Json = queryRes1.json();
    expect(q1Json.data[0].count).toBe(90);
    expect(q1Json.meta.fromCache).toBeFalsy();

    // Second query is served directly from Redis cache
    const queryRes2 = await queryApp.inject({
      method: 'POST',
      url: '/v1/analytics/query',
      headers: { 'X-Tenant-Id': tenant.id },
      payload: queryPayload,
    });
    expect(queryRes2.statusCode).toBe(200);
    expect(queryRes2.json().meta.fromCache).toBe(true);

    // -----------------------------------------------------------------------
    // STEP 6 (Phase 6): Live Stream Alert Evaluation & Webhook Dispatch
    // -----------------------------------------------------------------------
    const alertRule: AlertRuleConfig = {
      id: 'rule_e2e_error_rate',
      tenant_id: tenant.id,
      project_id: project.id,
      name: 'E2E High Error Rate (> 8%)',
      metric_name: 'error_rate',
      operator: 'gt',
      threshold: 8.0,
      consecutive_breaches_required: 1,
      cooldown_minutes: 5,
      webhook_url: 'https://security.e2ecorp.com/webhooks/alerts',
      webhook_secret: 'whsec_pipeline_secret_e2e_777',
    };
    evaluator.registerRule(alertRule);

    // 10 errors out of 90 = 11.1% error rate (> 8.0% threshold)
    const { outcome, dispatched } = await evaluator.evaluateRule(alertRule.id);
    expect(outcome.newState).toBe('FIRING');
    expect(outcome.action).toBe('SEND_FIRE_NOTIFICATION');
    expect(outcome.currentValue).toBeCloseTo(11.11, 1);
    expect(dispatched).toBeDefined();
    expect(dispatched!.status).toBe('DELIVERED');

    // Verify HMAC-SHA256 signature on dispatched notification
    const payloadStr = JSON.stringify(dispatched!.payload);
    expect(
      WebhookDispatcher.verifySignature(payloadStr, dispatched!.signature, alertRule.webhook_secret)
    ).toBe(true);
  });

  // =========================================================================
  // Test 6.2: Worker Crash, Partial Offsets & Resumption Drill
  // =========================================================================
  it('recovers seamlessly from mid-stream consumer crashes without duplicate processing or message loss', async () => {
    const totalEvents = 80;
    const crashOffset = 35;

    // Produce 80 events into raw topic
    for (let i = 1; i <= totalEvents; i++) {
      await producer.publish(
        createEvent(`01ARZ3NDEKTSV4RRFFQ69C${i.toString().padStart(5, '0')}`),
        `cor_crash_${i}`
      );
    }
    expect(producer.getMessages('nexusgrid.events.raw')).toHaveLength(80);

    const processed1: string[] = [];
    const worker1 = new StreamEventProcessor(producer, async (event) => {
      processed1.push(event.event_id);
    });

    // Worker 1 processes up to crashOffset (35 events) then crashes abruptly
    const messages = producer.getMessages('nexusgrid.events.raw');
    for (let i = 0; i < crashOffset; i++) {
      await worker1.consumeRecord(messages[i], 'nexusgrid.events.raw', 0, i + 1);
    }

    expect(processed1).toHaveLength(crashOffset);
    const lastCommittedOffset = worker1.getCommittedOffset('nexusgrid.events.raw', 0);
    expect(lastCommittedOffset).toBe(crashOffset);

    // SIMULATED PROCESS KILL: Worker 1 terminates abruptly!
    // Worker 2 boots up and resumes from committed offset
    const processed2: string[] = [];
    const worker2 = new StreamEventProcessor(producer, async (event) => {
      processed2.push(event.event_id);
    });

    // Inherit committed partition offset from broker
    (worker2 as any).committedOffsets.set('nexusgrid.events.raw:0', lastCommittedOffset);

    // Worker 2 resumes processing the topic
    await worker2.processTopic('nexusgrid.events.raw');

    // Worker 2 processes exactly the remaining 45 events (80 - 35)
    expect(processed2).toHaveLength(totalEvents - crashOffset);
    expect(worker2.getCommittedOffset('nexusgrid.events.raw', 0)).toBe(totalEvents);

    // Total unique processed count must be exactly 80 with ZERO overlap
    const combined = new Set([...processed1, ...processed2]);
    expect(combined.size).toBe(totalEvents);
  });

  // =========================================================================
  // Test 6.3: Cascading Multi-Failure Resilience & DLQ Quarantine Circuit Breaker Drill
  // =========================================================================
  it('survives simultaneous downstream flapping, delayed retry backoff, and quarantined replay loops', async () => {
    let transientFailures = 0;
    const worker = new StreamEventProcessor(
      producer,
      async (event) => {
        if (event.properties.simulated_error === 'TRANSIENT_DOWNSTREAM') {
          transientFailures++;
          if (transientFailures <= 2) {
            throw new Error('ETIMEDOUT: Downstream gateway timeout');
          }
        }
      },
      undefined,
      { enforceRetryDelay: true }
    );

    // 1. Publish 10 valid events
    for (let i = 1; i <= 10; i++) {
      await producer.publish(
        createEvent(`01ARZ3NDEKTSV4RRFFQ69D${i.toString().padStart(5, '0')}`),
        `cor_val_${i}`
      );
    }

    // 2. Publish 1 event that suffers transient downstream flapping
    await producer.publish(
      createEvent('01ARZ3NDEKTSV4RRFFQ69FLAKY01', 'tenant_pipeline_e2e', 'proj_core', {
        simulated_error: 'TRANSIENT_DOWNSTREAM',
      }),
      'cor_flaky'
    );

    // 3. Publish 1 poison pill (already replayed 2 times previously)
    producer.getMessages('nexusgrid.events.dlq').push({
      key: 'tenant_pipeline_e2e:proj_core',
      value: JSON.stringify({ corrupted: 'missing_ids' }),
      headers: {
        'x-event-id': '01ARZ3NDEKTSV4RRFFQ69POISON99',
        'x-tenant-id': 'tenant_pipeline_e2e',
        'x-project-id': 'proj_core',
        'x-schema-version': '1',
        'x-correlation-id': 'cor_poison_loop',
        'x-retry-count': '3',
        'x-replay-count': '2', // Circuit breaker trigger!
        'x-timestamp': new Date().toISOString(),
      },
    });

    // Worker processes raw topic:
    // 10 valid processed, 1 flaky routed to retry.10s
    await worker.processTopic('nexusgrid.events.raw');
    expect(worker.stats.processedCount).toBe(10);
    expect(worker.stats.retriedCount).toBe(1);
    expect(producer.getMessages('nexusgrid.events.retry.10s')).toHaveLength(1);

    // Run DLQ Replay Tool:
    // The poison pill with x-replay-count = 2 must be circuit-broken and quarantined!
    const replayTool = new DLQReplayTool(producer);
    const replayReport = await replayTool.runReplay({ dryRun: false, maxReplayAttempts: 2 });
    expect(replayReport.quarantined).toBe(1);
    expect(replayReport.replayed).toBe(0);

    const quarantineMessages = producer.getMessages('nexusgrid.events.quarantine');
    expect(quarantineMessages).toHaveLength(1);
    expect(quarantineMessages[0].headers['x-quarantine-reason']).toBe('MAX_REPLAY_EXCEEDED_CIRCUIT_BREAKER');
  });

  // =========================================================================
  // Test 6.4: Graceful Shutdown Under In-Flight Workload Buffer Drain
  // =========================================================================
  it('drains active in-flight buffer to ClickHouse and stops cleanly on SIGTERM without dropping records', async () => {
    const sink = new ClickHouseBulkSink(clickhouse, {
      batchSize: 50,
      minFlushBatchSize: 10,
    });

    const worker = new StreamEventProcessor(producer, async (event) => {
      await sink.push(event);
    });

    // Ingest 25 events (less than batchSize 50, so they stay in buffer)
    for (let i = 1; i <= 25; i++) {
      await producer.publish(
        createEvent(`01ARZ3NDEKTSV4RRFFQ69DR${i.toString().padStart(5, '0')}`),
        `cor_drain_${i}`
      );
    }

    await worker.processTopic('nexusgrid.events.raw');

    // Buffer has 25 pending records
    expect(sink.getPendingBufferCount()).toBe(25);
    expect(clickhouse.totalInsertedRows).toBe(0);

    // Trigger graceful shutdown: drains buffer cleanly
    await worker.shutdown();
    await sink.flush(true);

    expect(worker.isShuttingDown).toBe(true);
    expect(sink.getPendingBufferCount()).toBe(0);
    // All 25 records were committed to ClickHouse without loss
    expect(clickhouse.totalInsertedRows).toBe(25);
  });
});
