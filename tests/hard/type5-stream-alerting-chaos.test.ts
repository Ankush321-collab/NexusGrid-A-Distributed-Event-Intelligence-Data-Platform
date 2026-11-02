import { describe, it, expect, beforeEach } from 'vitest';
import { InMemoryClickHouseAdapter } from '../../packages/database/src/index.js';
import { AlertStateMachine, type AlertRuleConfig, type EvaluationOutcome } from '../../apps/alert-service/src/state/alert-state-machine.js';
import { WebhookDispatcher } from '../../apps/alert-service/src/dispatch/webhook-dispatcher.js';
import { RuleEvaluator } from '../../apps/alert-service/src/evaluator/rule-evaluator.js';
import type { EventEnvelope } from '../../packages/contracts/src/index.js';

describe('Hard-Level Test Type 5: High-Frequency Stream Anomaly Detection, Flapping Resilience & Webhook Disaster Recovery', () => {
  let clickhouse: InMemoryClickHouseAdapter;
  let fsm: AlertStateMachine;
  let dispatcher: WebhookDispatcher;
  let evaluator: RuleEvaluator;

  beforeEach(() => {
    clickhouse = new InMemoryClickHouseAdapter();
    fsm = new AlertStateMachine();
    dispatcher = new WebhookDispatcher();
    evaluator = new RuleEvaluator(clickhouse, fsm, dispatcher);
  });

  const createEvent = (
    id: string,
    occurredAt: string,
    props: Record<string, any> = {},
    tenant: string = 'tenant_alert_chaos',
    project: string = 'proj_chaos_mon'
  ): EventEnvelope => ({
    event_id: id,
    tenant_id: tenant,
    project_id: project,
    event_name: 'api.request',
    occurred_at: occurredAt,
    received_at: new Date().toISOString(),
    schema_version: 1,
    properties: {
      status_code: 200,
      latency_ms: 25.0,
      ...props,
    },
  });

  // =========================================================================
  // Test 5.1: High-Frequency Stream Anomaly Flood & Sliding-Window Accuracy
  // =========================================================================
  it('accurately evaluates sliding-window error rates and p95 latencies across a flood of 1,000 stream events', async () => {
    const baseTime = Date.now();
    const windowMinutes = 5;
    const rule: AlertRuleConfig = {
      id: 'rule_stream_flood_p95',
      tenant_id: 'tenant_alert_chaos',
      project_id: 'proj_chaos_mon',
      name: 'High P95 Latency (> 100ms)',
      metric_name: 'p95_latency_ms',
      operator: 'gt',
      threshold: 100.0,
      window_minutes: windowMinutes,
      consecutive_breaches_required: 1,
      cooldown_minutes: 5,
      webhook_url: 'https://pager.corp.internal/hooks',
      webhook_secret: 'sec_hard_level_hmac_999',
    };
    evaluator.registerRule(rule);

    // Ingest 1,000 events: 900 fast (20ms), 100 slow (250ms)
    const rawEvents: EventEnvelope[] = [];
    for (let i = 1; i <= 1000; i++) {
      const isSlow = i > 900;
      const eventTime = new Date(baseTime - (i % 240) * 1000).toISOString(); // distributed across last 4 minutes
      rawEvents.push(
        createEvent(`01ARZ3NDEKTSV4RRFFQ69A${i.toString().padStart(5, '0')}`, eventTime, {
          latency_ms: isSlow ? 250 : 20,
          status_code: isSlow ? 500 : 200,
        })
      );
    }

    const records = rawEvents.map((e) => InMemoryClickHouseAdapter.envelopeToClickHouseRecord(e));
    await clickhouse.insertBatch(records);

    // Compute metric value at baseTime
    const computedP95 = await evaluator.computeMetricValue(rule, baseTime);

    // Because 10% of values are 250ms (> 5%), the 95th percentile must be 250ms
    expect(computedP95).toBe(250);

    // Evaluate rule: 250ms > 100ms threshold -> triggers alert
    const { outcome } = await evaluator.evaluateRule(rule.id, baseTime);
    expect(outcome).toBeDefined();
    expect(outcome.action).toBe('SEND_FIRE_NOTIFICATION');
    expect(outcome.newState).toBe('FIRING');
    expect(dispatcher.dispatched).toHaveLength(1);
    expect(dispatcher.dispatched[0].payload.event).toBe('ALERT_FIRING');
    expect(dispatcher.dispatched[0].payload.current_value).toBe(250);
  });

  // =========================================================================
  // Test 5.2: Extreme Flapping Oscillations & Hysteresis State Machine Suppression
  // =========================================================================
  it('suppresses false-alarms during 40 rapid flapping oscillation cycles and enforces cooldown silence', () => {
    let now = Date.now();
    const flappingRule: AlertRuleConfig = {
      id: 'rule_flapping_hysteresis',
      tenant_id: 'tenant_alert_chaos',
      project_id: 'proj_chaos_mon',
      name: 'Flapping Error Rate (> 10%)',
      metric_name: 'error_rate',
      operator: 'gt',
      threshold: 10.0,
      consecutive_breaches_required: 3, // Requires 3 CONSECUTIVE breaches to trigger
      cooldown_minutes: 15, // 15-minute silence window
      webhook_url: 'https://pager.corp.internal/flapping',
      webhook_secret: 'sec_flapping_secret_001',
    };

    // Phase A: 20 rapid single-spike blips (12% -> 2% -> 15% -> 1% -> ...)
    // With consecutive_breaches_required = 3, single spikes must never trigger a page!
    for (let cycle = 1; cycle <= 20; cycle++) {
      const isSpike = cycle % 2 === 1;
      const errorRate = isSpike ? 15.0 : 2.0;
      const outcome = fsm.evaluate(flappingRule, errorRate, now);
      now += 30000; // +30s per check

      // Zero false alarms dispatched!
      expect(outcome.action).toBe('NOOP');
      if (isSpike) {
        expect(outcome.newState).toBe('PENDING');
      } else {
        expect(outcome.newState).toBe('OK');
      }
    }

    // Phase B: Sustained degradation (3 consecutive breaches: 14%, 16%, 18%)
    const b1 = fsm.evaluate(flappingRule, 14.0, now);
    expect(b1.newState).toBe('PENDING');
    expect(b1.action).toBe('NOOP');
    now += 60000;

    const b2 = fsm.evaluate(flappingRule, 16.0, now);
    expect(b2.newState).toBe('PENDING');
    expect(b2.action).toBe('NOOP');
    now += 60000;

    // 3rd consecutive breach: strictly transitions to FIRING and sends fire notification!
    const b3 = fsm.evaluate(flappingRule, 18.0, now);
    expect(b3.newState).toBe('FIRING');
    expect(b3.action).toBe('SEND_FIRE_NOTIFICATION');
    now += 60000;

    // Phase C: Cooldown suppression (Still breaching, but within 15 min cooldown)
    for (let min = 1; min <= 10; min++) {
      const duringCooldown = fsm.evaluate(flappingRule, 20.0, now);
      expect(duringCooldown.newState).toBe('FIRING');
      expect(duringCooldown.action).toBe('SUPPRESSED_BY_COOLDOWN');
      now += 60000;
    }

    // Phase D: Sustained recovery (Error rate drops to 0.5%)
    const recovery = fsm.evaluate(flappingRule, 0.5, now);
    expect(recovery.newState).toBe('OK');
    expect(recovery.action).toBe('SEND_RESOLVE_NOTIFICATION');
  });

  // =========================================================================
  // Test 5.3: Webhook Downstream Disaster Recovery & Cryptographic HMAC Verification
  // =========================================================================
  it('delivers incident notification across failing webhook endpoints with valid HMAC signatures', async () => {
    const rule: AlertRuleConfig = {
      id: 'rule_webhook_drill',
      tenant_id: 'tenant_alert_chaos',
      project_id: 'proj_chaos_mon',
      name: 'Critical Service Failure',
      metric_name: 'error_rate',
      operator: 'gt',
      threshold: 5.0,
      consecutive_breaches_required: 1,
      cooldown_minutes: 5,
      webhook_url: 'https://api.partner.com/alert-webhook',
      webhook_secret: 'super_secret_hmac_signing_key_42',
    };

    const outcome: EvaluationOutcome = {
      ruleId: rule.id,
      tenantId: rule.tenant_id,
      previousState: 'PENDING',
      newState: 'FIRING',
      action: 'SEND_FIRE_NOTIFICATION',
      currentValue: 35.5,
      threshold: 5.0,
      isBreached: true,
      dedupeKey: `alert:${rule.id}:${Date.now()}`,
    };

    // Attempt 1: Simulated transient network drop
    const failedRecord = await dispatcher.dispatch(rule, outcome, true);
    expect(failedRecord.status).toBe('FAILED');
    expect(failedRecord.url).toBe(rule.webhook_url);

    // Attempt 2: Downstream endpoint recovers -> delivered successfully
    const successRecord = await dispatcher.dispatch(rule, outcome, false);
    expect(successRecord.status).toBe('DELIVERED');

    // Cryptographic HMAC Verification:
    // Receiver validates that the signature matches HMAC-SHA256(payload, secret)
    const rawPayloadJson = JSON.stringify(successRecord.payload);
    const isValidSignature = WebhookDispatcher.verifySignature(
      rawPayloadJson,
      successRecord.signature,
      rule.webhook_secret
    );
    expect(isValidSignature).toBe(true);

    // Tainted payload fails verification
    const tamperedPayloadJson = JSON.stringify({ ...successRecord.payload, current_value: 0 });
    const isTamperedValid = WebhookDispatcher.verifySignature(
      tamperedPayloadJson,
      successRecord.signature,
      rule.webhook_secret
    );
    expect(isTamperedValid).toBe(false);
  });

  // =========================================================================
  // Test 5.4: Multi-Tenant Rule Cross-Talk & Concurrent Isolation Drill
  // =========================================================================
  it('evaluates 20 tenant alert rules concurrently without cross-tenant bleed or false positives', async () => {
    const tenantCount = 20;
    const rules: AlertRuleConfig[] = [];

    // Register 20 rules for 20 independent tenants
    for (let t = 1; t <= tenantCount; t++) {
      const tenantId = `tenant_concurrent_${t}`;
      const r: AlertRuleConfig = {
        id: `rule_tenant_${t}`,
        tenant_id: tenantId,
        project_id: `proj_${t}`,
        name: `Alert for ${tenantId}`,
        metric_name: 'error_rate',
        operator: 'gt',
        threshold: 10.0,
        consecutive_breaches_required: 1,
        cooldown_minutes: 5,
        webhook_url: `https://webhook.tenant${t}.com/alerts`,
        webhook_secret: `secret_tenant_${t}`,
      };
      evaluator.registerRule(r);
      rules.push(r);
    }

    // Seed events:
    // Only Tenant 7 and Tenant 15 have high error rates (80% 500s).
    // All other 18 tenants have completely clean 200s (0% error rate).
    const events: EventEnvelope[] = [];
    const nowIso = new Date().toISOString();

    for (let t = 1; t <= tenantCount; t++) {
      const tenantId = `tenant_concurrent_${t}`;
      const isFailing = t === 7 || t === 15;
      for (let i = 1; i <= 20; i++) {
        events.push(
          createEvent(
            `01ARZ3NDEKTSV4RRFFQ69MT${t.toString().padStart(2, '0')}${i.toString().padStart(2, '0')}`,
            nowIso,
            { status_code: isFailing && i <= 16 ? 500 : 200 },
            tenantId,
            `proj_${t}`
          )
        );
      }
    }

    await clickhouse.insertBatch(events.map((e) => InMemoryClickHouseAdapter.envelopeToClickHouseRecord(e)));

    // Concurrently evaluate all 20 rules
    const evaluationPromises = rules.map((r) => evaluator.evaluateRule(r.id));
    const results = await Promise.all(evaluationPromises);

    // Verify outcomes:
    for (let t = 1; t <= tenantCount; t++) {
      const { outcome } = results[t - 1];
      const isFailing = t === 7 || t === 15;

      if (isFailing) {
        expect(outcome).toBeDefined();
        expect(outcome.newState).toBe('FIRING');
        expect(outcome.action).toBe('SEND_FIRE_NOTIFICATION');
        expect(outcome.currentValue).toBe(80); // 16 / 20 = 80%
      } else {
        expect(outcome).toBeDefined();
        expect(outcome.newState).toBe('OK');
        expect(outcome.action).toBe('NOOP');
        expect(outcome.currentValue).toBe(0);
      }
    }

    // Webhooks must be dispatched ONLY for Tenant 7 and Tenant 15
    expect(dispatcher.dispatched).toHaveLength(2);
    const dispatchedTenants = dispatcher.dispatched.map((d) => d.payload.tenant_id).sort();
    expect(dispatchedTenants).toEqual(['tenant_concurrent_15', 'tenant_concurrent_7'].sort());
  });
});
