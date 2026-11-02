import { describe, it, expect, beforeEach } from 'vitest';
import { InMemoryClickHouseAdapter } from '../../packages/database/src/index.js';
import { AlertStateMachine, type AlertRuleConfig } from '../../apps/alert-service/src/state/alert-state-machine.js';
import { WebhookDispatcher } from '../../apps/alert-service/src/dispatch/webhook-dispatcher.js';
import { RuleEvaluator } from '../../apps/alert-service/src/evaluator/rule-evaluator.js';
import type { EventEnvelope } from '../../packages/contracts/src/index.js';

describe('Phase 6 - Anomaly Detection, Alerting Engine & Scheduled Aggregates', () => {
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

  const sampleRule: AlertRuleConfig = {
    id: 'rule_error_rate_5pct',
    tenant_id: 'tenant_alert_test',
    project_id: 'proj_alert_test',
    name: 'High Error Rate (> 5%)',
    metric_name: 'error_rate',
    operator: 'gt',
    threshold: 5.0,
    consecutive_breaches_required: 2, // Hysteresis: requires 2 consecutive breaches
    cooldown_minutes: 15,
    webhook_url: 'https://api.customer.com/webhooks/alerts',
    webhook_secret: 'whsec_super_secret_signing_key_123',
  };

  // =========================================================================
  // Test 6.1: Alert State Machine Lifecycle & Hysteresis Flapping Defense
  // =========================================================================
  it('prevents flapping via hysteresis, triggers on sustained breach, suppresses in cooldown, and recovers', () => {
    let now = Date.now();

    // Initial state: OK
    expect(fsm.getState(sampleRule.id)).toBe('OK');

    // Cycle 1: Breached (Error rate = 12% > 5%)
    // Hysteresis: 1st breach moves to PENDING, does NOT page on single spike!
    const cycle1 = fsm.evaluate(sampleRule, 12.0, now);
    expect(cycle1.previousState).toBe('OK');
    expect(cycle1.newState).toBe('PENDING');
    expect(cycle1.action).toBe('NOOP');

    // Cycle 2: Breach Sustained (Error rate = 14% > 5%)
    // 2nd breach satisfies consecutive_breaches_required -> Transitions to FIRING!
    now += 60000; // +1 minute
    const cycle2 = fsm.evaluate(sampleRule, 14.0, now);
    expect(cycle2.previousState).toBe('PENDING');
    expect(cycle2.newState).toBe('FIRING');
    expect(cycle2.action).toBe('SEND_FIRE_NOTIFICATION');

    // Cycle 3: Breach Continues (Error rate = 15% > 5%)
    // In Cooldown (< 15 mins): Action must be SUPPRESSED_BY_COOLDOWN (No duplicate notification)
    now += 60000; // +2 minutes
    const cycle3 = fsm.evaluate(sampleRule, 15.0, now);
    expect(cycle3.previousState).toBe('FIRING');
    expect(cycle3.newState).toBe('FIRING');
    expect(cycle3.action).toBe('SUPPRESSED_BY_COOLDOWN');

    // Cycle 4: System Heals! Value drops back below threshold (Error rate = 1.2% < 5%)
    // Transitions to OK, dispatches RECOVERY notification!
    now += 60000; // +3 minutes
    const cycle4 = fsm.evaluate(sampleRule, 1.2, now);
    expect(cycle4.previousState).toBe('FIRING');
    expect(cycle4.newState).toBe('OK');
    expect(cycle4.action).toBe('SEND_RESOLVE_NOTIFICATION');
  });

  // =========================================================================
  // Test 6.2: HMAC-SHA256 Webhook Cryptographic Signature Verification
  // =========================================================================
  it('dispatches cryptographically signed webhooks with HMAC-SHA256 that verify strictly', async () => {
    const outcome = fsm.evaluate(sampleRule, 20.0); // PENDING
    const firingOutcome = fsm.evaluate(sampleRule, 22.0); // FIRING

    const dispatched = await dispatcher.dispatch(sampleRule, firingOutcome);
    expect(dispatched.status).toBe('DELIVERED');
    expect(dispatched.signature.startsWith('sha256=')).toBe(true);

    const payloadJson = JSON.stringify(dispatched.payload);

    // 1. Valid secret -> Verification MUST succeed
    const isValid = WebhookDispatcher.verifySignature(payloadJson, dispatched.signature, sampleRule.webhook_secret);
    expect(isValid).toBe(true);

    // 2. Wrong secret -> Verification MUST fail
    const isInvalid = WebhookDispatcher.verifySignature(payloadJson, dispatched.signature, 'wrong_secret_key');
    expect(isInvalid).toBe(false);

    // 3. Tampered payload -> Verification MUST fail
    const tamperedPayload = JSON.stringify({ ...dispatched.payload, current_value: 999.0 });
    const isTamperedValid = WebhookDispatcher.verifySignature(tamperedPayload, dispatched.signature, sampleRule.webhook_secret);
    expect(isTamperedValid).toBe(false);
  });

  // =========================================================================
  // Test 6.3: End-to-End Metric Evaluation with ClickHouse Columnar Source
  // =========================================================================
  it('evaluates error rate from ClickHouse analytical queries and manages incident lifecycle', async () => {
    evaluator.registerRule(sampleRule);

    const now = Date.now();
    const eventTime = new Date(now - 60000).toISOString(); // 1 minute ago

    // Seed 100 events into ClickHouse: 85 Success (200), 15 Errors (500) -> 15% error rate
    const events: EventEnvelope[] = [];
    for (let i = 1; i <= 100; i++) {
      events.push({
        event_id: `01ARZ3NDEKTSV4RRFFQ69GALR${i.toString().padStart(3, '0')}`,
        tenant_id: sampleRule.tenant_id,
        project_id: sampleRule.project_id,
        event_name: 'api.call',
        occurred_at: eventTime,
        received_at: eventTime,
        schema_version: 1,
        properties: { status_code: i <= 15 ? 500 : 200 },
      });
    }

    await clickhouse.insertBatch(events.map((e) => InMemoryClickHouseAdapter.envelopeToClickHouseRecord(e)));

    // Evaluation Cycle 1: Computes 15% error rate -> Moves to PENDING
    const cycle1 = await evaluator.evaluateRule(sampleRule.id, now);
    expect(cycle1.outcome.currentValue).toBe(15);
    expect(cycle1.outcome.newState).toBe('PENDING');
    expect(dispatcher.dispatched).toHaveLength(0);

    // Evaluation Cycle 2: Computes 15% error rate -> Moves to FIRING -> Dispatches Webhook!
    const cycle2 = await evaluator.evaluateRule(sampleRule.id, now + 10000);
    expect(cycle2.outcome.newState).toBe('FIRING');
    expect(dispatcher.dispatched).toHaveLength(1);
    expect(dispatcher.dispatched[0].payload.event).toBe('ALERT_FIRING');
    expect(dispatcher.dispatched[0].payload.current_value).toBe(15);

    // Incident recorded in incident log
    expect(evaluator.incidents).toHaveLength(1);
    expect(evaluator.incidents[0].state).toBe('FIRING');
  });

  // =========================================================================
  // Test 6.4: Multi-Tenant Alert Isolation
  // =========================================================================
  it('guarantees tenant alert isolation: Tenant A alert rules do not evaluate or trigger on Tenant B data', async () => {
    evaluator.registerRule(sampleRule); // Tenant alert_test

    const now = Date.now();
    const eventTime = new Date(now - 60000).toISOString();

    // Seed errors ONLY for tenant_other
    const otherEvents: EventEnvelope[] = [];
    for (let i = 1; i <= 50; i++) {
      otherEvents.push({
        event_id: `01ARZ3NDEKTSV4RRFFQ69GOTH${i.toString().padStart(3, '0')}`,
        tenant_id: 'tenant_other',
        project_id: 'proj_other',
        event_name: 'api.call',
        occurred_at: eventTime,
        received_at: eventTime,
        schema_version: 1,
        properties: { status_code: 500 }, // All errors
      });
    }

    await clickhouse.insertBatch(otherEvents.map((e) => InMemoryClickHouseAdapter.envelopeToClickHouseRecord(e)));

    // Evaluate Tenant alert_test rule -> Error rate must be 0% because Tenant other is isolated
    const cycle = await evaluator.evaluateRule(sampleRule.id, now);
    expect(cycle.outcome.currentValue).toBe(0);
    expect(cycle.outcome.newState).toBe('OK');
    expect(dispatcher.dispatched).toHaveLength(0);
  });
});
