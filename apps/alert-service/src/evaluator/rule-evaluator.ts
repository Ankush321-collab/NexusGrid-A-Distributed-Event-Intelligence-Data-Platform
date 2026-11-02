import type { ClickHouseClientInterface } from '@nexusgrid/database';
import { AlertStateMachine, defaultAlertStateMachine, type AlertRuleConfig, type EvaluationOutcome } from '../state/alert-state-machine.js';
import { WebhookDispatcher, defaultWebhookDispatcher, type DispatchedRecord } from '../dispatch/webhook-dispatcher.js';

export interface AlertIncidentRecord {
  id: string;
  rule_id: string;
  tenant_id: string;
  state: string;
  current_value: number;
  threshold: number;
  dedupe_key: string;
  triggered_at: string;
  resolved_at?: string;
}

export class RuleEvaluator {
  public rules: Map<string, AlertRuleConfig> = new Map();
  public incidents: AlertIncidentRecord[] = [];

  constructor(
    private clickhouse: ClickHouseClientInterface,
    private fsm: AlertStateMachine = defaultAlertStateMachine,
    private dispatcher: WebhookDispatcher = defaultWebhookDispatcher
  ) {}

  public registerRule(rule: AlertRuleConfig): void {
    this.rules.set(rule.id, rule);
  }

  public getRule(ruleId: string): AlertRuleConfig | undefined {
    return this.rules.get(ruleId);
  }

  /**
   * Fetches analytical metric from ClickHouse for the rule's specified window
   */
  public async computeMetricValue(rule: AlertRuleConfig, nowMs: number = Date.now()): Promise<number> {
    const windowStartMs = nowMs - (rule.window_minutes || 5) * 60 * 1000;
    const startIso = new Date(windowStartMs).toISOString();
    const endIso = new Date(nowMs).toISOString();

    const queryResult = await this.clickhouse.query({
      tenant_id: rule.tenant_id,
      project_id: rule.project_id,
      start_time: startIso,
      end_time: endIso,
      metrics: ['count()', 'avg(latency_ms)', 'p95(latency_ms)'],
    });

    const rows = queryResult.data;
    if (rows.length === 0) return 0;

    const row = rows[0];

    if (rule.metric_name === 'p95_latency_ms') {
      return row['p95(latency_ms)'] ?? 0;
    }

    if (rule.metric_name === 'avg_latency_ms') {
      return row['avg(latency_ms)'] ?? 0;
    }

    if (rule.metric_name === 'error_rate') {
      // Query error events (status_code >= 500)
      const errorResult = await this.clickhouse.query({
        tenant_id: rule.tenant_id,
        project_id: rule.project_id,
        start_time: startIso,
        end_time: endIso,
        filters: [{ field: 'properties_numeric.status_code', op: 'gte', value: 500 }],
        metrics: ['count()'],
      });
      const totalCount = row.count || 0;
      const errorCount = errorResult.data[0]?.count || 0;
      if (totalCount === 0) return 0;
      return (errorCount / totalCount) * 100; // Error rate percentage
    }

    // Default to total event count
    return row.count || 0;
  }

  /**
   * Evaluates a single rule: computes metric, advances FSM, dispatches notification if needed
   */
  public async evaluateRule(ruleId: string, nowMs: number = Date.now()): Promise<{ outcome: EvaluationOutcome; dispatched?: DispatchedRecord }> {
    const rule = this.rules.get(ruleId);
    if (!rule) {
      throw new Error(`Alert rule "${ruleId}" not found`);
    }

    const value = await this.computeMetricValue(rule, nowMs);
    const outcome = this.fsm.evaluate(rule, value, nowMs);

    let dispatched: DispatchedRecord | undefined;

    if (outcome.action === 'SEND_FIRE_NOTIFICATION') {
      dispatched = await this.dispatcher.dispatch(rule, outcome);

      this.incidents.push({
        id: `inc_${Date.now()}_${rule.id}`,
        rule_id: rule.id,
        tenant_id: rule.tenant_id,
        state: 'FIRING',
        current_value: value,
        threshold: rule.threshold,
        dedupe_key: outcome.dedupeKey,
        triggered_at: new Date(nowMs).toISOString(),
      });
    } else if (outcome.action === 'SEND_RESOLVE_NOTIFICATION') {
      dispatched = await this.dispatcher.dispatch(rule, outcome);

      const ongoing = this.incidents.find((i) => i.rule_id === rule.id && i.state === 'FIRING');
      if (ongoing) {
        ongoing.state = 'RESOLVED';
        ongoing.resolved_at = new Date(nowMs).toISOString();
      }
    }

    return { outcome, dispatched };
  }

  /**
   * Runs evaluation cycle across all registered rules
   */
  public async evaluateAll(nowMs: number = Date.now()): Promise<Array<{ ruleId: string; outcome: EvaluationOutcome }>> {
    const results: Array<{ ruleId: string; outcome: EvaluationOutcome }> = [];
    for (const ruleId of this.rules.keys()) {
      const res = await this.evaluateRule(ruleId, nowMs);
      results.push({ ruleId, outcome: res.outcome });
    }
    return results;
  }
}
