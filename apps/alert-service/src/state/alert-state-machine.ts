import crypto from 'crypto';

export type AlertState = 'OK' | 'PENDING' | 'FIRING';

export interface AlertRuleConfig {
  id: string;
  tenant_id: string;
  project_id: string;
  name: string;
  metric_name: string;
  operator: 'gt' | 'gte' | 'lt' | 'lte' | 'eq';
  threshold: number;
  consecutive_breaches_required: number;
  cooldown_minutes: number;
  webhook_url: string;
  webhook_secret: string;
  window_minutes?: number;
}

export interface EvaluationOutcome {
  ruleId: string;
  tenantId: string;
  previousState: AlertState;
  newState: AlertState;
  currentValue: number;
  threshold: number;
  isBreached: boolean;
  action: 'NOOP' | 'SEND_FIRE_NOTIFICATION' | 'SUPPRESSED_BY_COOLDOWN' | 'SEND_RESOLVE_NOTIFICATION';
  dedupeKey: string;
}

export class AlertStateMachine {
  private states: Map<string, { state: AlertState; consecutiveBreaches: number; lastFiredAt?: number }> = new Map();

  public isThresholdBreached(value: number, operator: string, threshold: number): boolean {
    switch (operator) {
      case 'gt': return value > threshold;
      case 'gte': return value >= threshold;
      case 'lt': return value < threshold;
      case 'lte': return value <= threshold;
      case 'eq': return value === threshold;
      default: return false;
    }
  }

  public generateDedupeKey(ruleId: string, tenantId: string, bucketTimestamp: number): string {
    const raw = `${tenantId}:${ruleId}:${bucketTimestamp}`;
    return crypto.createHash('sha256').update(raw).digest('hex').slice(0, 16);
  }

  /**
   * Processes an incoming metric evaluation cycle for a rule through the FSM
   */
  public evaluate(rule: AlertRuleConfig, currentValue: number, now: number = Date.now()): EvaluationOutcome {
    let current = this.states.get(rule.id);
    if (!current) {
      current = { state: 'OK', consecutiveBreaches: 0 };
      this.states.set(rule.id, current);
    }

    const previousState = current.state;
    const isBreached = this.isThresholdBreached(currentValue, rule.operator, rule.threshold);
    let newState: AlertState = current.state;
    let action: EvaluationOutcome['action'] = 'NOOP';

    // 15-minute bucket for dedupe keys
    const bucket = Math.floor(now / (rule.cooldown_minutes * 60 * 1000));
    const dedupeKey = this.generateDedupeKey(rule.id, rule.tenant_id, bucket);

    if (isBreached) {
      current.consecutiveBreaches++;

      if (current.state === 'OK') {
        if (current.consecutiveBreaches >= rule.consecutive_breaches_required) {
          newState = 'FIRING';
          current.lastFiredAt = now;
          action = 'SEND_FIRE_NOTIFICATION';
        } else {
          newState = 'PENDING';
          action = 'NOOP';
        }
      } else if (current.state === 'PENDING') {
        if (current.consecutiveBreaches >= rule.consecutive_breaches_required) {
          newState = 'FIRING';
          current.lastFiredAt = now;
          action = 'SEND_FIRE_NOTIFICATION';
        } else {
          newState = 'PENDING';
          action = 'NOOP';
        }
      } else if (current.state === 'FIRING') {
        // Already firing: Check cooldown
        const cooldownMs = rule.cooldown_minutes * 60 * 1000;
        if (current.lastFiredAt && now - current.lastFiredAt < cooldownMs) {
          action = 'SUPPRESSED_BY_COOLDOWN';
        } else {
          current.lastFiredAt = now;
          action = 'SEND_FIRE_NOTIFICATION';
        }
      }
    } else {
      // Threshold not breached: Reset breach counter
      current.consecutiveBreaches = 0;

      if (current.state === 'FIRING') {
        newState = 'OK';
        action = 'SEND_RESOLVE_NOTIFICATION';
      } else {
        newState = 'OK';
        action = 'NOOP';
      }
    }

    current.state = newState;

    return {
      ruleId: rule.id,
      tenantId: rule.tenant_id,
      previousState,
      newState,
      currentValue,
      threshold: rule.threshold,
      isBreached,
      action,
      dedupeKey,
    };
  }

  public getState(ruleId: string): AlertState {
    return this.states.get(ruleId)?.state || 'OK';
  }

  public reset(): void {
    this.states.clear();
  }
}

export const defaultAlertStateMachine = new AlertStateMachine();

