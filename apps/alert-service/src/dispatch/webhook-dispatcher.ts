import crypto from 'crypto';
import type { AlertRuleConfig, EvaluationOutcome } from '../state/alert-state-machine.js';

export interface WebhookNotificationPayload {
  event: 'ALERT_FIRING' | 'ALERT_RESOLVED';
  rule_id: string;
  rule_name: string;
  tenant_id: string;
  project_id: string;
  metric_name: string;
  current_value: number;
  threshold: number;
  dedupe_key: string;
  timestamp: string;
}

export interface DispatchedRecord {
  url: string;
  signature: string;
  payload: WebhookNotificationPayload;
  attempt: number;
  status: 'DELIVERED' | 'FAILED';
}

export class WebhookDispatcher {
  public dispatched: DispatchedRecord[] = [];

  /**
   * Generates HMAC-SHA256 signature over stringified JSON payload
   */
  public static signPayload(payloadJson: string, secret: string): string {
    const hmac = crypto.createHmac('sha256', secret);
    return `sha256=${hmac.update(payloadJson).digest('hex')}`;
  }

  /**
   * Helper for clients/tests to verify HMAC signature
   */
  public static verifySignature(payloadJson: string, signatureHeader: string, secret: string): boolean {
    const expected = WebhookDispatcher.signPayload(payloadJson, secret);
    const bufA = Buffer.from(signatureHeader);
    const bufB = Buffer.from(expected);
    if (bufA.length !== bufB.length) return false;
    return crypto.timingSafeEqual(bufA, bufB);
  }

  public async dispatch(
    rule: AlertRuleConfig,
    outcome: EvaluationOutcome,
    mockHttpFailure: boolean = false
  ): Promise<DispatchedRecord> {
    const eventType = outcome.action === 'SEND_FIRE_NOTIFICATION' ? 'ALERT_FIRING' : 'ALERT_RESOLVED';

    const payload: WebhookNotificationPayload = {
      event: eventType,
      rule_id: rule.id,
      rule_name: rule.name,
      tenant_id: rule.tenant_id,
      project_id: rule.project_id,
      metric_name: rule.metric_name,
      current_value: outcome.currentValue,
      threshold: outcome.threshold,
      dedupe_key: outcome.dedupeKey,
      timestamp: new Date().toISOString(),
    };

    const payloadJson = JSON.stringify(payload);
    const signature = WebhookDispatcher.signPayload(payloadJson, rule.webhook_secret);

    const record: DispatchedRecord = {
      url: rule.webhook_url,
      signature,
      payload,
      attempt: 1,
      status: mockHttpFailure ? 'FAILED' : 'DELIVERED',
    };

    this.dispatched.push(record);
    return record;
  }

  public clear(): void {
    this.dispatched = [];
  }
}

export const defaultWebhookDispatcher = new WebhookDispatcher();
