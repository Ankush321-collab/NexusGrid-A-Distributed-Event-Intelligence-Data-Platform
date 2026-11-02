import { validateEventEnvelope, type EventEnvelope, type TenantContext } from '@nexusgrid/contracts';

export interface ValidationOutcome {
  valid: boolean;
  code?: string;
  detail?: string;
  invalidParams?: Array<{ name: string; reason: string }>;
}

export interface EventValidatorOptions {
  maxBatchSize?: number;
  maxPayloadBytes?: number;
}

const DEFAULT_MAX_BATCH_SIZE = 500;
const DEFAULT_MAX_PAYLOAD_BYTES = 1048576; // 1 MB
const ONE_HOUR_MS = 3600 * 1000;
const THIRTY_DAYS_MS = 30 * 24 * 3600 * 1000;

export class EventValidator {
  private maxBatchSize: number;
  private maxPayloadBytes: number;

  constructor(options: EventValidatorOptions = {}) {
    this.maxBatchSize = options.maxBatchSize ?? DEFAULT_MAX_BATCH_SIZE;
    this.maxPayloadBytes = options.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;
  }
  /**
   * Validates a single event against schema, tenant context, and timestamp boundaries
   */
  public validateSingle(event: unknown, tenantContext?: TenantContext): ValidationOutcome {
    const schemaRes = validateEventEnvelope(event);
    if (!schemaRes.valid) {
      return {
        valid: false,
        code: 'ERR_SCHEMA_VALIDATION_FAILED',
        detail: 'Event envelope failed schema validation',
        invalidParams: (schemaRes.errors || []).map((err) => ({ name: 'envelope', reason: err })),
      };
    }

    const env = event as EventEnvelope;

    // Tenant context matching (Defense against tenant spoofing)
    if (tenantContext) {
      if (env.tenant_id !== tenantContext.tenant_id) {
        return {
          valid: false,
          code: 'ERR_TENANT_MISMATCH',
          detail: `Payload tenant_id "${env.tenant_id}" does not match authenticated tenant "${tenantContext.tenant_id}"`,
        };
      }
      if (env.project_id !== tenantContext.project_id) {
        return {
          valid: false,
          code: 'ERR_PROJECT_MISMATCH',
          detail: `Payload project_id "${env.project_id}" does not match authenticated project "${tenantContext.project_id}"`,
        };
      }
    }

    // Timestamp freshness validation
    const occurredTime = Date.parse(env.occurred_at);
    const now = Date.now();

    // Check future skew (> 1 hour ahead)
    if (occurredTime > now + ONE_HOUR_MS) {
      return {
        valid: false,
        code: 'ERR_TIMESTAMP_IN_FUTURE',
        detail: `occurred_at (${env.occurred_at}) is more than 1 hour in the future`,
      };
    }

    // Check historical retention boundary (> 30 days old)
    if (occurredTime < now - THIRTY_DAYS_MS) {
      return {
        valid: false,
        code: 'ERR_TIMESTAMP_EXPIRED',
        detail: `occurred_at (${env.occurred_at}) is older than the 30-day ingestion window`,
      };
    }

    return { valid: true };
  }

  /**
   * Validates raw payload byte size
   */
  public validatePayloadSize(contentLengthHeader?: string | number): ValidationOutcome {
    if (contentLengthHeader) {
      const length = typeof contentLengthHeader === 'string' ? parseInt(contentLengthHeader, 10) : contentLengthHeader;
      if (length > this.maxPayloadBytes) {
        return {
          valid: false,
          code: 'ERR_PAYLOAD_TOO_LARGE',
          detail: `Request body exceeds maximum allowed size of 1MB (${length} bytes)`,
        };
      }
    }
    return { valid: true };
  }

  /**
   * Validates batch bounds
   */
  public validateBatchLimits(events: unknown[]): ValidationOutcome {
    if (!Array.isArray(events)) {
      return {
        valid: false,
        code: 'ERR_INVALID_BATCH_FORMAT',
        detail: 'Batch payload must contain an "events" array',
      };
    }

    if (events.length === 0) {
      return {
        valid: false,
        code: 'ERR_EMPTY_BATCH',
        detail: 'Batch contains 0 events',
      };
    }

    if (events.length > this.maxBatchSize) {
      return {
        valid: false,
        code: 'ERR_BATCH_LIMIT_EXCEEDED',
        detail: `Batch contains ${events.length} events, which exceeds maximum limit of ${this.maxBatchSize} events`,
      };
    }

    return { valid: true };
  }
}

export const defaultEventValidator = new EventValidator();

