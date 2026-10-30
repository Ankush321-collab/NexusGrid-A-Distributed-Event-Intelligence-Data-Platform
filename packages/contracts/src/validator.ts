import Ajv, { type ValidateFunction } from 'ajv';
import addFormats from 'ajv-formats';
import eventEnvelopeSchema from './schemas/event-envelope.json' assert { type: 'json' };
import errorEnvelopeSchema from './schemas/error-envelope.json' assert { type: 'json' };
import type { EventEnvelope, ProblemDetails } from './types/index.js';

let ajvInstance: Ajv | null = null;
let eventValidateFn: ValidateFunction | null = null;
let errorValidateFn: ValidateFunction | null = null;

function getAjv(): Ajv {
  if (!ajvInstance) {
    ajvInstance = new Ajv({ allErrors: true, strict: false });
    addFormats(ajvInstance);
  }
  return ajvInstance;
}

export interface ValidationResult {
  valid: boolean;
  errors?: string[];
}

// Regex for ULID format: 26 characters of Crockford base32
const ULID_REGEX = /^[0-9A-HJKMNP-TV-Z]{26}$/;

/**
 * Validates canonical event envelope
 */
export function validateEventEnvelope(data: unknown): ValidationResult {
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    return { valid: false, errors: ['Payload must be a non-null JSON object'] };
  }

  try {
    const ajv = getAjv();
    if (!eventValidateFn) {
      eventValidateFn = ajv.compile(eventEnvelopeSchema);
    }
    const isValid = eventValidateFn(data);
    if (!isValid && eventValidateFn.errors) {
      return {
        valid: false,
        errors: eventValidateFn.errors.map(
          (e) => `${e.instancePath || 'root'} ${e.message || 'is invalid'}`
        ),
      };
    }
    return { valid: true };
  } catch (_err) {
    // Fallback native schema check strictly matching the schema contract
    const d = data as Record<string, unknown>;
    const errors: string[] = [];

    if (!d.event_id || typeof d.event_id !== 'string' || !ULID_REGEX.test(d.event_id)) {
      errors.push('event_id must be a valid 26-character ULID string');
    }
    if (!d.tenant_id || typeof d.tenant_id !== 'string' || d.tenant_id.length < 3 || d.tenant_id.length > 64) {
      errors.push('tenant_id must be a string between 3 and 64 characters');
    }
    if (!d.project_id || typeof d.project_id !== 'string' || d.project_id.length < 3 || d.project_id.length > 64) {
      errors.push('project_id must be a string between 3 and 64 characters');
    }
    if (!d.event_name || typeof d.event_name !== 'string' || d.event_name.length < 1 || d.event_name.length > 128) {
      errors.push('event_name must be a string between 1 and 128 characters');
    }
    if (!d.occurred_at || typeof d.occurred_at !== 'string' || isNaN(Date.parse(d.occurred_at))) {
      errors.push('occurred_at must be a valid ISO 8601 date-time string');
    }
    if (typeof d.schema_version !== 'number' || d.schema_version < 1 || !Number.isInteger(d.schema_version)) {
      errors.push('schema_version must be an integer >= 1');
    }
    if (!d.properties || typeof d.properties !== 'object' || Array.isArray(d.properties)) {
      errors.push('properties must be an object');
    }

    // Additional properties disallowed
    const allowed = new Set([
      'event_id',
      'tenant_id',
      'project_id',
      'event_name',
      'occurred_at',
      'received_at',
      'schema_version',
      'idempotency_key',
      'properties',
    ]);
    for (const key of Object.keys(d)) {
      if (!allowed.has(key)) {
        errors.push(`Undeclared root property: "${key}" is not permitted`);
      }
    }

    return {
      valid: errors.length === 0,
      errors: errors.length > 0 ? errors : undefined,
    };
  }
}

/**
 * Validates RFC 7807 problem details
 */
export function validateProblemDetails(data: unknown): ValidationResult {
  if (!data || typeof data !== 'object') {
    return { valid: false, errors: ['Problem details must be an object'] };
  }
  const d = data as Record<string, unknown>;
  const errors: string[] = [];
  if (!d.type || typeof d.type !== 'string') errors.push('type must be a string URI');
  if (!d.title || typeof d.title !== 'string') errors.push('title must be a string');
  if (typeof d.status !== 'number') errors.push('status must be a number');
  if (!d.detail || typeof d.detail !== 'string') errors.push('detail must be a string');
  if (!d.code || typeof d.code !== 'string') errors.push('code must be a string');
  if (!d.trace_id || typeof d.trace_id !== 'string') errors.push('trace_id must be a string');

  return {
    valid: errors.length === 0,
    errors: errors.length > 0 ? errors : undefined,
  };
}

