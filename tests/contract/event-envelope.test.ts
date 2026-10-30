import { describe, it, expect } from 'vitest';
import {
  validateEventEnvelope,
  validateProblemDetails,
  type EventEnvelope,
  type ProblemDetails,
} from '../../packages/contracts/src/index.js';

describe('Phase 0 - Canonical Event Envelope Contract Validation', () => {
  const validEvent: EventEnvelope = {
    event_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV', // 26-char valid ULID
    tenant_id: 'tenant_acme_corp',
    project_id: 'project_analytics',
    event_name: 'user.checkout.completed',
    occurred_at: '2026-10-30T12:00:00.000Z',
    schema_version: 1,
    idempotency_key: 'idem_checkout_99182',
    properties: {
      cart_id: 'c_88192',
      total_cents: 4990,
      currency: 'USD',
      items_count: 3,
    },
  };

  it('accepts a fully compliant canonical event envelope', () => {
    const result = validateEventEnvelope(validEvent);
    expect(result.valid).toBe(true);
    expect(result.errors).toBeUndefined();
  });

  it('rejects an envelope missing mandatory event_id', () => {
    const invalid = { ...validEvent } as Partial<EventEnvelope>;
    delete invalid.event_id;
    const result = validateEventEnvelope(invalid);
    expect(result.valid).toBe(false);
    expect(result.errors?.length).toBeGreaterThan(0);
  });

  it('rejects an invalid ULID format (too short or invalid chars)', () => {
    const invalid = { ...validEvent, event_id: 'invalid-ulid-123' };
    const result = validateEventEnvelope(invalid);
    expect(result.valid).toBe(false);
  });

  it('rejects invalid timestamp format', () => {
    const invalid = { ...validEvent, occurred_at: 'not-a-valid-date' };
    const result = validateEventEnvelope(invalid);
    expect(result.valid).toBe(false);
  });

  it('rejects schema_version < 1', () => {
    const invalid = { ...validEvent, schema_version: 0 };
    const result = validateEventEnvelope(invalid);
    expect(result.valid).toBe(false);
  });

  it('rejects payloads with undeclared root properties (strict additionalProperties=false)', () => {
    const invalid = { ...validEvent, unexpected_extra_field: 'hacker_payload' };
    const result = validateEventEnvelope(invalid);
    expect(result.valid).toBe(false);
  });

  it('validates RFC 7807 problem details specification', () => {
    const problem: ProblemDetails = {
      type: 'https://nexusgrid.io/errors/invalid-payload',
      title: 'Invalid Event Payload',
      status: 400,
      detail: 'The submitted event envelope failed schema validation.',
      code: 'ERR_SCHEMA_VALIDATION_FAILED',
      trace_id: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
      invalid_params: [{ name: 'event_id', reason: 'must match pattern' }],
    };

    const res = validateProblemDetails(problem);
    expect(res.valid).toBe(true);
  });
});

