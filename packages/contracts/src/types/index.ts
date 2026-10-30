/**
 * NexusGrid Canonical Types and Shared Interfaces
 */

export interface EventEnvelope<T = Record<string, unknown>> {
  event_id: string; // ULID 26 characters
  tenant_id: string;
  project_id: string;
  event_name: string;
  occurred_at: string; // ISO 8601 UTC
  received_at?: string; // Stamped on gateway ingress
  schema_version: number;
  idempotency_key?: string;
  properties: T;
}

export interface ProblemDetails {
  type: string;
  title: string;
  status: number;
  detail: string;
  code: string;
  trace_id: string;
  invalid_params?: Array<{
    name: string;
    reason: string;
  }>;
  [key: string]: unknown;
}

export interface IngestionResponse {
  status: 'ACCEPTED' | 'REJECTED';
  event_id: string;
  correlation_id: string;
  received_at: string;
  partition?: number;
}

export interface BatchItemFailure {
  index: number;
  event_id?: string;
  reason: string;
}

export interface BatchIngestionResponse {
  status: 'ACCEPTED' | 'PARTIAL' | 'REJECTED';
  total: number;
  accepted: number;
  rejected: number;
  correlation_id: string;
  failures: BatchItemFailure[];
}

export interface TenantContext {
  tenant_id: string;
  project_id: string;
  key_id?: string;
  scopes: string[];
}

export interface KafkaEventMessage {
  key: string; // hash(tenant_id + ":" + project_id)
  value: string; // JSON serialized EventEnvelope
  headers: {
    'x-event-id': string;
    'x-tenant-id': string;
    'x-project-id': string;
    'x-schema-version': string;
    'x-correlation-id': string;
    'x-retry-count': string;
    'x-original-topic'?: string;
    'x-error-reason'?: string;
    'x-error-stack'?: string;
    'x-timestamp': string;
  };
}

export interface OutboxRecord {
  id: string | number;
  aggregate_type: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown>;
  headers: Record<string, string>;
  created_at: string;
  processed_at: string | null;
}

