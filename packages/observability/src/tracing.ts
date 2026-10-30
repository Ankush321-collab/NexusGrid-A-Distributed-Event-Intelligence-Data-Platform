import crypto from 'crypto';

export interface TraceContext {
  traceId: string;
  spanId: string;
  correlationId: string;
  traceparent: string;
}

export function generateTraceId(): string {
  return crypto.randomBytes(16).toString('hex');
}

export function generateSpanId(): string {
  return crypto.randomBytes(8).toString('hex');
}

export function generateCorrelationId(): string {
  // ULID-like sortable timestamp (10 hex) + randomness (16 hex)
  const time = Date.now().toString(16).padStart(12, '0');
  const rand = crypto.randomBytes(10).toString('hex');
  return `cor_${time}${rand}`;
}

export function extractOrGenerateTraceContext(headers?: Record<string, string | string[] | undefined>): TraceContext {
  let traceId = '';
  let spanId = generateSpanId();
  let correlationId = '';

  if (headers) {
    const traceparent = typeof headers['traceparent'] === 'string' ? headers['traceparent'] : undefined;
    if (traceparent) {
      const parts = traceparent.split('-');
      if (parts.length === 4 && parts[1] && parts[1].length === 32) {
        traceId = parts[1];
      }
    }

    const corr = headers['x-correlation-id'] || headers['x-request-id'];
    if (typeof corr === 'string' && corr.length > 0) {
      correlationId = corr;
    }

    const tid = headers['x-trace-id'];
    if (!traceId && typeof tid === 'string' && tid.length > 0) {
      traceId = tid;
    }
  }

  if (!traceId) {
    traceId = generateTraceId();
  }
  if (!correlationId) {
    correlationId = generateCorrelationId();
  }

  return {
    traceId,
    spanId,
    correlationId,
    traceparent: `00-${traceId}-${spanId}-01`,
  };
}

