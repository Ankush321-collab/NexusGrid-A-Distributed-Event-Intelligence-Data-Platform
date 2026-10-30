import { describe, it, expect, vi } from 'vitest';
import {
  Logger,
  MetricsRegistry,
  generateTraceId,
  generateCorrelationId,
  extractOrGenerateTraceContext,
} from '../../packages/observability/src/index.js';

describe('Phase 0 - Structured Observability, Tracing, and Metrics', () => {
  it('generates compliant 32-character hex trace IDs and correlation IDs', () => {
    const traceId = generateTraceId();
    expect(traceId).toHaveLength(32);
    expect(/^[0-9a-f]{32}$/.test(traceId)).toBe(true);

    const corrId = generateCorrelationId();
    expect(corrId.startsWith('cor_')).toBe(true);
  });

  it('extracts existing W3C traceparent context or generates new', () => {
    const incomingHeaders = {
      traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
      'x-correlation-id': 'cor_client_req_99',
    };

    const ctx = extractOrGenerateTraceContext(incomingHeaders);
    expect(ctx.traceId).toBe('4bf92f3577b34da6a3ce929d0e0e4736');
    expect(ctx.correlationId).toBe('cor_client_req_99');
    expect(ctx.traceparent.startsWith('00-4bf92f3577b34da6a3ce929d0e0e4736-')).toBe(true);
  });

  it('structured logger emits JSON with correlation ID and service name', () => {
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const logger = new Logger('test-service', 'info', {
      trace_id: 'trace_123',
      correlation_id: 'corr_456',
    });

    logger.info('Processing started', { batch_size: 100 });

    expect(logSpy).toHaveBeenCalled();
    const loggedRaw = logSpy.mock.calls[0][0];
    const parsed = JSON.parse(loggedRaw);

    expect(parsed.service).toBe('test-service');
    expect(parsed.level).toBe('info');
    expect(parsed.message).toBe('Processing started');
    expect(parsed.trace_id).toBe('trace_123');
    expect(parsed.batch_size).toBe(100);
    expect(parsed.timestamp).toBeDefined();

    logSpy.mockRestore();
  });

  it('metrics registry aggregates counters and percentile histograms (p50, p95, p99)', () => {
    const registry = new MetricsRegistry();
    registry.incrementCounter('events_ingested_total', 10, { tenant_id: 't1' });
    registry.incrementCounter('events_ingested_total', 15, { tenant_id: 't1' });

    expect(registry.getCounter('events_ingested_total', { tenant_id: 't1' })).toBe(25);

    // Record durations
    for (let i = 1; i <= 100; i++) {
      registry.observeDuration('ingest_duration_ms', i, { route: '/v1/events' });
    }

    const stats = registry.getHistogramStats('ingest_duration_ms', { route: '/v1/events' });
    expect(stats.count).toBe(100);
    expect(stats.p50).toBeGreaterThanOrEqual(49);
    expect(stats.p50).toBeLessThanOrEqual(52);
    expect(stats.p95).toBeGreaterThanOrEqual(94);
    expect(stats.p99).toBeGreaterThanOrEqual(98);
  });
});

