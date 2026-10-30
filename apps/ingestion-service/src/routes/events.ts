import type { FastifyInstance } from 'fastify';
import { EventValidator, defaultEventValidator } from '../validators/event-validator.js';
import { KafkaProducer, defaultKafkaProducer } from '../producer/kafka-producer.js';
import { extractOrGenerateTraceContext } from '@nexusgrid/observability';
import type { EventEnvelope, BatchItemFailure } from '@nexusgrid/contracts';

export function registerEventRoutes(
  app: FastifyInstance,
  validator: EventValidator = defaultEventValidator,
  producer: KafkaProducer = defaultKafkaProducer
) {
  // Ingest Single Event
  app.post('/v1/events', async (req, reply) => {
    const traceCtx = extractOrGenerateTraceContext(req.headers as any);
    reply.header('X-Correlation-Id', traceCtx.correlationId);
    reply.header('X-Trace-Id', traceCtx.traceId);

    // Validate body size limit
    const sizeCheck = validator.validatePayloadSize(req.headers['content-length']);
    if (!sizeCheck.valid) {
      return reply.status(413).send({
        type: 'https://nexusgrid.io/errors/payload-too-large',
        title: 'Payload Too Large',
        status: 413,
        detail: sizeCheck.detail,
        code: sizeCheck.code,
        trace_id: traceCtx.traceId,
      });
    }

    const payload = req.body;
    const validation = validator.validateSingle(payload, req.tenantContext);

    if (!validation.valid) {
      const status = validation.code === 'ERR_TIMESTAMP_IN_FUTURE' || validation.code === 'ERR_TIMESTAMP_EXPIRED'
        ? 422
        : validation.code === 'ERR_TENANT_MISMATCH' || validation.code === 'ERR_PROJECT_MISMATCH'
        ? 403
        : 400;

      return reply.status(status).send({
        type: 'https://nexusgrid.io/errors/invalid-event',
        title: 'Invalid Event Envelope',
        status,
        detail: validation.detail,
        code: validation.code,
        trace_id: traceCtx.traceId,
        invalid_params: validation.invalidParams,
      });
    }

    const event = payload as EventEnvelope;
    const receivedAt = new Date().toISOString();
    event.received_at = receivedAt;

    // Publish to Kafka topic partitioned by hash(tenant_id + ":" + project_id)
    const pub = await producer.publish(event, traceCtx.correlationId);

    return reply.status(202).send({
      status: 'ACCEPTED',
      event_id: event.event_id,
      correlation_id: traceCtx.correlationId,
      received_at: receivedAt,
      partition: pub.partition,
    });
  });

  // Ingest Batch of Events
  app.post('/v1/events:batch', async (req, reply) => {
    const traceCtx = extractOrGenerateTraceContext(req.headers as any);
    reply.header('X-Correlation-Id', traceCtx.correlationId);
    reply.header('X-Trace-Id', traceCtx.traceId);

    // Validate body size limit
    const sizeCheck = validator.validatePayloadSize(req.headers['content-length']);
    if (!sizeCheck.valid) {
      return reply.status(413).send({
        type: 'https://nexusgrid.io/errors/payload-too-large',
        title: 'Payload Too Large',
        status: 413,
        detail: sizeCheck.detail,
        code: sizeCheck.code,
        trace_id: traceCtx.traceId,
      });
    }

    const body = req.body as { events?: unknown[] };
    if (!body || !Array.isArray(body.events)) {
      return reply.status(400).send({
        type: 'https://nexusgrid.io/errors/bad-request',
        title: 'Bad Request',
        status: 400,
        detail: 'Batch payload must contain an "events" array',
        code: 'ERR_INVALID_BATCH',
        trace_id: traceCtx.traceId,
      });
    }

    const limitsCheck = validator.validateBatchLimits(body.events);
    if (!limitsCheck.valid) {
      const status = limitsCheck.code === 'ERR_BATCH_LIMIT_EXCEEDED' ? 413 : 400;
      return reply.status(status).send({
        type: 'https://nexusgrid.io/errors/batch-limit',
        title: 'Batch Limits Exceeded',
        status,
        detail: limitsCheck.detail,
        code: limitsCheck.code,
        trace_id: traceCtx.traceId,
      });
    }

    const receivedAt = new Date().toISOString();
    const validEvents: EventEnvelope[] = [];
    const failures: BatchItemFailure[] = [];

    body.events.forEach((rawEvent, idx) => {
      const result = validator.validateSingle(rawEvent, req.tenantContext);
      if (result.valid) {
        const evt = rawEvent as EventEnvelope;
        evt.received_at = receivedAt;
        validEvents.push(evt);
      } else {
        failures.push({
          index: idx,
          event_id: (rawEvent as any)?.event_id,
          reason: result.detail || 'Validation failed',
        });
      }
    });

    // If valid events exist, publish them
    if (validEvents.length > 0) {
      await producer.publishBatch(validEvents, traceCtx.correlationId);
    }

    const overallStatus =
      failures.length === 0 ? 'ACCEPTED' : validEvents.length > 0 ? 'PARTIAL' : 'REJECTED';

    const httpCode = overallStatus === 'REJECTED' ? 400 : 202;

    return reply.status(httpCode).send({
      status: overallStatus,
      total: body.events.length,
      accepted: validEvents.length,
      rejected: failures.length,
      correlation_id: traceCtx.correlationId,
      failures,
    });
  });
}

