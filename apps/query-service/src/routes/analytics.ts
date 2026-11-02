import type { FastifyInstance } from 'fastify';
import { ASTValidator, defaultASTValidator } from '../builder/ast-validator.js';
import type { QueryCacheManager } from '../cache/cache-manager.js';
import type { ClickHouseClientInterface } from '@nexusgrid/database';
import { extractOrGenerateTraceContext } from '@nexusgrid/observability';

export function registerAnalyticsRoutes(
  app: FastifyInstance,
  clickhouse: ClickHouseClientInterface,
  cacheManager: QueryCacheManager,
  validator: ASTValidator = defaultASTValidator
) {
  // Structured Parametric Analytics Query
  app.post('/v1/analytics/query', async (req, reply) => {
    const traceCtx = extractOrGenerateTraceContext(req.headers as any);
    reply.header('X-Correlation-Id', traceCtx.correlationId);
    reply.header('X-Trace-Id', traceCtx.traceId);

    const tenantId = (req.headers['x-tenant-id'] as string) || req.tenantContext?.tenant_id;
    if (!tenantId) {
      return reply.status(401).send({
        type: 'https://nexusgrid.io/errors/unauthorized',
        title: 'Unauthorized',
        status: 401,
        detail: 'Missing authenticated tenant context or X-Tenant-Id header',
        code: 'ERR_UNAUTHORIZED',
        trace_id: traceCtx.traceId,
      });
    }

    const validation = validator.validate(req.body, tenantId);
    if (!validation.valid || !validation.params) {
      return reply.status(400).send({
        type: 'https://nexusgrid.io/errors/invalid-query',
        title: 'Invalid Analytical Query',
        status: 400,
        detail: validation.error,
        code: validation.code || 'ERR_INVALID_QUERY',
        trace_id: traceCtx.traceId,
      });
    }

    const params = validation.params;

    // Execute with Stampede-Protected Cache Layer
    const result = await cacheManager.executeWithCache(params, async () => {
      return await clickhouse.query(params);
    });

    if (result.meta.fromCache) {
      reply.header('X-Cache-Lookup', 'HIT');
    } else {
      reply.header('X-Cache-Lookup', 'MISS');
    }

    return reply.status(200).send({
      status: 'SUCCESS',
      data: result.data,
      meta: {
        ...result.meta,
        correlation_id: traceCtx.correlationId,
      },
    });
  });

  // Overview Summary Metric Endpoint
  app.get('/v1/analytics/overview', async (req, reply) => {
    const traceCtx = extractOrGenerateTraceContext(req.headers as any);
    const tenantId = (req.headers['x-tenant-id'] as string) || req.tenantContext?.tenant_id;
    const query = req.query as { project_id?: string };

    if (!tenantId) {
      return reply.status(401).send({
        type: 'https://nexusgrid.io/errors/unauthorized',
        title: 'Unauthorized',
        status: 401,
        detail: 'Missing authenticated tenant context',
        code: 'ERR_UNAUTHORIZED',
        trace_id: traceCtx.traceId,
      });
    }

    const count = await clickhouse.count(tenantId, query.project_id);
    return reply.status(200).send({
      tenant_id: tenantId,
      project_id: query.project_id || 'all',
      total_events: count,
      correlation_id: traceCtx.correlationId,
    });
  });
}
