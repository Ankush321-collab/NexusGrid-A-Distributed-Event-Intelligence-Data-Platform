import type { FastifyRequest, FastifyReply } from 'fastify';
import type { InMemoryRedisClient } from '@nexusgrid/database';

export function createRateLimiter(
  redis: InMemoryRedisClient,
  maxTokens: number = 100,
  refillRate: number = 10
) {
  return async function rateLimitMiddleware(req: FastifyRequest, reply: FastifyReply) {
    // Determine tenant identity (from authenticated context or header/ip)
    const tenantId = req.tenantContext?.tenant_id || (req.headers['x-tenant-id'] as string) || 'anonymous';
    const cost = 1;

    const result = await redis.evalTokenBucket(tenantId, maxTokens, refillRate, cost);

    reply.header('X-RateLimit-Limit', maxTokens.toString());
    reply.header('X-RateLimit-Remaining', Math.max(0, result.remainingTokens).toString());

    if (!result.allowed) {
      reply.header('Retry-After', (result.retryAfterSec || 1).toString());
      return reply.status(429).send({
        type: 'https://nexusgrid.io/errors/too-many-requests',
        title: 'Too Many Requests',
        status: 429,
        detail: `Rate limit exceeded for tenant "${tenantId}". Please retry after ${result.retryAfterSec} seconds.`,
        code: 'ERR_RATE_LIMIT_EXCEEDED',
        trace_id: (req.headers['x-trace-id'] as string) || 'trace_default',
      });
    }
  };
}

