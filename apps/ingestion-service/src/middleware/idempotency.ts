import type { FastifyRequest, FastifyReply } from 'fastify';
import type { InMemoryRedisClient } from '@nexusgrid/database';

declare module 'fastify' {
  interface FastifyRequest {
    idempotencyKey?: string;
  }
}

export function createIdempotencyMiddleware(redis: InMemoryRedisClient) {
  return async function idempotencyMiddleware(req: FastifyRequest, reply: FastifyReply) {
    const rawKey = req.headers['idempotency-key'];
    if (!rawKey || typeof rawKey !== 'string') {
      return; // No idempotency key provided
    }

    const tenantId = req.tenantContext?.tenant_id || (req.headers['x-tenant-id'] as string) || 'default';
    const key = rawKey.trim();
    req.idempotencyKey = key;

    const lock = await redis.acquireIdempotencyLock(tenantId, key);

    if (lock.status === 'COMPLETED') {
      reply.header('X-Cache-Lookup', 'HIT');
      reply.header('Idempotency-Key', key);
      return reply.status(lock.cachedResponse.statusCode).send(lock.cachedResponse.body);
    }

    if (lock.status === 'IN_PROGRESS') {
      return reply.status(409).send({
        type: 'https://nexusgrid.io/errors/conflict',
        title: 'Conflict',
        status: 409,
        detail: `A request with idempotency key "${key}" is currently in progress. Please retry shortly.`,
        code: 'ERR_CONCURRENT_IDEMPOTENT_REQUEST',
        trace_id: (req.headers['x-trace-id'] as string) || 'trace_default',
      });
    }

    // ACQUIRED: Hook into send to cache successful 202 outcome
    const originalSend = reply.send.bind(reply);
    reply.send = function (payload: any) {
      if (reply.statusCode >= 200 && reply.statusCode < 300) {
        let bodyToCache = payload;
        try {
          if (typeof payload === 'string') {
            bodyToCache = JSON.parse(payload);
          }
        } catch {
          bodyToCache = payload;
        }

        redis.completeIdempotency(tenantId, key, {
          statusCode: reply.statusCode,
          body: bodyToCache,
        }).catch(() => {});
      }
      return originalSend(payload);
    };
  };
}

