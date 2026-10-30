import type { FastifyRequest, FastifyReply } from 'fastify';
import type { AuthService } from '../services/auth.service.js';
import type { TenantContext } from '@nexusgrid/contracts';

declare module 'fastify' {
  interface FastifyRequest {
    tenantContext?: TenantContext;
  }
}

export function createAuthMiddleware(authService: AuthService) {
  return async function authMiddleware(req: FastifyRequest, reply: FastifyReply) {
    const authHeader = req.headers['authorization'];
    const apiKeyHeader = req.headers['x-api-key'];

    let token = '';
    if (typeof apiKeyHeader === 'string' && apiKeyHeader.length > 0) {
      token = apiKeyHeader.trim();
    } else if (typeof authHeader === 'string' && authHeader.startsWith('Bearer ')) {
      token = authHeader.substring(7).trim();
    }

    if (!token) {
      return reply.status(401).send({
        type: 'https://nexusgrid.io/errors/unauthorized',
        title: 'Unauthorized',
        status: 401,
        detail: 'Missing API Key in Authorization Bearer or X-API-Key header',
        code: 'ERR_UNAUTHORIZED',
        trace_id: (req.headers['x-trace-id'] as string) || 'trace_default',
      });
    }

    const context = await authService.authenticateKey(token);
    if (!context) {
      return reply.status(401).send({
        type: 'https://nexusgrid.io/errors/unauthorized',
        title: 'Unauthorized',
        status: 401,
        detail: 'Invalid, revoked, or expired API Key provided',
        code: 'ERR_INVALID_KEY',
        trace_id: (req.headers['x-trace-id'] as string) || 'trace_default',
      });
    }

    req.tenantContext = context;
  };
}

