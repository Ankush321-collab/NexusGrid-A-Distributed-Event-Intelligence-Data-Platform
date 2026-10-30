import type { FastifyInstance } from 'fastify';
import type { AuthService } from '../services/auth.service.js';
import type { InMemoryPostgresAdapter } from '@nexusgrid/database';
import { createAuthMiddleware } from '../middleware/auth.middleware.js';

export function registerAuthRoutes(app: FastifyInstance, authService: AuthService, db: InMemoryPostgresAdapter) {
  const requireAuth = createAuthMiddleware(authService);

  // Tenant creation
  app.post('/v1/auth/tenants', async (req, reply) => {
    const body = req.body as { id?: string; name?: string };
    if (!body?.id || !body?.name) {
      return reply.status(400).send({
        type: 'https://nexusgrid.io/errors/bad-request',
        title: 'Bad Request',
        status: 400,
        detail: 'Tenant "id" and "name" are required',
        code: 'ERR_INVALID_BODY',
        trace_id: (req.headers['x-trace-id'] as string) || 'trace_default',
      });
    }

    try {
      const tenant = await authService.createTenant(body.id, body.name);
      return reply.status(201).send(tenant);
    } catch (err: any) {
      return reply.status(409).send({
        type: 'https://nexusgrid.io/errors/conflict',
        title: 'Conflict',
        status: 409,
        detail: err.message,
        code: 'ERR_TENANT_EXISTS',
        trace_id: (req.headers['x-trace-id'] as string) || 'trace_default',
      });
    }
  });

  // Project creation
  app.post('/v1/auth/projects', async (req, reply) => {
    const body = req.body as { tenant_id: string; id: string; name: string; environment?: 'dev' | 'staging' | 'prod' };
    if (!body?.tenant_id || !body?.id || !body?.name) {
      return reply.status(400).send({
        type: 'https://nexusgrid.io/errors/bad-request',
        title: 'Bad Request',
        status: 400,
        detail: 'tenant_id, id, and name are required',
        code: 'ERR_INVALID_BODY',
        trace_id: (req.headers['x-trace-id'] as string) || 'trace_default',
      });
    }

    try {
      const project = await authService.createProject(body.tenant_id, body.id, body.name, body.environment);
      return reply.status(201).send(project);
    } catch (err: any) {
      return reply.status(400).send({
        type: 'https://nexusgrid.io/errors/project-creation-failed',
        title: 'Project Creation Failed',
        status: 400,
        detail: err.message,
        code: 'ERR_PROJECT_FAILED',
        trace_id: (req.headers['x-trace-id'] as string) || 'trace_default',
      });
    }
  });

  // API Key generation
  app.post('/v1/auth/keys', async (req, reply) => {
    const body = req.body as { tenant_id: string; project_id: string; name: string; scopes?: string[] };
    if (!body?.tenant_id || !body?.project_id || !body?.name) {
      return reply.status(400).send({
        type: 'https://nexusgrid.io/errors/bad-request',
        title: 'Bad Request',
        status: 400,
        detail: 'tenant_id, project_id, and name are required',
        code: 'ERR_INVALID_BODY',
        trace_id: (req.headers['x-trace-id'] as string) || 'trace_default',
      });
    }

    try {
      const result = await authService.createApiKey({
        tenantId: body.tenant_id,
        projectId: body.project_id,
        name: body.name,
        scopes: body.scopes,
      });

      return reply.status(201).send({
        id: result.apiKey.id,
        tenant_id: result.apiKey.tenant_id,
        project_id: result.apiKey.project_id,
        name: result.apiKey.name,
        key_prefix: result.apiKey.key_prefix,
        plaintext_key: result.plaintextKey, // Return plaintext ONCE
        scopes: result.apiKey.scopes,
        created_at: result.apiKey.created_at,
      });
    } catch (err: any) {
      return reply.status(403).send({
        type: 'https://nexusgrid.io/errors/forbidden',
        title: 'Forbidden',
        status: 403,
        detail: err.message,
        code: 'ERR_FORBIDDEN_KEY_CREATION',
        trace_id: (req.headers['x-trace-id'] as string) || 'trace_default',
      });
    }
  });

  // Revoke API Key
  app.delete('/v1/auth/keys/:id', { preHandler: [requireAuth] }, async (req, reply) => {
    const { id } = req.params as { id: string };
    const tenantId = req.tenantContext!.tenant_id;

    const revoked = await authService.revokeApiKey(tenantId, id, req.tenantContext!.key_id);
    if (!revoked) {
      return reply.status(404).send({
        type: 'https://nexusgrid.io/errors/not-found',
        title: 'Not Found',
        status: 404,
        detail: `API key "${id}" not found under tenant "${tenantId}"`,
        code: 'ERR_KEY_NOT_FOUND',
        trace_id: (req.headers['x-trace-id'] as string) || 'trace_default',
      });
    }

    return reply.status(200).send({ success: true, revoked_id: id });
  });

  // Cursor-paginated audit logs (tenant isolated)
  app.get('/v1/auth/audit-logs', { preHandler: [requireAuth] }, async (req, reply) => {
    const query = req.query as { cursor?: string; limit?: string };
    const tenantId = req.tenantContext!.tenant_id;
    const cursor = query.cursor ? parseInt(query.cursor, 10) : undefined;
    const limit = query.limit ? Math.min(100, parseInt(query.limit, 10)) : 20;

    const result = await db.listAuditLogs(tenantId, cursor, limit);
    return reply.status(200).send(result);
  });
}

