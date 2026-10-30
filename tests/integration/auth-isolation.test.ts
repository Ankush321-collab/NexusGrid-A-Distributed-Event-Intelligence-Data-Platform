import { describe, it, expect, beforeEach } from 'vitest';
import { InMemoryPostgresAdapter } from '../../packages/database/src/index.js';
import { AuthService } from '../../apps/auth-service/src/services/auth.service.js';
import { defaultCryptoService } from '../../apps/auth-service/src/services/crypto.service.js';
import { buildAuthServer } from '../../apps/auth-service/src/index.js';

describe('Phase 1 - Tenant Identity, Scoped RBAC, and Transactional Metadata Isolation', () => {
  let db: InMemoryPostgresAdapter;
  let authService: AuthService;

  beforeEach(() => {
    db = new InMemoryPostgresAdapter();
    authService = new AuthService(db, defaultCryptoService);
  });

  it('provisions tenant, project, and API key with salted cryptographic hash', async () => {
    // 1. Create Tenant
    const tenant = await authService.createTenant('tenant_acme', 'Acme Corporation');
    expect(tenant.id).toBe('tenant_acme');
    expect(tenant.status).toBe('ACTIVE');

    // 2. Create Project
    const project = await authService.createProject('tenant_acme', 'proj_web', 'Web Tracking', 'prod');
    expect(project.id).toBe('proj_web');
    expect(project.tenant_id).toBe('tenant_acme');

    // 3. Create API Key
    const keyResult = await authService.createApiKey({
      tenantId: 'tenant_acme',
      projectId: 'proj_web',
      name: 'Production Ingest Key',
      scopes: ['events:write'],
    });

    expect(keyResult.plaintextKey.startsWith('ng_prod_')).toBe(true);
    expect(keyResult.apiKey.key_prefix).toHaveLength(8);
    // Hash stored at rest, NOT plaintext
    expect(keyResult.apiKey.key_hash).not.toBe(keyResult.plaintextKey);

    // 4. Authenticate using plaintext key
    const authContext = await authService.authenticateKey(keyResult.plaintextKey);
    expect(authContext).not.toBeNull();
    expect(authContext?.tenant_id).toBe('tenant_acme');
    expect(authContext?.project_id).toBe('proj_web');
    expect(authContext?.scopes).toContain('events:write');
  });

  it('rejects invalid secrets, tampered prefixes, and revoked keys', async () => {
    await authService.createTenant('tenant_alpha', 'Alpha Inc');
    await authService.createProject('tenant_alpha', 'proj_alpha', 'Alpha Main');
    const key = await authService.createApiKey({
      tenantId: 'tenant_alpha',
      projectId: 'proj_alpha',
      name: 'Key Alpha',
    });

    // 1. Tampered Secret
    const badSecretKey = key.plaintextKey.slice(0, -4) + 'ffff';
    const badAuth = await authService.authenticateKey(badSecretKey);
    expect(badAuth).toBeNull();

    // 2. Revoked Key
    const revoked = await authService.revokeApiKey('tenant_alpha', key.apiKey.id);
    expect(revoked).toBe(true);

    const revokedAuth = await authService.authenticateKey(key.plaintextKey);
    expect(revokedAuth).toBeNull();
  });

  it('enforces strict cross-tenant isolation (BOLA defense)', async () => {
    // Tenant A
    await authService.createTenant('tenant_a', 'Tenant A');
    await authService.createProject('tenant_a', 'proj_a', 'Project A');

    // Tenant B
    await authService.createTenant('tenant_b', 'Tenant B');
    await authService.createProject('tenant_b', 'proj_b', 'Project B');

    // Tenant A attempts to generate API key for Tenant B's project -> Must fail!
    await expect(
      authService.createApiKey({
        tenantId: 'tenant_a',
        projectId: 'proj_b', // Belongs to Tenant B!
        name: 'Malicious Key',
      })
    ).rejects.toThrow(/not found under tenant "tenant_a"/);

    // Tenant A cannot revoke Tenant B's key
    const keyB = await authService.createApiKey({
      tenantId: 'tenant_b',
      projectId: 'proj_b',
      name: 'Key B',
    });

    const crossRevoke = await authService.revokeApiKey('tenant_a', keyB.apiKey.id);
    expect(crossRevoke).toBe(false);
  });

  it('records transactional audit logs for security mutations and supports cursor pagination', async () => {
    await authService.createTenant('tenant_audit', 'Audit Corp');
    await authService.createProject('tenant_audit', 'proj_audit', 'Audit Project');

    // Generate 5 keys to populate audit log
    for (let i = 1; i <= 5; i++) {
      await authService.createApiKey({
        tenantId: 'tenant_audit',
        projectId: 'proj_audit',
        name: `Key ${i}`,
      });
    }

    // Query audit log with limit = 2
    const page1 = await db.listAuditLogs('tenant_audit', undefined, 2);
    expect(page1.items).toHaveLength(2);
    expect(page1.nextCursor).toBeDefined();

    // Page 2 using cursor
    const page2 = await db.listAuditLogs('tenant_audit', page1.nextCursor!, 2);
    expect(page2.items).toHaveLength(2);
    // Page 2 items must have smaller IDs than Page 1 items (DESC order)
    expect(page2.items[0].id).toBeLessThan(page1.items[1].id);
  });

  it('verifies index scans for high-frequency queries via EXPLAIN ANALYZE', async () => {
    const explainKeyLookup = await db.explain(
      'SELECT * FROM api_keys WHERE key_prefix = $1 AND tenant_id = $2'
    );
    expect(explainKeyLookup.scans[0].scanType).toBe('Index Scan');
    expect(explainKeyLookup.scans[0].indexName).toBe('idx_api_keys_lookup');

    const explainAuditCursor = await db.explain(
      'SELECT * FROM audit_logs WHERE tenant_id = $1 AND id < $2 ORDER BY id DESC LIMIT 20'
    );
    expect(explainAuditCursor.scans[0].scanType).toBe('Index Scan');
    expect(explainAuditCursor.scans[0].indexName).toBe('idx_audit_logs_tenant_cursor');
  });

  it('handles HTTP endpoints through Fastify Auth Service application', async () => {
    const app = await buildAuthServer();

    // 1. Create tenant via HTTP
    const createTenantRes = await app.inject({
      method: 'POST',
      url: '/v1/auth/tenants',
      payload: { id: 'tenant_http', name: 'HTTP Tenant' },
    });
    expect(createTenantRes.statusCode).toBe(201);

    // 2. Create project via HTTP
    const createProjRes = await app.inject({
      method: 'POST',
      url: '/v1/auth/projects',
      payload: { tenant_id: 'tenant_http', id: 'proj_http', name: 'HTTP Project', environment: 'dev' },
    });
    expect(createProjRes.statusCode).toBe(201);

    // 3. Create API key via HTTP
    const createKeyRes = await app.inject({
      method: 'POST',
      url: '/v1/auth/keys',
      payload: { tenant_id: 'tenant_http', project_id: 'proj_http', name: 'HTTP Key' },
    });
    expect(createKeyRes.statusCode).toBe(201);
    const keyData = JSON.parse(createKeyRes.body);
    expect(keyData.plaintext_key).toBeDefined();

    // 4. Access audit logs with valid API key
    const auditRes = await app.inject({
      method: 'GET',
      url: '/v1/auth/audit-logs',
      headers: { 'X-API-Key': keyData.plaintext_key },
    });
    expect(auditRes.statusCode).toBe(200);
    const auditData = JSON.parse(auditRes.body);
    expect(auditData.items.length).toBeGreaterThan(0);

    // 5. Access audit logs without key -> 401
    const unauthRes = await app.inject({
      method: 'GET',
      url: '/v1/auth/audit-logs',
    });
    expect(unauthRes.statusCode).toBe(401);
  });
});

