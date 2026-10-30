import { InMemoryPostgresAdapter, type ApiKeyRecord, type ProjectRecord, type TenantRecord } from '@nexusgrid/database';
import { CryptoService, defaultCryptoService, type GeneratedApiKey } from './crypto.service.js';
import type { TenantContext } from '@nexusgrid/contracts';

export class AuthService {
  constructor(
    private db: InMemoryPostgresAdapter,
    private crypto: CryptoService = defaultCryptoService
  ) {}

  public async createTenant(id: string, name: string): Promise<TenantRecord> {
    const tenant = await this.db.insertTenant({ id, name });
    await this.db.insertAuditLog({
      tenant_id: id,
      actor_id: 'system',
      action: 'TENANT_CREATED',
      resource_type: 'tenant',
      resource_id: id,
      metadata: { name },
    });
    return tenant;
  }

  public async createProject(
    tenantId: string,
    projectId: string,
    name: string,
    environment: 'dev' | 'staging' | 'prod' = 'dev',
    actorId: string = 'system'
  ): Promise<ProjectRecord> {
    const project = await this.db.insertProject({
      id: projectId,
      tenant_id: tenantId,
      name,
      environment,
    });

    await this.db.insertAuditLog({
      tenant_id: tenantId,
      actor_id: actorId,
      action: 'PROJECT_CREATED',
      resource_type: 'project',
      resource_id: projectId,
      metadata: { name, environment },
    });

    return project;
  }

  public async createApiKey(params: {
    tenantId: string;
    projectId: string;
    name: string;
    scopes?: string[];
    actorId?: string;
  }): Promise<{ apiKey: ApiKeyRecord; plaintextKey: string }> {
    const { tenantId, projectId, name, scopes = ['events:write'], actorId = 'system' } = params;

    // Verify tenant owns the project
    const project = await this.db.getProject(tenantId, projectId);
    if (!project) {
      throw new Error(`Project "${projectId}" not found under tenant "${tenantId}"`);
    }

    const gen = this.crypto.generateApiKey(project.environment);
    const apiKey = await this.db.insertApiKey({
      id: gen.id,
      tenant_id: tenantId,
      project_id: projectId,
      name,
      key_prefix: gen.prefix,
      key_hash: gen.hash,
      scopes,
    });

    // Write transactional audit record
    await this.db.insertAuditLog({
      tenant_id: tenantId,
      actor_id: actorId,
      action: 'API_KEY_CREATED',
      resource_type: 'api_key',
      resource_id: apiKey.id,
      metadata: { key_prefix: gen.prefix, project_id: projectId, scopes },
    });

    // Write transactional outbox record for Kafka propagation
    await this.db.insertOutboxRecord({
      aggregate_type: 'api_key',
      aggregate_id: apiKey.id,
      event_type: 'api_key.created',
      payload: {
        id: apiKey.id,
        tenant_id: tenantId,
        project_id: projectId,
        key_prefix: gen.prefix,
        scopes,
      },
    });

    return { apiKey, plaintextKey: gen.plaintextKey };
  }

  public async authenticateKey(rawKey: string): Promise<TenantContext | null> {
    const parsed = this.crypto.parseApiKey(rawKey);
    if (!parsed) return null;

    const candidates = await this.db.findApiKeysByPrefix(parsed.prefix);
    for (const key of candidates) {
      // Check revoked
      if (key.revoked_at) continue;

      // Check expired
      if (key.expires_at && new Date(key.expires_at) < new Date()) continue;

      // Verify cryptographic secret hash
      if (this.crypto.verifySecret(parsed.secret, key.key_hash)) {
        return {
          tenant_id: key.tenant_id,
          project_id: key.project_id,
          key_id: key.id,
          scopes: key.scopes,
        };
      }
    }
    return null;
  }

  public async revokeApiKey(tenantId: string, keyId: string, actorId: string = 'system'): Promise<boolean> {
    const success = await this.db.revokeApiKey(tenantId, keyId);
    if (success) {
      await this.db.insertAuditLog({
        tenant_id: tenantId,
        actor_id: actorId,
        action: 'API_KEY_REVOKED',
        resource_type: 'api_key',
        resource_id: keyId,
      });

      await this.db.insertOutboxRecord({
        aggregate_type: 'api_key',
        aggregate_id: keyId,
        event_type: 'api_key.revoked',
        payload: { id: keyId, tenant_id: tenantId },
      });
    }
    return success;
  }
}

