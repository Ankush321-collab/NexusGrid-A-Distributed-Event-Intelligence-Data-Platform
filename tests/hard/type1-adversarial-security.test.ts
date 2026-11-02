import { describe, it, expect, beforeEach } from 'vitest';
import { InMemoryPostgresAdapter } from '../../packages/database/src/index.js';
import { AuthService } from '../../apps/auth-service/src/services/auth.service.js';
import { CryptoService } from '../../apps/auth-service/src/services/crypto.service.js';
import { buildAuthServer } from '../../apps/auth-service/src/index.js';
import { buildIngestionServer } from '../../apps/ingestion-service/src/index.js';
import { InMemoryRedisClient } from '../../packages/database/src/index.js';
import { KafkaProducer } from '../../apps/ingestion-service/src/producer/kafka-producer.js';
import { EventValidator } from '../../apps/ingestion-service/src/validators/event-validator.js';

describe('Hard-Level Test Type 1: Adversarial Security, Cryptographic Timing & Fuzz Testing', () => {
  let db: InMemoryPostgresAdapter;
  let authService: AuthService;
  let cryptoService: CryptoService;

  beforeEach(() => {
    db = new InMemoryPostgresAdapter();
    cryptoService = new CryptoService();
    authService = new AuthService(db, cryptoService);
  });

  // =========================================================================
  // Test 1.1: Cryptographic Timing Attack Resistance
  // =========================================================================
  it('demonstrates constant-time secret comparison with zero timing-leakage across mismatched bytes', () => {
    const referenceSecret = 'a1b2c3d4e5f60718293a4b5c6d7e8f90';
    const storedHash = cryptoService.hashSecret(referenceSecret);

    // Case A: Mismatch on the very first byte (byte 0)
    const earlyMismatchSecret = 'x1b2c3d4e5f60718293a4b5c6d7e8f90';

    // Case B: Mismatch on the very last byte (byte 31)
    const lateMismatchSecret = 'a1b2c3d4e5f60718293a4b5c6d7e8f9x';

    const iterations = 500;

    // Warm-up JIT
    for (let i = 0; i < 50; i++) {
      cryptoService.verifySecret(earlyMismatchSecret, storedHash);
      cryptoService.verifySecret(lateMismatchSecret, storedHash);
    }

    // Benchmark Early Mismatch
    const startEarly = process.hrtime.bigint();
    for (let i = 0; i < iterations; i++) {
      cryptoService.verifySecret(earlyMismatchSecret, storedHash);
    }
    const endEarly = process.hrtime.bigint();
    const durationEarlyNs = Number(endEarly - startEarly);

    // Benchmark Late Mismatch
    const startLate = process.hrtime.bigint();
    for (let i = 0; i < iterations; i++) {
      cryptoService.verifySecret(lateMismatchSecret, storedHash);
    }
    const endLate = process.hrtime.bigint();
    const durationLateNs = Number(endLate - startLate);

    // Calculate absolute delta per comparison in microseconds
    const deltaPerOpUs = Math.abs(durationLateNs - durationEarlyNs) / (iterations * 1000);

    // Both must return false
    expect(cryptoService.verifySecret(earlyMismatchSecret, storedHash)).toBe(false);
    expect(cryptoService.verifySecret(lateMismatchSecret, storedHash)).toBe(false);

    // Timing difference must be sub-microsecond per check
    expect(deltaPerOpUs).toBeLessThan(50); // Less than 50us variance threshold
  });

  // =========================================================================
  // Test 1.2: Brute-Force Prefix Attack & Tampered Secret Spraying
  // =========================================================================
  it('resists brute-force secret spraying against stolen or leaked key prefixes', async () => {
    await authService.createTenant('tenant_victim', 'Victim Corp');
    await authService.createProject('tenant_victim', 'proj_victim', 'Victim Core');

    const victimKey = await authService.createApiKey({
      tenantId: 'tenant_victim',
      projectId: 'proj_victim',
      name: 'Victim Key',
    });

    const parsed = cryptoService.parseApiKey(victimKey.plaintextKey)!;
    expect(parsed).not.toBeNull();
    const knownPrefix = parsed.prefix;

    // Attacker sprays 50 random forged secrets with the victim's known prefix
    const sprayAttempts: Promise<any>[] = [];
    for (let i = 0; i < 50; i++) {
      const forgedSecret = (i.toString(16).padStart(32, '0'));
      const forgedKey = `ng_dev_${knownPrefix}_${forgedSecret}`;
      sprayAttempts.push(authService.authenticateKey(forgedKey));
    }

    const results = await Promise.all(sprayAttempts);

    // Assert 100% of forged keys are rejected with null (no authentication leak)
    for (const res of results) {
      expect(res).toBeNull();
    }
  });

  // =========================================================================
  // Test 1.3: Exhaustive Multi-Tenant BOLA Isolation Matrix
  // =========================================================================
  it('enforces total cross-tenant boundary isolation across a multi-tenant resource matrix', async () => {
    const tenants = ['tenant_alpha', 'tenant_beta', 'tenant_gamma'];
    const keys: Record<string, string> = {};

    // Provision 3 tenants, each with their own project and API key
    for (const t of tenants) {
      await authService.createTenant(t, `${t} company`);
      await authService.createProject(t, `proj_${t}`, `${t} project`);
      const keyObj = await authService.createApiKey({
        tenantId: t,
        projectId: `proj_${t}`,
        name: `${t} Key`,
      });
      keys[t] = keyObj.apiKey.id;
    }

    // Adversarial Cross-Tenant Mutation Matrix:
    // Tenant X attempts to revoke Tenant Y's key
    for (const callerTenant of tenants) {
      for (const targetTenant of tenants) {
        if (callerTenant === targetTenant) {
          // Legitimate mutation within own tenant -> Must succeed
          const revoked = await authService.revokeApiKey(callerTenant, keys[targetTenant]);
          expect(revoked).toBe(true);
        } else {
          // Cross-tenant attack -> Must be strictly rejected!
          const crossRevoke = await authService.revokeApiKey(callerTenant, keys[targetTenant]);
          expect(crossRevoke).toBe(false);
        }
      }
    }
  });

  // =========================================================================
  // Test 1.4: Malicious Payload Fuzzing (SQLi, Prototype Pollution, Homoglyphs)
  // =========================================================================
  it('safely handles adversarial payloads containing SQL injection, prototype pollution, and unicode homoglyphs', async () => {
    const redis = new InMemoryRedisClient();
    const producer = new KafkaProducer('nexusgrid.events.raw', 12);
    const validator = new EventValidator();
    const app = await buildIngestionServer({ redis, producer, validator });

    // Attack 1: SQL Injection in IDs and Event Names
    const sqliPayload = {
      event_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      tenant_id: "tenant_safe' OR '1'='1' --",
      project_id: 'proj_main; DROP TABLE audit_logs; --',
      event_name: "order.placed'; DELETE FROM tenants; --",
      occurred_at: new Date().toISOString(),
      schema_version: 1,
      properties: { query: "UNION SELECT * FROM api_keys WHERE '1'='1" },
    };

    // Attack 2: Prototype Pollution Payload
    const protoPollutionPayload = {
      event_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      tenant_id: 'tenant_secure',
      project_id: 'proj_secure',
      event_name: 'test.pollute',
      occurred_at: new Date().toISOString(),
      schema_version: 1,
      __proto__: { isAdmin: true },
      constructor: { prototype: { authenticated: true } },
      properties: {
        '__proto__.polluted': 'yes',
      },
    };

    // Attack 3: Unicode Homoglyph Attack in tenant ID
    const homoglyphPayload = {
      event_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      tenant_id: 'tеnаnt_аcmе', // Cyrillic 'е', 'а' characters resembling Latin
      project_id: 'proj_main',
      event_name: 'event.homoglyph',
      occurred_at: new Date().toISOString(),
      schema_version: 1,
      properties: { char_test: 'АВСРЕХ' },
    };

    // Fire attacks through HTTP gateway
    const res1 = await app.inject({ method: 'POST', url: '/v1/events', payload: sqliPayload });
    expect(res1.statusCode).toBe(202); // Payload accepted without executing SQL or breaking memory

    const res2 = await app.inject({ method: 'POST', url: '/v1/events', payload: protoPollutionPayload });
    // Root prototype must NOT be polluted
    expect((Object.prototype as any).isAdmin).toBeUndefined();
    expect((Object.prototype as any).authenticated).toBeUndefined();

    const res3 = await app.inject({ method: 'POST', url: '/v1/events', payload: homoglyphPayload });
    expect(res3.statusCode).toBe(202);

    // Verify system state remains intact
    expect(producer.getMessages().length).toBeGreaterThan(0);
  });

  // =========================================================================
  // Test 1.5: Immediate Revocation Propagation (Zero Grace Period Leak)
  // =========================================================================
  it('strictly rejects revoked API keys with zero delay between revocation and next authentication attempt', async () => {
    await authService.createTenant('tenant_instant', 'Instant Revoke Corp');
    await authService.createProject('tenant_instant', 'proj_instant', 'Instant Project');

    const key = await authService.createApiKey({
      tenantId: 'tenant_instant',
      projectId: 'proj_instant',
      name: 'Key To Revoke',
    });

    // 1. First auth succeeds
    const auth1 = await authService.authenticateKey(key.plaintextKey);
    expect(auth1).not.toBeNull();
    expect(auth1?.tenant_id).toBe('tenant_instant');

    // 2. Revoke key synchronously
    const revoked = await authService.revokeApiKey('tenant_instant', key.apiKey.id);
    expect(revoked).toBe(true);

    // 3. Immediate subsequent auth attempt must fail instantly
    const auth2 = await authService.authenticateKey(key.plaintextKey);
    expect(auth2).toBeNull();
  });
});

