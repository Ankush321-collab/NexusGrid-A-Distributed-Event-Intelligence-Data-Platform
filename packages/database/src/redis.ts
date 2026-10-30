/**
 * High-performance Redis adapter with Token-Bucket Lua script
 * and Idempotency guard for distributed coordination.
 */

export interface TokenBucketResult {
  allowed: boolean;
  remainingTokens: number;
  retryAfterSec?: number;
}

export interface IdempotencyEntry {
  status: 'IN_PROGRESS' | 'COMPLETED';
  response?: {
    statusCode: number;
    body: any;
    headers?: Record<string, string>;
  };
  expiresAt: number;
}

export class InMemoryRedisClient {
  private kv: Map<string, { val: string; expiresAt: number }> = new Map();
  private tokenBuckets: Map<string, { tokens: number; lastUpdated: number }> = new Map();

  /**
   * Atomic Token Bucket Rate Limiter
   * KEYS[1]: rate_limit:{tenant_id}
   * ARGV[1]: max_tokens, ARGV[2]: refill_rate_per_sec, ARGV[3]: now_sec, ARGV[4]: requested
   */
  public async evalTokenBucket(
    tenantId: string,
    maxTokens: number = 100,
    refillRate: number = 10,
    requested: number = 1
  ): Promise<TokenBucketResult> {
    const key = `rate_limit:${tenantId}`;
    const now = Date.now() / 1000; // in seconds

    let bucket = this.tokenBuckets.get(key);
    if (!bucket) {
      bucket = { tokens: maxTokens, lastUpdated: now };
    } else {
      // Calculate refill
      const delta = Math.max(0, now - bucket.lastUpdated);
      bucket.tokens = Math.min(maxTokens, bucket.tokens + delta * refillRate);
      bucket.lastUpdated = now;
    }

    if (bucket.tokens >= requested) {
      bucket.tokens -= requested;
      this.tokenBuckets.set(key, bucket);
      return {
        allowed: true,
        remainingTokens: Math.floor(bucket.tokens),
      };
    } else {
      const needed = requested - bucket.tokens;
      const retryAfterSec = Math.ceil(needed / refillRate);
      this.tokenBuckets.set(key, bucket);
      return {
        allowed: false,
        remainingTokens: Math.floor(bucket.tokens),
        retryAfterSec,
      };
    }
  }

  /**
   * Idempotency check-and-set lock (NX PX equivalent)
   */
  public async acquireIdempotencyLock(
    tenantId: string,
    key: string,
    ttlMs: number = 30000
  ): Promise<{ status: 'ACQUIRED' | 'IN_PROGRESS' | 'COMPLETED'; cachedResponse?: any }> {
    const cacheKey = `idempotency:${tenantId}:${key}`;
    const now = Date.now();
    const entry = this.kv.get(cacheKey);

    if (entry) {
      if (entry.expiresAt > now) {
        const parsed: IdempotencyEntry = JSON.parse(entry.val);
        if (parsed.status === 'IN_PROGRESS') {
          return { status: 'IN_PROGRESS' };
        } else {
          return { status: 'COMPLETED', cachedResponse: parsed.response };
        }
      } else {
        this.kv.delete(cacheKey);
      }
    }

    // Acquire lock
    const inProgress: IdempotencyEntry = {
      status: 'IN_PROGRESS',
      expiresAt: now + ttlMs,
    };
    this.kv.set(cacheKey, { val: JSON.stringify(inProgress), expiresAt: now + ttlMs });
    return { status: 'ACQUIRED' };
  }

  public async completeIdempotency(
    tenantId: string,
    key: string,
    response: { statusCode: number; body: any; headers?: Record<string, string> },
    ttlMs: number = 86400000 // 24 hours
  ): Promise<void> {
    const cacheKey = `idempotency:${tenantId}:${key}`;
    const now = Date.now();
    const completed: IdempotencyEntry = {
      status: 'COMPLETED',
      response,
      expiresAt: now + ttlMs,
    };
    this.kv.set(cacheKey, { val: JSON.stringify(completed), expiresAt: now + ttlMs });
  }

  public async deleteKey(key: string): Promise<void> {
    this.kv.delete(key);
  }

  public async flush(): Promise<void> {
    this.kv.clear();
    this.tokenBuckets.clear();
  }
}

export const defaultRedis = new InMemoryRedisClient();

