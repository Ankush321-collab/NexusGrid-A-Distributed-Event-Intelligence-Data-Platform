import crypto from 'crypto';

export interface GeneratedApiKey {
  id: string;
  plaintextKey: string;
  prefix: string;
  hash: string;
}

export class CryptoService {
  private salt: string;

  constructor(salt: string = process.env.API_KEY_SALT || 'nexusgrid_static_salt_v1') {
    this.salt = salt;
  }

  /**
   * Generates a secure API key following the standard format:
   * ng_<environment>_<prefix8>_<secret32>
   */
  public generateApiKey(environment: 'dev' | 'staging' | 'prod' = 'dev'): GeneratedApiKey {
    const keyId = `key_${crypto.randomBytes(12).toString('hex')}`;
    const prefix = crypto.randomBytes(4).toString('hex'); // 8 characters
    const secret = crypto.randomBytes(16).toString('hex'); // 32 characters
    const plaintextKey = `ng_${environment}_${prefix}_${secret}`;
    const hash = this.hashSecret(secret);

    return {
      id: keyId,
      plaintextKey,
      prefix,
      hash,
    };
  }

  /**
   * Hashes the API key secret with SHA-256 and salt
   */
  public hashSecret(secret: string): string {
    return crypto.createHash('sha256').update(`${this.salt}:${secret}`).digest('hex');
  }

  /**
   * Constant-time timing-safe hash comparison to prevent timing attacks
   */
  public verifySecret(providedSecret: string, storedHash: string): boolean {
    const computedHash = this.hashSecret(providedSecret);
    const bufA = Buffer.from(computedHash, 'utf8');
    const bufB = Buffer.from(storedHash, 'utf8');
    if (bufA.length !== bufB.length) return false;
    return crypto.timingSafeEqual(bufA, bufB);
  }

  /**
   * Parses an API key string into its constituents
   */
  public parseApiKey(rawKey: string): { environment: string; prefix: string; secret: string } | null {
    const parts = rawKey.split('_');
    if (parts.length !== 4 || parts[0] !== 'ng') {
      return null;
    }
    return {
      environment: parts[1],
      prefix: parts[2],
      secret: parts[3],
    };
  }
}

export const defaultCryptoService = new CryptoService();

