import { z } from 'zod';

export const CommonEnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'staging', 'production']).default('development'),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
  SERVICE_NAME: z.string().default('nexusgrid-service'),
  PORT: z.coerce.number().int().positive().default(3000),
  HOST: z.string().default('0.0.0.0'),
});

export const DatabaseEnvSchema = CommonEnvSchema.extend({
  POSTGRES_HOST: z.string().default('localhost'),
  POSTGRES_PORT: z.coerce.number().default(5432),
  POSTGRES_DB: z.string().default('nexusgrid'),
  POSTGRES_USER: z.string().default('nexusgrid'),
  POSTGRES_PASSWORD: z.string().default('nexusgrid_secure_pwd'),
  POSTGRES_MAX_CONNECTIONS: z.coerce.number().default(20),
  POSTGRES_IDLE_TIMEOUT_MS: z.coerce.number().default(30000),
});

export const RedisEnvSchema = CommonEnvSchema.extend({
  REDIS_HOST: z.string().default('localhost'),
  REDIS_PORT: z.coerce.number().default(6379),
  REDIS_PASSWORD: z.string().optional(),
  REDIS_KEY_PREFIX: z.string().default('ng:'),
  RATE_LIMIT_MAX_TOKENS: z.coerce.number().default(100),
  RATE_LIMIT_REFILL_RATE: z.coerce.number().default(10), // tokens per second
  IDEMPOTENCY_TTL_SEC: z.coerce.number().default(86400), // 24 hours
});

export const KafkaEnvSchema = CommonEnvSchema.extend({
  KAFKA_BROKERS: z.string().default('localhost:9092'),
  KAFKA_CLIENT_ID: z.string().default('nexusgrid-client'),
  KAFKA_CONSUMER_GROUP: z.string().default('nexusgrid-workers-v1'),
  KAFKA_RAW_TOPIC: z.string().default('nexusgrid.events.raw'),
  KAFKA_DLQ_TOPIC: z.string().default('nexusgrid.events.dlq'),
  KAFKA_RETRY_10S_TOPIC: z.string().default('nexusgrid.events.retry.10s'),
  KAFKA_RETRY_1M_TOPIC: z.string().default('nexusgrid.events.retry.1m'),
  KAFKA_RETRY_5M_TOPIC: z.string().default('nexusgrid.events.retry.5m'),
  KAFKA_MAX_RETRIES: z.coerce.number().default(5),
});

export const ClickHouseEnvSchema = CommonEnvSchema.extend({
  CLICKHOUSE_HOST: z.string().default('http://localhost:8123'),
  CLICKHOUSE_DB: z.string().default('nexusgrid'),
  CLICKHOUSE_USER: z.string().default('default'),
  CLICKHOUSE_PASSWORD: z.string().default(''),
});

export const IngestionServiceEnvSchema = CommonEnvSchema.merge(RedisEnvSchema).merge(KafkaEnvSchema).extend({
  SERVICE_NAME: z.string().default('ingestion-service'),
  PORT: z.coerce.number().int().positive().default(8080),
  MAX_PAYLOAD_BYTES: z.coerce.number().int().positive().default(1048576), // 1MB
  MAX_BATCH_EVENTS: z.coerce.number().int().positive().default(500),
});

export const AuthServiceEnvSchema = CommonEnvSchema.merge(DatabaseEnvSchema).extend({
  SERVICE_NAME: z.string().default('auth-service'),
  PORT: z.coerce.number().int().positive().default(8081),
  API_KEY_SALT_ROUNDS: z.coerce.number().int().positive().default(12),
});

export const EventProcessorEnvSchema = CommonEnvSchema.merge(KafkaEnvSchema).merge(ClickHouseEnvSchema).extend({
  SERVICE_NAME: z.string().default('event-processor-worker'),
  PORT: z.coerce.number().int().positive().default(8082),
  BATCH_SIZE_ITEMS: z.coerce.number().int().positive().default(5000),
  BATCH_TIMEOUT_MS: z.coerce.number().int().positive().default(500),
});

export type IngestionConfig = z.infer<typeof IngestionServiceEnvSchema>;
export type AuthConfig = z.infer<typeof AuthServiceEnvSchema>;
export type EventProcessorConfig = z.infer<typeof EventProcessorEnvSchema>;

export function loadConfig<T extends z.ZodTypeAny>(schema: T, env: Record<string, string | undefined> = process.env): z.infer<T> {
  const result = schema.safeParse(env);
  if (!result.success) {
    const formatted = result.error.errors.map((e) => `[${e.path.join('.')}]: ${e.message}`).join(', ');
    throw new Error(`Invalid environment configuration: ${formatted}`);
  }
  return result.data;
}
