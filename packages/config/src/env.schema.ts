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
  POSTGRES_PORT: z.coerce.number().int().positive().default(5432),
  POSTGRES_DB: z.string().default('nexusgrid'),
  POSTGRES_USER: z.string().default('nexusgrid'),
  POSTGRES_PASSWORD: z.string().default('nexusgrid_secure_pwd'),
  POSTGRES_MAX_CONNECTIONS: z.coerce.number().int().positive().default(20),
  POSTGRES_IDLE_TIMEOUT_MS: z.coerce.number().int().positive().default(30000),
});

export const RedisEnvSchema = CommonEnvSchema.extend({
  REDIS_HOST: z.string().default('localhost'),
  REDIS_PORT: z.coerce.number().int().positive().default(6379),
  REDIS_PASSWORD: z.string().optional(),
  REDIS_KEY_PREFIX: z.string().default('ng:'),
  RATE_LIMIT_MAX_TOKENS: z.coerce.number().int().positive().default(100),
  RATE_LIMIT_REFILL_RATE: z.coerce.number().int().positive().default(10), // tokens per second
  IDEMPOTENCY_TTL_SEC: z.coerce.number().int().positive().default(86400), // 24 hours
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
  KAFKA_MAX_RETRIES: z.coerce.number().int().positive().default(5),
});

export const ClickHouseEnvSchema = CommonEnvSchema.extend({
  CLICKHOUSE_HOST: z.string().default('http://localhost:8123'),
  CLICKHOUSE_DB: z.string().default('nexusgrid'),
  CLICKHOUSE_USER: z.string().default('default'),
  CLICKHOUSE_PASSWORD: z.string().default(''),
});

export const MinioEnvSchema = CommonEnvSchema.extend({
  MINIO_ENDPOINT: z.string().default('localhost'),
  MINIO_PORT: z.coerce.number().int().positive().default(9002),
  MINIO_ACCESS_KEY: z.string().default('minioadmin'),
  MINIO_SECRET_KEY: z.string().default('minioadmin_secure_pwd'),
  MINIO_BUCKET: z.string().default('nexusgrid-archives'),
  MINIO_USE_SSL: z.coerce.boolean().default(false),
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
  API_KEY_SALT: z.string().default('nexusgrid_static_salt_v1'),
  API_KEY_SALT_ROUNDS: z.coerce.number().int().positive().default(12),
});

export const EventProcessorEnvSchema = CommonEnvSchema.merge(KafkaEnvSchema).merge(ClickHouseEnvSchema).merge(MinioEnvSchema).extend({
  SERVICE_NAME: z.string().default('event-processor-worker'),
  PORT: z.coerce.number().int().positive().default(8082),
  BATCH_SIZE_ITEMS: z.coerce.number().int().positive().default(5000),
  BATCH_TIMEOUT_MS: z.coerce.number().int().positive().default(500),
  ENABLE_S3_ARCHIVE: z.coerce.boolean().default(true),
});

export const QueryServiceEnvSchema = CommonEnvSchema.merge(RedisEnvSchema).merge(ClickHouseEnvSchema).extend({
  SERVICE_NAME: z.string().default('query-service'),
  PORT: z.coerce.number().int().positive().default(8083),
  QUERY_MAX_SCAN_DAYS: z.coerce.number().int().positive().default(90),
  QUERY_TIMEOUT_MS: z.coerce.number().int().positive().default(10000),
  CACHE_TTL_SEC: z.coerce.number().int().positive().default(60),
});

export const AlertServiceEnvSchema = CommonEnvSchema.merge(DatabaseEnvSchema).merge(ClickHouseEnvSchema).merge(RedisEnvSchema).extend({
  SERVICE_NAME: z.string().default('alert-service'),
  PORT: z.coerce.number().int().positive().default(8084),
  ALERT_EVAL_INTERVAL_MS: z.coerce.number().int().positive().default(10000),
  ALERT_WEBHOOK_TIMEOUT_MS: z.coerce.number().int().positive().default(5000),
  WEBHOOK_SIGNING_SECRET: z.string().default('nexusgrid_webhook_secret_key_v1'),
});

export const OutboxPublisherEnvSchema = CommonEnvSchema.merge(DatabaseEnvSchema).merge(KafkaEnvSchema).extend({
  SERVICE_NAME: z.string().default('outbox-publisher-worker'),
  PORT: z.coerce.number().int().positive().default(8085),
  OUTBOX_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(500),
  OUTBOX_BATCH_SIZE: z.coerce.number().int().positive().default(100),
});

export type CommonConfig = z.infer<typeof CommonEnvSchema>;
export type DatabaseConfig = z.infer<typeof DatabaseEnvSchema>;
export type RedisConfig = z.infer<typeof RedisEnvSchema>;
export type KafkaConfig = z.infer<typeof KafkaEnvSchema>;
export type ClickHouseConfig = z.infer<typeof ClickHouseEnvSchema>;
export type MinioConfig = z.infer<typeof MinioEnvSchema>;
export type IngestionConfig = z.infer<typeof IngestionServiceEnvSchema>;
export type AuthConfig = z.infer<typeof AuthServiceEnvSchema>;
export type EventProcessorConfig = z.infer<typeof EventProcessorEnvSchema>;
export type QueryConfig = z.infer<typeof QueryServiceEnvSchema>;
export type AlertConfig = z.infer<typeof AlertServiceEnvSchema>;
export type OutboxConfig = z.infer<typeof OutboxPublisherEnvSchema>;

export function loadConfig<T extends z.ZodTypeAny>(schema: T, env: Record<string, string | undefined> = process.env): z.infer<T> {
  const result = schema.safeParse(env);
  if (!result.success) {
    const formatted = result.error.errors.map((e) => `[${e.path.join('.')}]: ${e.message}`).join(', ');
    throw new Error(`Invalid environment configuration: ${formatted}`);
  }
  return result.data;
}
