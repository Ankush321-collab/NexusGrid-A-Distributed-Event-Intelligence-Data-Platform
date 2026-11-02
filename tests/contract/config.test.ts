import { describe, it, expect } from 'vitest';
import {
  loadConfig,
  CommonEnvSchema,
  IngestionServiceEnvSchema,
  AuthServiceEnvSchema,
  KafkaEnvSchema,
  QueryServiceEnvSchema,
  AlertServiceEnvSchema,
  EventProcessorEnvSchema,
  OutboxPublisherEnvSchema,
  MinioEnvSchema,
} from '../../packages/config/src/index.js';

describe('Phase 0 - Validated 12-Factor Environment Configurations', () => {
  it('loads valid configuration with defaults', () => {
    const config = loadConfig(CommonEnvSchema, {});
    expect(config.NODE_ENV).toBe('development');
    expect(config.PORT).toBe(3000);
    expect(config.LOG_LEVEL).toBe('info');
  });

  it('correctly parses and coerces types in ingestion service config', () => {
    const customEnv = {
      NODE_ENV: 'production',
      PORT: '9000',
      RATE_LIMIT_MAX_TOKENS: '500',
      MAX_BATCH_EVENTS: '250',
      KAFKA_BROKERS: 'kafka-node-1:9092,kafka-node-2:9092',
    };

    const config = loadConfig(IngestionServiceEnvSchema, customEnv);
    expect(config.PORT).toBe(9000);
    expect(config.RATE_LIMIT_MAX_TOKENS).toBe(500);
    expect(config.MAX_BATCH_EVENTS).toBe(250);
    expect(config.KAFKA_BROKERS).toBe('kafka-node-1:9092,kafka-node-2:9092');
  });

  it('fails fast on invalid environment configuration', () => {
    const badEnv = {
      PORT: '-10', // invalid port
    };

    expect(() => loadConfig(IngestionServiceEnvSchema, badEnv)).toThrow(
      /Invalid environment configuration/
    );
  });

  it('validates Kafka and Postgres topic and database defaults', () => {
    const kafkaConfig = loadConfig(KafkaEnvSchema, {});
    expect(kafkaConfig.KAFKA_RAW_TOPIC).toBe('nexusgrid.events.raw');
    expect(kafkaConfig.KAFKA_DLQ_TOPIC).toBe('nexusgrid.events.dlq');

    const authConfig = loadConfig(AuthServiceEnvSchema, {});
    expect(authConfig.POSTGRES_DB).toBe('nexusgrid');
    expect(authConfig.POSTGRES_PORT).toBe(5432);
  });

  it('validates Query, Alert, EventProcessor, Outbox, and MinIO schemas', () => {
    const queryConfig = loadConfig(QueryServiceEnvSchema, {});
    expect(queryConfig.PORT).toBe(8083);
    expect(queryConfig.QUERY_MAX_SCAN_DAYS).toBe(90);
    expect(queryConfig.CACHE_TTL_SEC).toBe(60);

    const alertConfig = loadConfig(AlertServiceEnvSchema, {});
    expect(alertConfig.PORT).toBe(8084);
    expect(alertConfig.ALERT_EVAL_INTERVAL_MS).toBe(10000);
    expect(alertConfig.WEBHOOK_SIGNING_SECRET).toBe('nexusgrid_webhook_secret_key_v1');

    const epConfig = loadConfig(EventProcessorEnvSchema, {});
    expect(epConfig.PORT).toBe(8082);
    expect(epConfig.BATCH_SIZE_ITEMS).toBe(5000);
    expect(epConfig.BATCH_TIMEOUT_MS).toBe(500);
    expect(epConfig.ENABLE_S3_ARCHIVE).toBe(true);

    const outboxConfig = loadConfig(OutboxPublisherEnvSchema, {});
    expect(outboxConfig.PORT).toBe(8085);
    expect(outboxConfig.OUTBOX_POLL_INTERVAL_MS).toBe(500);
    expect(outboxConfig.OUTBOX_BATCH_SIZE).toBe(100);

    const minioConfig = loadConfig(MinioEnvSchema, {});
    expect(minioConfig.MINIO_PORT).toBe(9002);
    expect(minioConfig.MINIO_BUCKET).toBe('nexusgrid-archives');
  });
});

