import { describe, it, expect } from 'vitest';
import {
  loadConfig,
  CommonEnvSchema,
  IngestionServiceEnvSchema,
  AuthServiceEnvSchema,
  KafkaEnvSchema,
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
});

