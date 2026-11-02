import Fastify from 'fastify';
import cors from '@fastify/cors';
import { defaultRedis } from '@nexusgrid/database';
import { defaultEventValidator, EventValidator } from './validators/event-validator.js';
import { defaultKafkaProducer, KafkaProducer } from './producer/kafka-producer.js';
import { createRateLimiter } from './rate-limit/rate-limiter.js';
import { createIdempotencyMiddleware } from './middleware/idempotency.js';
import { registerEventRoutes } from './routes/events.js';
import { defaultLogger } from '@nexusgrid/observability';
import { loadConfig, IngestionServiceEnvSchema } from '@nexusgrid/config';

export interface BuildIngestionServerOptions {
  redis?: typeof defaultRedis;
  validator?: EventValidator;
  producer?: KafkaProducer;
  maxTokens?: number;
  refillRate?: number;
}

export async function buildIngestionServer(opts: BuildIngestionServerOptions = {}) {
  const config = loadConfig(IngestionServiceEnvSchema);

  const app = Fastify({
    logger: false,
    bodyLimit: config.MAX_PAYLOAD_BYTES,
  });

  await app.register(cors, { origin: true });

  const redis = opts.redis || defaultRedis;
  const validator = opts.validator || new EventValidator({
    maxBatchSize: config.MAX_BATCH_EVENTS,
    maxPayloadBytes: config.MAX_PAYLOAD_BYTES,
  });
  const producer = opts.producer || new KafkaProducer(config.KAFKA_RAW_TOPIC);
  const maxTokens = opts.maxTokens ?? config.RATE_LIMIT_MAX_TOKENS;
  const refillRate = opts.refillRate ?? config.RATE_LIMIT_REFILL_RATE;

  // Rate Limiting Hook
  const rateLimiter = createRateLimiter(redis, maxTokens, refillRate);
  app.addHook('preHandler', rateLimiter);

  // Idempotency Hook
  const idempotency = createIdempotencyMiddleware(redis);
  app.addHook('preHandler', idempotency);

  // Health probe
  app.get('/health', async () => ({ status: 'UP', service: config.SERVICE_NAME }));

  // Register event routes
  registerEventRoutes(app, validator, producer);

  return app;
}

if (process.env.NODE_ENV !== 'test' && import.meta.url === `file://${process.argv[1]}`) {
  const config = loadConfig(IngestionServiceEnvSchema);
  buildIngestionServer().then((app) => {
    app.listen({ port: config.PORT, host: config.HOST }, (err, address) => {
      if (err) {
        defaultLogger.fatal('Failed to start ingestion service', { error: err.message });
        process.exit(1);
      }
      defaultLogger.info(`Ingestion Service listening on ${address}`);
    });
  });
}

