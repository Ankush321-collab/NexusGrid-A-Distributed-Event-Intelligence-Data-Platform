import Fastify from 'fastify';
import cors from '@fastify/cors';
import { defaultRedis } from '@nexusgrid/database';
import { defaultEventValidator } from './validators/event-validator.js';
import { defaultKafkaProducer } from './producer/kafka-producer.js';
import { createRateLimiter } from './rate-limit/rate-limiter.js';
import { createIdempotencyMiddleware } from './middleware/idempotency.js';
import { registerEventRoutes } from './routes/events.js';
import { defaultLogger } from '@nexusgrid/observability';

export interface BuildIngestionServerOptions {
  redis?: typeof defaultRedis;
  validator?: typeof defaultEventValidator;
  producer?: typeof defaultKafkaProducer;
  maxTokens?: number;
  refillRate?: number;
}

export async function buildIngestionServer(opts: BuildIngestionServerOptions = {}) {
  const app = Fastify({
    logger: false,
    bodyLimit: 1048576, // 1MB
  });

  await app.register(cors, { origin: true });

  const redis = opts.redis || defaultRedis;
  const validator = opts.validator || defaultEventValidator;
  const producer = opts.producer || defaultKafkaProducer;
  const maxTokens = opts.maxTokens ?? 100;
  const refillRate = opts.refillRate ?? 10;

  // Rate Limiting Hook
  const rateLimiter = createRateLimiter(redis, maxTokens, refillRate);
  app.addHook('preHandler', rateLimiter);

  // Idempotency Hook
  const idempotency = createIdempotencyMiddleware(redis);
  app.addHook('preHandler', idempotency);

  // Health probe
  app.get('/health', async () => ({ status: 'UP', service: 'ingestion-service' }));

  // Register event routes
  registerEventRoutes(app, validator, producer);

  return app;
}

if (process.env.NODE_ENV !== 'test' && import.meta.url === `file://${process.argv[1]}`) {
  const port = parseInt(process.env.PORT || '8080', 10);
  buildIngestionServer().then((app) => {
    app.listen({ port, host: '0.0.0.0' }, (err, address) => {
      if (err) {
        defaultLogger.fatal('Failed to start ingestion service', { error: err.message });
        process.exit(1);
      }
      defaultLogger.info(`Ingestion Service listening on ${address}`);
    });
  });
}

