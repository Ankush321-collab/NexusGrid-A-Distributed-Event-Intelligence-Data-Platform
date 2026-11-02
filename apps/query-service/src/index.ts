import Fastify from 'fastify';
import cors from '@fastify/cors';
import { defaultClickHouse, defaultRedis, type ClickHouseClientInterface } from '@nexusgrid/database';
import { QueryCacheManager } from './cache/cache-manager.js';
import { defaultASTValidator, ASTValidator } from './builder/ast-validator.js';
import { registerAnalyticsRoutes } from './routes/analytics.js';
import { defaultLogger } from '@nexusgrid/observability';
import { loadConfig, QueryServiceEnvSchema } from '@nexusgrid/config';

export interface BuildQueryServerOptions {
  clickhouse?: ClickHouseClientInterface;
  redis?: typeof defaultRedis;
  cacheManager?: QueryCacheManager;
  validator?: ASTValidator;
}

export async function buildQueryServer(opts: BuildQueryServerOptions = {}) {
  const config = loadConfig(QueryServiceEnvSchema);
  const app = Fastify({
    logger: false,
  });

  await app.register(cors, { origin: true });

  const clickhouse = opts.clickhouse || defaultClickHouse;
  const redis = opts.redis || defaultRedis;
  const cacheManager = opts.cacheManager || new QueryCacheManager(redis, config.CACHE_TTL_SEC * 1000);
  const validator = opts.validator || new ASTValidator(config.QUERY_MAX_SCAN_DAYS);

  app.get('/health', async () => ({ status: 'UP', service: config.SERVICE_NAME }));

  registerAnalyticsRoutes(app, clickhouse, cacheManager, validator);

  return app;
}

if (process.env.NODE_ENV !== 'test' && import.meta.url === `file://${process.argv[1]}`) {
  const config = loadConfig(QueryServiceEnvSchema);
  buildQueryServer().then((app) => {
    app.listen({ port: config.PORT, host: config.HOST }, (err, address) => {
      if (err) {
        defaultLogger.fatal('Failed to start query service', { error: err.message });
        process.exit(1);
      }
      defaultLogger.info(`Query Service listening on ${address}`);
    });

    const shutdown = async (signal: string) => {
      defaultLogger.info(`Received ${signal}. Closing Query Service HTTP server...`);
      await app.close();
      defaultLogger.info('Query Service shut down gracefully.');
      process.exit(0);
    };
    process.once('SIGTERM', () => shutdown('SIGTERM'));
    process.once('SIGINT', () => shutdown('SIGINT'));
  });
}

