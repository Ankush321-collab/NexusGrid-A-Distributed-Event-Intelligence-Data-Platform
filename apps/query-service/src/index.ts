import Fastify from 'fastify';
import cors from '@fastify/cors';
import { defaultClickHouse, defaultRedis, type ClickHouseClientInterface } from '@nexusgrid/database';
import { QueryCacheManager } from './cache/cache-manager.js';
import { defaultASTValidator, ASTValidator } from './builder/ast-validator.js';
import { registerAnalyticsRoutes } from './routes/analytics.js';
import { defaultLogger } from '@nexusgrid/observability';

export interface BuildQueryServerOptions {
  clickhouse?: ClickHouseClientInterface;
  redis?: typeof defaultRedis;
  cacheManager?: QueryCacheManager;
  validator?: ASTValidator;
}

export async function buildQueryServer(opts: BuildQueryServerOptions = {}) {
  const app = Fastify({
    logger: false,
  });

  await app.register(cors, { origin: true });

  const clickhouse = opts.clickhouse || defaultClickHouse;
  const redis = opts.redis || defaultRedis;
  const cacheManager = opts.cacheManager || new QueryCacheManager(redis);
  const validator = opts.validator || defaultASTValidator;

  app.get('/health', async () => ({ status: 'UP', service: 'query-service' }));

  registerAnalyticsRoutes(app, clickhouse, cacheManager, validator);

  return app;
}

if (process.env.NODE_ENV !== 'test' && import.meta.url === `file://${process.argv[1]}`) {
  const port = parseInt(process.env.PORT || '8083', 10);
  buildQueryServer().then((app) => {
    app.listen({ port, host: '0.0.0.0' }, (err, address) => {
      if (err) {
        defaultLogger.fatal('Failed to start query service', { error: err.message });
        process.exit(1);
      }
      defaultLogger.info(`Query Service listening on ${address}`);
    });
  });
}
