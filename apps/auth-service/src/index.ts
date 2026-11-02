import Fastify from 'fastify';
import cors from '@fastify/cors';
import { defaultDatabase } from '@nexusgrid/database';
import { AuthService } from './services/auth.service.js';
import { CryptoService } from './services/crypto.service.js';
import { registerAuthRoutes } from './routes/auth.routes.js';
import { defaultLogger } from '@nexusgrid/observability';
import { loadConfig, AuthServiceEnvSchema } from '@nexusgrid/config';

export interface BuildAuthServerOptions {
  authService?: AuthService;
  database?: typeof defaultDatabase;
  cryptoService?: CryptoService;
}

export async function buildAuthServer(opts: BuildAuthServerOptions = {}) {
  const config = loadConfig(AuthServiceEnvSchema);
  const app = Fastify({
    logger: false, // Observability package handles structured logging
  });

  await app.register(cors, { origin: true });

  const db = opts.database || defaultDatabase;
  const crypto = opts.cryptoService || new CryptoService(config.API_KEY_SALT);
  const authService = opts.authService || new AuthService(db, crypto);
  registerAuthRoutes(app, authService, db);

  app.get('/health', async () => ({ status: 'UP', service: config.SERVICE_NAME }));

  return app;
}

if (process.env.NODE_ENV !== 'test' && import.meta.url === `file://${process.argv[1]}`) {
  const config = loadConfig(AuthServiceEnvSchema);
  buildAuthServer().then((app) => {
    app.listen({ port: config.PORT, host: config.HOST }, (err, address) => {
      if (err) {
        defaultLogger.fatal('Failed to start auth service', { error: err.message });
        process.exit(1);
      }
      defaultLogger.info(`Auth Service listening on ${address}`);
    });

    const shutdown = async (signal: string) => {
      defaultLogger.info(`Received ${signal}. Closing Auth Service HTTP server...`);
      await app.close();
      defaultLogger.info('Auth Service shut down gracefully.');
      process.exit(0);
    };
    process.once('SIGTERM', () => shutdown('SIGTERM'));
    process.once('SIGINT', () => shutdown('SIGINT'));
  });
}

