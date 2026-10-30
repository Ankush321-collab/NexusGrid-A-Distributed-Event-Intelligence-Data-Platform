import Fastify from 'fastify';
import cors from '@fastify/cors';
import { defaultDatabase } from '@nexusgrid/database';
import { AuthService } from './services/auth.service.js';
import { registerAuthRoutes } from './routes/auth.routes.js';
import { defaultLogger } from '@nexusgrid/observability';

export async function buildAuthServer() {
  const app = Fastify({
    logger: false, // Observability package handles structured logging
  });

  await app.register(cors, { origin: true });

  const authService = new AuthService(defaultDatabase);
  registerAuthRoutes(app, authService, defaultDatabase);

  app.get('/health', async () => ({ status: 'UP', service: 'auth-service' }));

  return app;
}

if (process.env.NODE_ENV !== 'test' && import.meta.url === `file://${process.argv[1]}`) {
  const port = parseInt(process.env.PORT || '8081', 10);
  buildAuthServer().then((app) => {
    app.listen({ port, host: '0.0.0.0' }, (err, address) => {
      if (err) {
        defaultLogger.fatal('Failed to start auth service', { error: err.message });
        process.exit(1);
      }
      defaultLogger.info(`Auth Service listening on ${address}`);
    });
  });
}

