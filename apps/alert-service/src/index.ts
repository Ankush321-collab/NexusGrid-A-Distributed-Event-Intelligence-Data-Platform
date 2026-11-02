import Fastify from 'fastify';
import cors from '@fastify/cors';
import { defaultClickHouse } from '@nexusgrid/database';
import { RuleEvaluator } from './evaluator/rule-evaluator.js';
import { defaultAlertStateMachine } from './state/alert-state-machine.js';
import { defaultWebhookDispatcher } from './dispatch/webhook-dispatcher.js';
import { defaultLogger } from '@nexusgrid/observability';
import { loadConfig, AlertServiceEnvSchema } from '@nexusgrid/config';

export async function buildAlertServer(evaluator?: RuleEvaluator) {
  const config = loadConfig(AlertServiceEnvSchema);
  const app = Fastify({ logger: false });
  await app.register(cors, { origin: true });

  const ruleEvaluator = evaluator || new RuleEvaluator(defaultClickHouse, defaultAlertStateMachine, defaultWebhookDispatcher);

  app.get('/health', async () => ({ status: 'UP', service: config.SERVICE_NAME }));

  // Register Rule
  app.post('/v1/alerts/rules', async (req, reply) => {
    const body = req.body as any;
    if (!body?.id || !body?.tenant_id || !body?.project_id || !body?.threshold) {
      return reply.status(400).send({
        type: 'https://nexusgrid.io/errors/bad-request',
        title: 'Bad Request',
        status: 400,
        detail: 'id, tenant_id, project_id, and threshold are required',
        code: 'ERR_INVALID_BODY',
      });
    }

    ruleEvaluator.registerRule(body);
    return reply.status(201).send({ status: 'CREATED', rule: body });
  });

  // Trigger Manual Evaluation Cycle
  app.post('/v1/alerts/evaluate', async (req, reply) => {
    const results = await ruleEvaluator.evaluateAll();
    return reply.status(200).send({ evaluated_count: results.length, results });
  });

  return app;
}

if (process.env.NODE_ENV !== 'test' && import.meta.url === `file://${process.argv[1]}`) {
  const config = loadConfig(AlertServiceEnvSchema);
  buildAlertServer().then((app) => {
    app.listen({ port: config.PORT, host: config.HOST }, (err, address) => {
      if (err) {
        defaultLogger.fatal('Failed to start alert service', { error: err.message });
        process.exit(1);
      }
      defaultLogger.info(`Alert Service listening on ${address}`);
    });

    const shutdown = async (signal: string) => {
      defaultLogger.info(`Received ${signal}. Closing Alert Service HTTP server...`);
      await app.close();
      defaultLogger.info('Alert Service shut down gracefully.');
      process.exit(0);
    };
    process.once('SIGTERM', () => shutdown('SIGTERM'));
    process.once('SIGINT', () => shutdown('SIGINT'));
  });
}

