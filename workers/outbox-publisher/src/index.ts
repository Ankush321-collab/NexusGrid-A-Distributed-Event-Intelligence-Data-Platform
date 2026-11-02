import { defaultDatabase, defaultKafkaProducer } from '@nexusgrid/database';
import { OutboxPoller } from './poller/outbox-poller.js';
import { defaultLogger } from '@nexusgrid/observability';
import { loadConfig, OutboxPublisherEnvSchema } from '@nexusgrid/config';

export function startOutboxPublisher() {
  const config = loadConfig(OutboxPublisherEnvSchema);
  defaultLogger.info(`Starting ${config.SERVICE_NAME}...`);
  const poller = new OutboxPoller(defaultDatabase, defaultKafkaProducer, config.OUTBOX_POLL_INTERVAL_MS);
  poller.start();
  return poller;
}

if (process.env.NODE_ENV !== 'test' && import.meta.url === `file://${process.argv[1]}`) {
  startOutboxPublisher();
}

