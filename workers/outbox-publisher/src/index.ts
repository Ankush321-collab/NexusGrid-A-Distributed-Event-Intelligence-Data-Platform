import { defaultDatabase } from '@nexusgrid/database';
import { defaultKafkaProducer } from '../../apps/ingestion-service/src/producer/kafka-producer.js';
import { OutboxPoller } from './poller/outbox-poller.js';
import { defaultLogger } from '@nexusgrid/observability';

export function startOutboxPublisher() {
  defaultLogger.info('Starting Transactional Outbox Publisher Worker...');
  const poller = new OutboxPoller(defaultDatabase, defaultKafkaProducer, 500);
  poller.start();
  return poller;
}

if (process.env.NODE_ENV !== 'test' && import.meta.url === `file://${process.argv[1]}`) {
  startOutboxPublisher();
}

