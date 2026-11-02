import { defaultKafkaProducer } from '@nexusgrid/database';
import { StreamEventProcessor } from './consumer/kafka-consumer.js';
import { defaultLogger } from '@nexusgrid/observability';
import { loadConfig, EventProcessorEnvSchema } from '@nexusgrid/config';

export async function startWorker() {
  const config = loadConfig(EventProcessorEnvSchema);
  defaultLogger.info(`Starting ${config.SERVICE_NAME}...`);
  const processor = new StreamEventProcessor(defaultKafkaProducer, async (event) => {
    defaultLogger.debug('Processing event in worker', { event_id: event.event_id });
  });

  // Start continuous polling loop
  const timer = setInterval(() => {
    processor.processTopic(config.KAFKA_RAW_TOPIC).catch((err) => {
      defaultLogger.error('Error during worker processing', { error: err.message });
    });
  }, config.BATCH_TIMEOUT_MS);

  // Problem 6 Solution: Graceful shutdown on SIGTERM / SIGINT
  const onSignal = async (signal: string) => {
    defaultLogger.info(`Received ${signal}. Initiating graceful shutdown...`);
    clearInterval(timer);
    await processor.shutdown();
    defaultLogger.info('Event processor shut down cleanly.');
    if (process.env.NODE_ENV !== 'test') {
      process.exit(0);
    }
  };

  process.once('SIGTERM', () => onSignal('SIGTERM'));
  process.once('SIGINT', () => onSignal('SIGINT'));

  return processor;
}

if (process.env.NODE_ENV !== 'test' && import.meta.url === `file://${process.argv[1]}`) {
  startWorker().catch((err) => {
    defaultLogger.fatal('Worker failed to initialize', { error: err.message });
    process.exit(1);
  });
}

