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
  setInterval(() => {
    processor.processTopic(config.KAFKA_RAW_TOPIC).catch((err) => {
      defaultLogger.error('Error during worker processing', { error: err.message });
    });
  }, config.BATCH_TIMEOUT_MS);

  return processor;
}

if (process.env.NODE_ENV !== 'test' && import.meta.url === `file://${process.argv[1]}`) {
  startWorker().catch((err) => {
    defaultLogger.fatal('Worker failed to initialize', { error: err.message });
    process.exit(1);
  });
}

