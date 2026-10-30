import { defaultKafkaProducer } from '../../apps/ingestion-service/src/producer/kafka-producer.js';
import { StreamEventProcessor } from './consumer/kafka-consumer.js';
import { defaultLogger } from '@nexusgrid/observability';

export async function startWorker() {
  defaultLogger.info('Starting Stream Event Processor Worker...');
  const processor = new StreamEventProcessor(defaultKafkaProducer, async (event) => {
    defaultLogger.debug('Processing event in worker', { event_id: event.event_id });
  });

  // Start continuous polling loop
  setInterval(() => {
    processor.processTopic().catch((err) => {
      defaultLogger.error('Error during worker processing', { error: err.message });
    });
  }, 100);

  return processor;
}

if (process.env.NODE_ENV !== 'test' && import.meta.url === `file://${process.argv[1]}`) {
  startWorker().catch((err) => {
    defaultLogger.fatal('Worker failed to initialize', { error: err.message });
    process.exit(1);
  });
}

