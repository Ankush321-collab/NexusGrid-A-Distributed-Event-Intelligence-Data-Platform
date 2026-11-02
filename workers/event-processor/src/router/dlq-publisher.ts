import type { KafkaEventMessage } from '@nexusgrid/contracts';
import { KafkaProducer } from '@nexusgrid/database';
import type { ClassifiedFailure } from './error-classifier.js';

export class DLQPublisher {
  constructor(private producer: KafkaProducer) {}

  public async routeFailure(
    originalMessage: KafkaEventMessage,
    originalTopic: string,
    partition: number,
    offset: number,
    failure: ClassifiedFailure
  ): Promise<void> {
    const enrichedHeaders: KafkaEventMessage['headers'] = {
      ...originalMessage.headers,
      'x-original-topic': originalTopic,
      'x-retry-count': failure.retryCount.toString(),
      'x-error-reason': failure.reason,
      'x-error-stack': failure.stack || 'N/A',
      'x-timestamp': new Date().toISOString(),
    };

    const targetMessage: KafkaEventMessage = {
      key: originalMessage.key,
      value: originalMessage.value,
      headers: enrichedHeaders,
    };

    if (!this.producer.publishedMessages.has(failure.targetTopic)) {
      this.producer.publishedMessages.set(failure.targetTopic, []);
    }
    this.producer.publishedMessages.get(failure.targetTopic)!.push(targetMessage);
  }
}

