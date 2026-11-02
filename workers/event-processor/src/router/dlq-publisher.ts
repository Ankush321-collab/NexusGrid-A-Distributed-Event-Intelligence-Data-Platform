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
    const replayCount = parseInt(originalMessage.headers['x-replay-count'] || '0', 10);

    // Problem 5 Solution: DLQ Replay Circuit Breaker
    // If a poisoned message has already been replayed >= 2 times and fails again,
    // quarantine it to prevent infinite loop
    let targetTopic = failure.targetTopic;
    let isQuarantined = false;

    if (failure.type !== 'RETRIABLE' && replayCount >= 2) {
      targetTopic = 'nexusgrid.events.quarantine';
      isQuarantined = true;
    }

    const enrichedHeaders: KafkaEventMessage['headers'] = {
      ...originalMessage.headers,
      'x-original-topic': originalTopic,
      'x-retry-count': failure.retryCount.toString(),
      'x-error-reason': failure.reason,
      'x-error-stack': failure.stack || 'N/A',
      'x-timestamp': new Date().toISOString(),
      'x-retry-timestamp': new Date().toISOString(),
      ...(isQuarantined ? { 'x-quarantine-reason': 'MAX_REPLAY_EXCEEDED_CIRCUIT_BREAKER' } : {}),
    };

    const targetMessage: KafkaEventMessage = {
      key: originalMessage.key,
      value: originalMessage.value,
      headers: enrichedHeaders,
    };

    if (!this.producer.publishedMessages.has(targetTopic)) {
      this.producer.publishedMessages.set(targetTopic, []);
    }
    this.producer.publishedMessages.get(targetTopic)!.push(targetMessage);
  }
}

