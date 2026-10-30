import type { KafkaEventMessage, EventEnvelope } from '@nexusgrid/contracts';
import { KafkaProducer } from '../../../apps/ingestion-service/src/producer/kafka-producer.js';
import { ErrorClassifier, defaultErrorClassifier } from '../router/error-classifier.js';
import { DLQPublisher } from '../router/dlq-publisher.js';
import { EventBatcher } from '../batcher/event-batcher.js';

export interface ConsumerStats {
  processedCount: number;
  retriedCount: number;
  dlqCount: number;
  committedOffset: number;
}

export type EventProcessorFn = (event: EventEnvelope) => Promise<void>;

export class StreamEventProcessor {
  private classifier: ErrorClassifier;
  private dlqPublisher: DLQPublisher;
  private batcher: EventBatcher<EventEnvelope>;
  private committedOffsets: Map<string, number> = new Map();
  public stats: ConsumerStats = {
    processedCount: 0,
    retriedCount: 0,
    dlqCount: 0,
    committedOffset: 0,
  };

  constructor(
    private producer: KafkaProducer,
    private processFn?: EventProcessorFn,
    classifier: ErrorClassifier = defaultErrorClassifier
  ) {
    this.classifier = classifier;
    this.dlqPublisher = new DLQPublisher(producer);
    this.batcher = new EventBatcher<EventEnvelope>({
      maxBatchSize: 100,
      flushIntervalMs: 50,
      onFlush: async (batch) => {
        if (this.processFn) {
          for (const evt of batch) {
            await this.processFn(evt);
          }
        }
      },
    });
  }

  /**
   * Consumes a single Kafka record with explicit error classification & DLQ defense
   */
  public async consumeRecord(
    message: KafkaEventMessage,
    topic: string = 'nexusgrid.events.raw',
    partition: number = 0,
    offset: number = 1
  ): Promise<void> {
    const currentRetries = parseInt(message.headers['x-retry-count'] || '0', 10);

    let parsedEnvelope: EventEnvelope;
    try {
      // Step 1: Deserialize JSON payload
      parsedEnvelope = JSON.parse(message.value);

      // Validate basic shape
      if (!parsedEnvelope.event_id || !parsedEnvelope.tenant_id) {
        throw new Error('CORRUPTED_PAYLOAD: Missing essential envelope fields');
      }
    } catch (err: any) {
      // Non-retriable poison pill!
      const failure = this.classifier.classify(err, currentRetries);
      await this.dlqPublisher.routeFailure(message, topic, partition, offset, failure);
      this.stats.dlqCount++;
      this.commitOffset(topic, partition, offset);
      return;
    }

    // Step 2: Downstream processing
    try {
      if (this.processFn) {
        await this.processFn(parsedEnvelope);
      } else {
        await this.batcher.push(parsedEnvelope);
      }
      this.stats.processedCount++;
      this.commitOffset(topic, partition, offset);
    } catch (procErr: any) {
      // Classify processing error
      const failure = this.classifier.classify(procErr, currentRetries);

      if (failure.type === 'RETRIABLE') {
        await this.dlqPublisher.routeFailure(message, topic, partition, offset, failure);
        this.stats.retriedCount++;
      } else {
        // Poison or Terminal Exhaustion
        await this.dlqPublisher.routeFailure(message, topic, partition, offset, failure);
        this.stats.dlqCount++;
      }

      this.commitOffset(topic, partition, offset);
    }
  }

  /**
   * Consumes all available messages from a topic in the producer starting from committed offset
   */
  public async processTopic(topic: string = 'nexusgrid.events.raw', partition: number = 0): Promise<void> {
    const messages = this.producer.getMessages(topic);
    const startIndex = this.getCommittedOffset(topic, partition);
    for (let i = startIndex; i < messages.length; i++) {
      await this.consumeRecord(messages[i], topic, partition, i + 1);
    }
    await this.batcher.flush();
  }

  private commitOffset(topic: string, partition: number, offset: number): void {
    const key = `${topic}:${partition}`;
    this.committedOffsets.set(key, offset);
    this.stats.committedOffset = offset;
  }

  public getCommittedOffset(topic: string = 'nexusgrid.events.raw', partition: number = 0): number {
    return this.committedOffsets.get(`${topic}:${partition}`) || 0;
  }
}

