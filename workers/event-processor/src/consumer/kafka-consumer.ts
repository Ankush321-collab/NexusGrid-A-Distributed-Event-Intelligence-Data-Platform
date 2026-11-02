import type { KafkaEventMessage, EventEnvelope } from '@nexusgrid/contracts';
import { KafkaProducer } from '@nexusgrid/database';
import { ErrorClassifier, defaultErrorClassifier } from '../router/error-classifier.js';
import { DLQPublisher } from '../router/dlq-publisher.js';
import { EventBatcher } from '../batcher/event-batcher.js';

export interface ConsumerStats {
  processedCount: number;
  retriedCount: number;
  dlqCount: number;
  delayedCount: number;
  quarantinedCount: number;
  committedOffset: number;
}

export type EventProcessorFn = (event: EventEnvelope) => Promise<void>;

export interface StreamEventProcessorOptions {
  enforceRetryDelay?: boolean;
}

export class StreamEventProcessor {
  private classifier: ErrorClassifier;
  private dlqPublisher: DLQPublisher;
  private batcher: EventBatcher<EventEnvelope>;
  private committedOffsets: Map<string, number> = new Map();
  public isShuttingDown: boolean = false;
  public enforceRetryDelay: boolean;
  public stats: ConsumerStats = {
    processedCount: 0,
    retriedCount: 0,
    dlqCount: 0,
    delayedCount: 0,
    quarantinedCount: 0,
    committedOffset: 0,
  };

  constructor(
    private producer: KafkaProducer,
    private processFn?: EventProcessorFn,
    classifier: ErrorClassifier = defaultErrorClassifier,
    options: StreamEventProcessorOptions = {}
  ) {
    this.classifier = classifier;
    this.enforceRetryDelay = options.enforceRetryDelay ?? false;
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
   * Calculates required temporal delay for retry topics to prevent Retry Storms
   */
  public getTopicRetryDelayMs(topic: string): number {
    if (topic.includes('.retry.10s')) return 10000;
    if (topic.includes('.retry.1m')) return 60000;
    if (topic.includes('.retry.5m')) return 300000;
    return 0;
  }

  /**
   * Consumes a single Kafka record with explicit error classification & DLQ defense
   */
  public async consumeRecord(
    message: KafkaEventMessage,
    topic: string = 'nexusgrid.events.raw',
    partition: number = 0,
    offset: number = 1,
    now: number = Date.now()
  ): Promise<{ status: 'PROCESSED' | 'DELAYED' | 'DLQ' | 'RETRIED' } | void> {
    if (this.isShuttingDown) return;

    // Problem 2 Solution: Check if this is a delayed retry topic and enforce backoff
    const delay = this.getTopicRetryDelayMs(topic);
    if (this.enforceRetryDelay && delay > 0) {
      const queuedAt = Date.parse(message.headers['x-retry-timestamp'] || message.headers['x-timestamp'] || '0');
      if (now - queuedAt < delay) {
        this.stats.delayedCount++;
        return { status: 'DELAYED' };
      }
    }

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
      const replayCount = parseInt(message.headers['x-replay-count'] || '0', 10);
      if (replayCount >= 2) {
        this.stats.quarantinedCount++;
      } else {
        this.stats.dlqCount++;
      }
      this.commitOffset(topic, partition, offset);
      return { status: 'DLQ' };
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
      return { status: 'PROCESSED' };
    } catch (procErr: any) {
      // Classify processing error
      const failure = this.classifier.classify(procErr, currentRetries);

      if (failure.type === 'RETRIABLE') {
        await this.dlqPublisher.routeFailure(message, topic, partition, offset, failure);
        this.stats.retriedCount++;
        this.commitOffset(topic, partition, offset);
        return { status: 'RETRIED' };
      } else {
        // Poison or Terminal Exhaustion
        await this.dlqPublisher.routeFailure(message, topic, partition, offset, failure);
        const replayCount = parseInt(message.headers['x-replay-count'] || '0', 10);
        if (replayCount >= 2) {
          this.stats.quarantinedCount++;
        } else {
          this.stats.dlqCount++;
        }
        this.commitOffset(topic, partition, offset);
        return { status: 'DLQ' };
      }
    }
  }

  /**
   * Consumes all available messages from a topic starting from committed offset.
   * If a record on a retry topic is not ready yet, pauses at that offset to prevent retry storms.
   */
  public async processTopic(
    topic: string = 'nexusgrid.events.raw',
    partition: number = 0,
    now: number = Date.now()
  ): Promise<void> {
    if (this.isShuttingDown) return;

    const messages = this.producer.getMessages(topic);
    const startIndex = this.getCommittedOffset(topic, partition);
    for (let i = startIndex; i < messages.length; i++) {
      if (this.isShuttingDown) break;
      const res = await this.consumeRecord(messages[i], topic, partition, i + 1, now);
      if (res && res.status === 'DELAYED') {
        // Stop consuming retry partition until backoff delay expires
        break;
      }
    }
    await this.batcher.flush();
  }

  /**
   * Problem 6 Solution: Graceful Shutdown
   * Flushes in-flight micro-batch buffer and halts consumer loops safely without dropped events
   */
  public async shutdown(): Promise<void> {
    this.isShuttingDown = true;
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

