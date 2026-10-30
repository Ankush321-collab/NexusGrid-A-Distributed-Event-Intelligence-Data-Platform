import crypto from 'crypto';
import type { EventEnvelope, KafkaEventMessage } from '@nexusgrid/contracts';

export interface ProducerResult {
  topic: string;
  partition: number;
  offset: number;
}

export class KafkaProducer {
  private topic: string;
  private partitionCount: number;
  public publishedMessages: Map<string, KafkaEventMessage[]> = new Map();
  private offsetCounter: Map<string, number> = new Map();

  constructor(topic: string = 'nexusgrid.events.raw', partitionCount: number = 12) {
    this.topic = topic;
    this.partitionCount = partitionCount;
  }

  /**
   * Deterministic Murmur-like partition hash based on tenant_id and project_id
   * Guarantees all events for a project land on the exact same partition in order.
   */
  public computePartition(tenantId: string, projectId: string): number {
    const key = `${tenantId}:${projectId}`;
    const hash = crypto.createHash('md5').update(key).digest();
    const intVal = hash.readUInt32BE(0);
    return intVal % this.partitionCount;
  }

  public async publish(
    event: EventEnvelope,
    correlationId: string,
    targetTopic?: string
  ): Promise<ProducerResult> {
    const topic = targetTopic || this.topic;
    const partition = this.computePartition(event.tenant_id, event.project_id);

    const message: KafkaEventMessage = {
      key: `${event.tenant_id}:${event.project_id}`,
      value: JSON.stringify(event),
      headers: {
        'x-event-id': event.event_id,
        'x-tenant-id': event.tenant_id,
        'x-project-id': event.project_id,
        'x-schema-version': event.schema_version.toString(),
        'x-correlation-id': correlationId,
        'x-retry-count': '0',
        'x-timestamp': new Date().toISOString(),
      },
    };

    if (!this.publishedMessages.has(topic)) {
      this.publishedMessages.set(topic, []);
    }
    this.publishedMessages.get(topic)!.push(message);

    const currentOffset = (this.offsetCounter.get(topic) || 0) + 1;
    this.offsetCounter.set(topic, currentOffset);

    return {
      topic,
      partition,
      offset: currentOffset,
    };
  }

  public async publishBatch(
    events: EventEnvelope[],
    correlationId: string,
    targetTopic?: string
  ): Promise<ProducerResult[]> {
    const results: ProducerResult[] = [];
    for (const evt of events) {
      results.push(await this.publish(evt, correlationId, targetTopic));
    }
    return results;
  }

  public getMessages(topic?: string): KafkaEventMessage[] {
    return this.publishedMessages.get(topic || this.topic) || [];
  }

  public clear(): void {
    this.publishedMessages.clear();
    this.offsetCounter.clear();
  }
}

export const defaultKafkaProducer = new KafkaProducer();

