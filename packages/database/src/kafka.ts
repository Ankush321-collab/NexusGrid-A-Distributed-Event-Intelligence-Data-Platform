import crypto from 'crypto';
import type { EventEnvelope, KafkaEventMessage } from '@nexusgrid/contracts';

export interface ProducerResult {
  topic: string;
  partition: number;
  offset: number;
  partitionKey: string;
}

export interface PublishOptions {
  entityKey?: string;
  virtualSalt?: number;
  targetTopic?: string;
}

export class KafkaProducer {
  private topic: string;
  private partitionCount: number;
  public publishedMessages: Map<string, KafkaEventMessage[]> = new Map();
  private offsetCounter: Map<string, number> = new Map();

  constructor(
    topic: string = process.env.KAFKA_RAW_TOPIC || 'nexusgrid.events.raw',
    partitionCount: number = 12
  ) {
    this.topic = topic;
    this.partitionCount = partitionCount;
  }

  /**
   * Deterministic Murmur-like partition hash with Sub-Key Entity Salting & Virtual Partitions
   * - Without entityKey: Partitions by tenant_id:project_id (preserves per-project ordering)
   * - With entityKey: Partitions by tenant_id:project_id:entity_id (prevents hot tenant single-partition saturation)
   * - With virtualSalt: Appends virtual partition salt (distributes bulk unkeyed high-volume streams)
   */
  public computePartition(
    tenantId: string,
    projectId: string,
    entityKey?: string,
    virtualSalt?: number
  ): { partition: number; partitionKey: string } {
    let key = `${tenantId}:${projectId}`;
    if (entityKey) {
      key += `:${entityKey}`;
    } else if (virtualSalt !== undefined) {
      key += `:vsalt_${virtualSalt}`;
    }

    const hash = crypto.createHash('md5').update(key).digest();
    const intVal = hash.readUInt32BE(0);
    return {
      partition: intVal % this.partitionCount,
      partitionKey: key,
    };
  }

  public async publish(
    event: EventEnvelope,
    correlationId: string,
    targetTopicOrOpts?: string | PublishOptions
  ): Promise<ProducerResult> {
    const opts: PublishOptions =
      typeof targetTopicOrOpts === 'string'
        ? { targetTopic: targetTopicOrOpts }
        : targetTopicOrOpts || {};

    const topic = opts.targetTopic || this.topic;

    // Auto-detect entity key from event properties if not explicitly provided
    let entityKey = opts.entityKey;
    if (!entityKey && event.properties && typeof event.properties === 'object') {
      const props = event.properties as Record<string, any>;
      entityKey = props.user_id || props.customer_id || props.device_id || props.entity_id || props.order_id;
    }

    const { partition, partitionKey } = this.computePartition(
      event.tenant_id,
      event.project_id,
      entityKey,
      opts.virtualSalt
    );

    const message: KafkaEventMessage = {
      key: partitionKey,
      value: JSON.stringify(event),
      headers: {
        'x-event-id': event.event_id,
        'x-tenant-id': event.tenant_id,
        'x-project-id': event.project_id,
        'x-schema-version': event.schema_version.toString(),
        'x-correlation-id': correlationId,
        'x-partition-key': partitionKey,
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
      partitionKey,
    };
  }

  public async publishBatch(
    events: EventEnvelope[],
    correlationId: string,
    targetTopicOrOpts?: string | PublishOptions
  ): Promise<ProducerResult[]> {
    const results: ProducerResult[] = [];
    for (const evt of events) {
      results.push(await this.publish(evt, correlationId, targetTopicOrOpts));
    }
    return results;
  }

  public getMessages(topic?: string): KafkaEventMessage[] {
    const t = topic || this.topic;
    if (!this.publishedMessages.has(t)) {
      this.publishedMessages.set(t, []);
    }
    return this.publishedMessages.get(t)!;
  }

  public clear(): void {
    this.publishedMessages.clear();
    this.offsetCounter.clear();
  }
}

export const defaultKafkaProducer = new KafkaProducer();

