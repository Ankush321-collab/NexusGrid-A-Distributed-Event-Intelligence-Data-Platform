import type { InMemoryPostgresAdapter } from '@nexusgrid/database';
import type { KafkaProducer } from '../../../apps/ingestion-service/src/producer/kafka-producer.js';
import type { EventEnvelope } from '@nexusgrid/contracts';

export class OutboxPoller {
  private isRunning: boolean = false;
  private intervalTimer: NodeJS.Timeout | null = null;
  public publishedCount: number = 0;

  constructor(
    private db: InMemoryPostgresAdapter,
    private producer: KafkaProducer,
    private pollIntervalMs: number = 100
  ) {}

  /**
   * Polls and publishes one batch of unprocessed outbox records
   */
  public async pollAndPublishBatch(batchSize: number = 50): Promise<number> {
    const records = await this.db.fetchUnprocessedOutbox(batchSize);
    if (records.length === 0) return 0;

    for (const record of records) {
      const syntheticEnvelope: EventEnvelope = {
        event_id: `outbox_${record.id}_${Date.now()}`.padEnd(26, '0').slice(0, 26),
        tenant_id: (record.payload.tenant_id as string) || 'system',
        project_id: (record.payload.project_id as string) || 'system',
        event_name: record.event_type,
        occurred_at: record.created_at,
        schema_version: 1,
        properties: record.payload,
      };

      await this.producer.publish(syntheticEnvelope, `cor_outbox_${record.id}`, 'nexusgrid.events.integration');
      await this.db.markOutboxProcessed(record.id);
      this.publishedCount++;
    }

    return records.length;
  }

  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.intervalTimer = setInterval(async () => {
      try {
        await this.pollAndPublishBatch();
      } catch (_err) {}
    }, this.pollIntervalMs);
  }

  public stop(): void {
    this.isRunning = false;
    if (this.intervalTimer) {
      clearInterval(this.intervalTimer);
      this.intervalTimer = null;
    }
  }
}

