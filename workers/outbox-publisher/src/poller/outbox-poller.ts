import type { InMemoryPostgresAdapter, KafkaProducer } from '@nexusgrid/database';
import type { EventEnvelope } from '@nexusgrid/contracts';

export class OutboxPoller {
  private isRunning: boolean = false;
  private intervalTimer: NodeJS.Timeout | null = null;
  public publishedCount: number = 0;
  public prunedCount: number = 0;
  private currentIntervalMs: number;

  constructor(
    private db: InMemoryPostgresAdapter,
    private producer: KafkaProducer,
    private pollIntervalMs: number = 100
  ) {
    this.currentIntervalMs = pollIntervalMs;
  }

  /**
   * Problem 4 Solution: Prunes aged outbox records to avoid database table bloat
   */
  public async pruneProcessed(retentionDays: number = 7): Promise<number> {
    if ('pruneProcessedOutbox' in this.db) {
      const deleted = await (this.db as any).pruneProcessedOutbox(retentionDays);
      this.prunedCount += deleted;
      return deleted;
    }
    return 0;
  }

  /**
   * Polls and publishes one batch of unprocessed outbox records
   */
  public async pollAndPublishBatch(batchSize: number = 50): Promise<number> {
    const records = await this.db.fetchUnprocessedOutbox(batchSize, true);
    if (records.length === 0) {
      // Adaptive backoff when outbox is idle (reduces PostgreSQL lock contention)
      this.currentIntervalMs = Math.min(1000, Math.floor(this.currentIntervalMs * 1.5));
      return 0;
    }

    // Reset backoff to base interval on active workload
    this.currentIntervalMs = this.pollIntervalMs;

    for (const record of records) {
      try {
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
      } catch (err) {
        if ('unlockOutboxRecord' in this.db) {
          (this.db as any).unlockOutboxRecord(record.id);
        }
        throw err;
      }
    }

    return records.length;
  }

  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;

    const scheduleNext = () => {
      if (!this.isRunning) return;
      this.intervalTimer = setTimeout(async () => {
        try {
          await this.pollAndPublishBatch();
        } catch (_err) {}
        scheduleNext();
      }, this.currentIntervalMs);
    };

    scheduleNext();
  }

  public stop(): void {
    this.isRunning = false;
    if (this.intervalTimer) {
      clearTimeout(this.intervalTimer);
      this.intervalTimer = null;
    }
  }

  public getCurrentIntervalMs(): number {
    return this.currentIntervalMs;
  }
}

