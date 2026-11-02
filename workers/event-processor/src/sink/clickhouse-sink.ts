import { InMemoryClickHouseAdapter, type ClickHouseClientInterface, type ClickHouseEventRecord } from '@nexusgrid/database';
import type { EventEnvelope } from '@nexusgrid/contracts';

export interface S3ArchiveEntry {
  s3Key: string;
  tenantId: string;
  eventCount: number;
  uploadedAt: string;
  data: string;
}

export interface SinkOptions {
  batchSize?: number;
  flushIntervalMs?: number;
  enableS3Archive?: boolean;
}

export class ClickHouseBulkSink {
  private buffer: ClickHouseEventRecord[] = [];
  private rawBuffer: EventEnvelope[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private batchSize: number;
  private flushIntervalMs: number;
  private enableS3Archive: boolean;
  public totalFlushedToClickHouse: number = 0;
  public totalArchivedToS3: number = 0;
  public archivedBatches: S3ArchiveEntry[] = [];

  constructor(
    private clickhouse: ClickHouseClientInterface,
    options: SinkOptions = {}
  ) {
    this.batchSize = options.batchSize || 5000;
    this.flushIntervalMs = options.flushIntervalMs || 500;
    this.enableS3Archive = options.enableS3Archive !== false;
  }

  public async push(event: EventEnvelope): Promise<void> {
    const chRecord = InMemoryClickHouseAdapter.envelopeToClickHouseRecord(event);
    this.buffer.push(chRecord);
    this.rawBuffer.push(event);

    if (this.buffer.length >= this.batchSize) {
      await this.flush();
    } else if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => {
        this.flush().catch(() => {});
      }, this.flushIntervalMs);
    }
  }

  public async pushBatch(events: EventEnvelope[]): Promise<void> {
    for (const evt of events) {
      await this.push(evt);
    }
  }

  public async flush(): Promise<{ inserted: number; archived: number }> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    if (this.buffer.length === 0) {
      return { inserted: 0, archived: 0 };
    }

    const recordsToInsert = this.buffer.slice();
    const rawEventsToArchive = this.rawBuffer.slice();
    this.buffer = [];
    this.rawBuffer = [];

    // 1. Bulk Insert into ClickHouse
    let inserted = 0;
    try {
      inserted = await this.clickhouse.insertBatch(recordsToInsert);
      this.totalFlushedToClickHouse += inserted;
    } catch (err: any) {
      // Re-queue on unexpected transient failure
      this.buffer.unshift(...recordsToInsert);
      this.rawBuffer.unshift(...rawEventsToArchive);
      throw err;
    }

    // 2. Stream raw batch to Cold S3/MinIO Archive
    let archived = 0;
    if (this.enableS3Archive && rawEventsToArchive.length > 0) {
      const now = new Date();
      const tenantId = rawEventsToArchive[0]?.tenant_id || 'system';
      const yyyy = now.getUTCFullYear();
      const mm = (now.getUTCMonth() + 1).toString().padStart(2, '0');
      const dd = now.getUTCDate().toString().padStart(2, '0');
      const batchId = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
      const s3Key = `s3://nexusgrid-archives/${tenantId}/${yyyy}/${mm}/${dd}/events_${batchId}.json`;

      const entry: S3ArchiveEntry = {
        s3Key,
        tenantId,
        eventCount: rawEventsToArchive.length,
        uploadedAt: now.toISOString(),
        data: JSON.stringify(rawEventsToArchive),
      };

      this.archivedBatches.push(entry);
      this.totalArchivedToS3 += rawEventsToArchive.length;
      archived = rawEventsToArchive.length;
    }

    return { inserted, archived };
  }

  public getPendingBufferCount(): number {
    return this.buffer.length;
  }
}
