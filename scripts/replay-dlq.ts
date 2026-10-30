import { KafkaProducer } from '../apps/ingestion-service/src/producer/kafka-producer.js';
import type { KafkaEventMessage, EventEnvelope } from '@nexusgrid/contracts';

export interface ReplayOptions {
  dryRun?: boolean;
  tenantId?: string;
  maxEvents?: number;
  targetTopic?: string;
}

export interface ReplayReport {
  scanned: number;
  eligible: number;
  replayed: number;
  dryRun: boolean;
  events: Array<{
    eventId: string;
    tenantId: string;
    originalTopic?: string;
    errorReason?: string;
  }>;
}

export class DLQReplayTool {
  constructor(private producer: KafkaProducer) {}

  public async runReplay(options: ReplayOptions = {}): Promise<ReplayReport> {
    const dryRun = options.dryRun ?? true;
    const targetTopic = options.targetTopic || 'nexusgrid.events.raw';
    const dlqMessages = this.producer.getMessages('nexusgrid.events.dlq');

    const report: ReplayReport = {
      scanned: dlqMessages.length,
      eligible: 0,
      replayed: 0,
      dryRun,
      events: [],
    };

    const remainingInDlq: KafkaEventMessage[] = [];

    for (const msg of dlqMessages) {
      const msgTenant = msg.headers['x-tenant-id'];
      if (options.tenantId && msgTenant !== options.tenantId) {
        remainingInDlq.push(msg);
        continue;
      }

      report.eligible++;
      report.events.push({
        eventId: msg.headers['x-event-id'],
        tenantId: msgTenant,
        originalTopic: msg.headers['x-original-topic'],
        errorReason: msg.headers['x-error-reason'],
      });

      if (!dryRun) {
        // Reset retry headers and publish back to target topic
        try {
          const parsed = JSON.parse(msg.value) as EventEnvelope;
          await this.producer.publish(parsed, msg.headers['x-correlation-id'] || 'replayed', targetTopic);
          report.replayed++;
        } catch {
          remainingInDlq.push(msg);
        }
      } else {
        remainingInDlq.push(msg);
      }
    }

    if (!dryRun) {
      this.producer.publishedMessages.set('nexusgrid.events.dlq', remainingInDlq);
    }

    return report;
  }
}

