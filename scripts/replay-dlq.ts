import { KafkaProducer } from '../apps/ingestion-service/src/producer/kafka-producer.js';
import type { KafkaEventMessage, EventEnvelope } from '@nexusgrid/contracts';

export interface ReplayOptions {
  dryRun?: boolean;
  tenantId?: string;
  maxEvents?: number;
  targetTopic?: string;
  maxReplayAttempts?: number;
}

export interface ReplayReport {
  scanned: number;
  eligible: number;
  replayed: number;
  quarantined: number;
  dryRun: boolean;
  events: Array<{
    eventId: string;
    tenantId: string;
    originalTopic?: string;
    errorReason?: string;
    replayCount?: number;
    quarantined?: boolean;
  }>;
}

export class DLQReplayTool {
  constructor(private producer: KafkaProducer) {}

  public async runReplay(options: ReplayOptions = {}): Promise<ReplayReport> {
    const dryRun = options.dryRun ?? true;
    const targetTopic = options.targetTopic || 'nexusgrid.events.raw';
    const maxReplayAttempts = options.maxReplayAttempts ?? 2;
    const dlqMessages = this.producer.getMessages('nexusgrid.events.dlq');

    const report: ReplayReport = {
      scanned: dlqMessages.length,
      eligible: 0,
      replayed: 0,
      quarantined: 0,
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

      const currentReplays = parseInt(msg.headers['x-replay-count'] || '0', 10);

      // Problem 5 Solution: DLQ Replay Circuit Breaker
      // If a message has failed replay >= maxReplayAttempts, quarantine it to break infinite loop
      if (currentReplays >= maxReplayAttempts) {
        report.quarantined++;
        report.events.push({
          eventId: msg.headers['x-event-id'],
          tenantId: msgTenant,
          originalTopic: msg.headers['x-original-topic'],
          errorReason: 'QUARANTINED_CIRCUIT_BREAKER_EXCEEDED_MAX_REPLAYS',
          replayCount: currentReplays,
          quarantined: true,
        });

        if (!dryRun) {
          const quarantinedMsg: KafkaEventMessage = {
            ...msg,
            headers: {
              ...msg.headers,
              'x-quarantine-reason': 'MAX_REPLAY_EXCEEDED_CIRCUIT_BREAKER',
              'x-quarantined-at': new Date().toISOString(),
            },
          };
          if (!this.producer.publishedMessages.has('nexusgrid.events.quarantine')) {
            this.producer.publishedMessages.set('nexusgrid.events.quarantine', []);
          }
          this.producer.publishedMessages.get('nexusgrid.events.quarantine')!.push(quarantinedMsg);
        } else {
          remainingInDlq.push(msg);
        }
        continue;
      }

      report.eligible++;
      report.events.push({
        eventId: msg.headers['x-event-id'],
        tenantId: msgTenant,
        originalTopic: msg.headers['x-original-topic'],
        errorReason: msg.headers['x-error-reason'],
        replayCount: currentReplays,
        quarantined: false,
      });

      if (!dryRun) {
        // Reset retry headers, increment replay count, and publish back to target topic
        try {
          const parsed = JSON.parse(msg.value) as EventEnvelope;
          await this.producer.publish(parsed, msg.headers['x-correlation-id'] || 'replayed', targetTopic);
          const publishedList = this.producer.publishedMessages.get(targetTopic);
          if (publishedList && publishedList.length > 0) {
            publishedList[publishedList.length - 1].headers['x-replay-count'] = (currentReplays + 1).toString();
          }
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

