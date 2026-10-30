export interface BatcherOptions<T> {
  maxBatchSize: number;
  flushIntervalMs: number;
  onFlush: (batch: T[]) => Promise<void>;
}

export class EventBatcher<T> {
  private buffer: T[] = [];
  private timer: NodeJS.Timeout | null = null;
  private maxBatchSize: number;
  private flushIntervalMs: number;
  private onFlush: (batch: T[]) => Promise<void>;
  private isFlushing: boolean = false;

  constructor(options: BatcherOptions<T>) {
    this.maxBatchSize = options.maxBatchSize;
    this.flushIntervalMs = options.flushIntervalMs;
    this.onFlush = options.onFlush;
  }

  public async push(item: T): Promise<void> {
    this.buffer.push(item);

    if (this.buffer.length >= this.maxBatchSize) {
      await this.flush();
    } else if (!this.timer) {
      this.timer = setTimeout(() => {
        this.flush().catch(() => {});
      }, this.flushIntervalMs);
    }
  }

  public async flush(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    if (this.buffer.length === 0 || this.isFlushing) {
      return;
    }

    this.isFlushing = true;
    const batch = this.buffer.slice();
    this.buffer = [];

    try {
      await this.onFlush(batch);
    } finally {
      this.isFlushing = false;
    }
  }

  public getPendingCount(): number {
    return this.buffer.length;
  }
}

