/**
 * NexusGrid Prometheus Metrics Registry & Collector
 */

export interface MetricLabels {
  tenant_id?: string;
  project_id?: string;
  method?: string;
  route?: string;
  status_code?: string | number;
  topic?: string;
  partition?: string | number;
  error_type?: string;
  [key: string]: unknown;
}

export class MetricsRegistry {
  private counters: Map<string, number> = new Map();
  private histograms: Map<string, number[]> = new Map();

  private keyOf(name: string, labels: MetricLabels = {}): string {
    const labelPairs = Object.entries(labels)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}="${v}"`)
      .join(',');
    return labelPairs ? `${name}{${labelPairs}}` : name;
  }

  public incrementCounter(name: string, value: number = 1, labels: MetricLabels = {}): void {
    const key = this.keyOf(name, labels);
    const curr = this.counters.get(key) || 0;
    this.counters.set(key, curr + value);
  }

  public observeDuration(name: string, durationMs: number, labels: MetricLabels = {}): void {
    const key = this.keyOf(name, labels);
    const list = this.histograms.get(key) || [];
    list.push(durationMs);
    this.histograms.set(key, list);
  }

  public getCounter(name: string, labels: MetricLabels = {}): number {
    const key = this.keyOf(name, labels);
    return this.counters.get(key) || 0;
  }

  public getHistogramStats(name: string, labels: MetricLabels = {}): { count: number; p50: number; p95: number; p99: number; avg: number } {
    const key = this.keyOf(name, labels);
    const values = (this.histograms.get(key) || []).slice().sort((a, b) => a - b);
    if (values.length === 0) {
      return { count: 0, p50: 0, p95: 0, p99: 0, avg: 0 };
    }
    const sum = values.reduce((acc, v) => acc + v, 0);
    const p50 = values[Math.floor(values.length * 0.5)] ?? 0;
    const p95 = values[Math.floor(values.length * 0.95)] ?? 0;
    const p99 = values[Math.floor(values.length * 0.99)] ?? 0;
    return {
      count: values.length,
      p50,
      p95,
      p99,
      avg: Math.round((sum / values.length) * 100) / 100,
    };
  }

  public scrapePrometheusText(): string {
    const lines: string[] = [];
    lines.push('# HELP nexusgrid_http_requests_total Total number of HTTP requests processed');
    lines.push('# TYPE nexusgrid_http_requests_total counter');
    for (const [key, val] of this.counters.entries()) {
      lines.push(`${key} ${val}`);
    }
    return lines.join('\n');
  }

  public reset(): void {
    this.counters.clear();
    this.histograms.clear();
  }
}

export const defaultMetrics = new MetricsRegistry();

