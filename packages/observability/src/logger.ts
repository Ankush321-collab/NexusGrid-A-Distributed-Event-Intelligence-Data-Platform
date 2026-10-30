export type LogLevel = 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal';

const LOG_LEVEL_SEVERITY: Record<LogLevel, number> = {
  trace: 10,
  debug: 20,
  info: 30,
  warn: 40,
  error: 50,
  fatal: 60,
};

export interface LogContext {
  service?: string;
  trace_id?: string;
  correlation_id?: string;
  tenant_id?: string;
  project_id?: string;
  [key: string]: unknown;
}

export class Logger {
  private service: string;
  private minLevel: LogLevel;
  private defaultContext: LogContext;

  constructor(service: string = 'nexusgrid', minLevel: LogLevel = 'info', defaultContext: LogContext = {}) {
    this.service = service;
    this.minLevel = minLevel;
    this.defaultContext = defaultContext;
  }

  public child(context: LogContext): Logger {
    return new Logger(this.service, this.minLevel, {
      ...this.defaultContext,
      ...context,
    });
  }

  private shouldLog(level: LogLevel): boolean {
    return LOG_LEVEL_SEVERITY[level] >= LOG_LEVEL_SEVERITY[this.minLevel];
  }

  private emit(level: LogLevel, message: string, meta?: Record<string, unknown>): void {
    if (!this.shouldLog(level)) return;

    const payload = {
      timestamp: new Date().toISOString(),
      level,
      service: this.service,
      message,
      ...this.defaultContext,
      ...meta,
    };

    const serialized = JSON.stringify(payload);
    if (level === 'error' || level === 'fatal') {
      console.error(serialized);
    } else if (level === 'warn') {
      console.warn(serialized);
    } else {
      console.log(serialized);
    }
  }

  public trace(message: string, meta?: Record<string, unknown>): void {
    this.emit('trace', message, meta);
  }

  public debug(message: string, meta?: Record<string, unknown>): void {
    this.emit('debug', message, meta);
  }

  public info(message: string, meta?: Record<string, unknown>): void {
    this.emit('info', message, meta);
  }

  public warn(message: string, meta?: Record<string, unknown>): void {
    this.emit('warn', message, meta);
  }

  public error(message: string, meta?: Record<string, unknown>): void {
    this.emit('error', message, meta);
  }

  public fatal(message: string, meta?: Record<string, unknown>): void {
    this.emit('fatal', message, meta);
  }
}

export const defaultLogger = new Logger('nexusgrid-core', (process.env.LOG_LEVEL as LogLevel) || 'info');

