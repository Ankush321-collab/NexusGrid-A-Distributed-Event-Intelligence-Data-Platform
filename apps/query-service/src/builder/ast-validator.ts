import type { QueryFilter, StructuredQueryParams } from '@nexusgrid/database';

export interface ValidatedQueryPlan {
  valid: boolean;
  error?: string;
  code?: string;
  params?: StructuredQueryParams;
}

const ALLOWED_METRIC_PREFIXES = ['count()', 'avg(', 'p95(', 'sum(', 'min(', 'max('];
const ALLOWED_OPERATORS = new Set(['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'in']);

// Strict identifier regex to prevent SQL injection in column names
const SAFE_IDENTIFIER_REGEX = /^[a-zA-Z0-9_.]+$/;

export class ASTValidator {
  private maxScanDays: number;
  private maxTimeRangeMs: number;

  constructor(maxScanDays: number = 90) {
    this.maxScanDays = maxScanDays;
    this.maxTimeRangeMs = maxScanDays * 24 * 3600 * 1000;
  }

  /**
   * Validates analytical query parameters against strict allowlists and scan boundaries
   */
  public validate(raw: any, authenticatedTenantId: string): ValidatedQueryPlan {
    if (!raw || typeof raw !== 'object') {
      return { valid: false, code: 'ERR_INVALID_BODY', error: 'Query body must be an object' };
    }

    if (!raw.project_id || typeof raw.project_id !== 'string') {
      return { valid: false, code: 'ERR_MISSING_PROJECT', error: 'project_id is required' };
    }

    if (!SAFE_IDENTIFIER_REGEX.test(raw.project_id)) {
      return { valid: false, code: 'ERR_SQL_INJECTION_DETECTED', error: 'project_id contains disallowed characters' };
    }

    if (!raw.time_range || typeof raw.time_range !== 'object') {
      return { valid: false, code: 'ERR_MISSING_TIME_RANGE', error: 'time_range { start, end } is required' };
    }

    const startMs = Date.parse(raw.time_range.start);
    const endMs = Date.parse(raw.time_range.end);

    if (isNaN(startMs) || isNaN(endMs)) {
      return { valid: false, code: 'ERR_INVALID_TIMESTAMP', error: 'time_range start and end must be ISO 8601 strings' };
    }

    if (startMs >= endMs) {
      return { valid: false, code: 'ERR_INVALID_TIME_WINDOW', error: 'time_range.start must be strictly before time_range.end' };
    }

    // Time-range bound protection (Max configured days)
    if (endMs - startMs > this.maxTimeRangeMs) {
      return {
        valid: false,
        code: 'ERR_SCAN_COST_LIMIT_EXCEEDED',
        error: `Query time range exceeds maximum allowed limit of ${this.maxScanDays} days (${Math.round((endMs - startMs) / (24 * 3600 * 1000))} days requested)`,
      };
    }

    // Validate Metrics allowlist
    const metrics: string[] | undefined = raw.metrics;
    if (metrics && Array.isArray(metrics)) {
      for (const m of metrics) {
        const isAllowed = ALLOWED_METRIC_PREFIXES.some((prefix) => m.startsWith(prefix));
        if (!isAllowed) {
          return {
            valid: false,
            code: 'ERR_DISALLOWED_METRIC',
            error: `Metric expression "${m}" is not permitted. Only predefined functions [count, avg, p95, sum, min, max] are allowed.`,
          };
        }
        // Check SQL injection characters in metric expression
        if (/['";\-/*]/.test(m)) {
          return { valid: false, code: 'ERR_SQL_INJECTION_DETECTED', error: `Disallowed characters detected in metric "${m}"` };
        }
      }
    }

    // Validate Group By
    const groupBy: string[] = raw.group_by || [];
    for (const g of groupBy) {
      if (!SAFE_IDENTIFIER_REGEX.test(g)) {
        return { valid: false, code: 'ERR_INVALID_GROUP_BY', error: `Group by column "${g}" contains invalid characters` };
      }
    }

    // Validate Filters
    const validatedFilters: QueryFilter[] = [];
    if (raw.filters && Array.isArray(raw.filters)) {
      for (const f of raw.filters) {
        if (!f.field || !SAFE_IDENTIFIER_REGEX.test(f.field)) {
          return { valid: false, code: 'ERR_INVALID_FILTER_FIELD', error: `Filter field "${f.field}" contains disallowed characters` };
        }
        if (!ALLOWED_OPERATORS.has(f.op)) {
          return { valid: false, code: 'ERR_INVALID_FILTER_OP', error: `Filter operator "${f.op}" is not allowed` };
        }
        if (typeof f.value === 'string' && /['";\-/*]/.test(f.value)) {
          return { valid: false, code: 'ERR_SQL_INJECTION_DETECTED', error: `Filter value contains dangerous SQL injection characters` };
        }
        validatedFilters.push({
          field: f.field,
          op: f.op,
          value: f.value,
        });
      }
    }

    const structuredParams: StructuredQueryParams = {
      tenant_id: authenticatedTenantId,
      project_id: raw.project_id,
      start_time: raw.time_range.start,
      end_time: raw.time_range.end,
      event_names: raw.event_names,
      filters: validatedFilters,
      group_by: groupBy.length > 0 ? groupBy : undefined,
      metrics,
      limit: Math.min(1000, raw.limit || 100),
      final: raw.final === true,
    };

    return {
      valid: true,
      params: structuredParams,
    };
  }
}

export const defaultASTValidator = new ASTValidator();
