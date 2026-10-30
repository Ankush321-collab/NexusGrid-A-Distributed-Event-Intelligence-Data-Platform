export type FailureType = 'RETRIABLE' | 'NON_RETRIABLE_POISON' | 'TERMINAL_EXHAUSTED';

export interface ClassifiedFailure {
  type: FailureType;
  retryCount: number;
  targetTopic: string;
  reason: string;
  stack?: string;
}

export const MAX_RETRY_ATTEMPTS = 5;

export const RETRY_TOPICS = [
  'nexusgrid.events.retry.10s',
  'nexusgrid.events.retry.1m',
  'nexusgrid.events.retry.5m',
];

export const DLQ_TOPIC = 'nexusgrid.events.dlq';

export class ErrorClassifier {
  /**
   * Determines failure category based on error type and retry count
   */
  public classify(error: Error | any, currentRetryCount: number = 0): ClassifiedFailure {
    const errorMsg = error?.message || String(error);
    const stack = error?.stack;

    // Check for non-retriable poison failures:
    // JSON syntax error, schema deserialization crash, fatal constraint violation
    const isPoison =
      error instanceof SyntaxError ||
      errorMsg.includes('Unexpected token') ||
      errorMsg.includes('SyntaxError') ||
      errorMsg.includes('CORRUPTED_PAYLOAD') ||
      errorMsg.includes('DESERIALIZATION_FAILED');

    if (isPoison) {
      return {
        type: 'NON_RETRIABLE_POISON',
        retryCount: currentRetryCount,
        targetTopic: DLQ_TOPIC,
        reason: `Non-retriable poison message: ${errorMsg}`,
        stack,
      };
    }

    // Check retry exhaustion
    if (currentRetryCount >= MAX_RETRY_ATTEMPTS) {
      return {
        type: 'TERMINAL_EXHAUSTED',
        retryCount: currentRetryCount,
        targetTopic: DLQ_TOPIC,
        reason: `Exhausted all ${MAX_RETRY_ATTEMPTS} retries. Final error: ${errorMsg}`,
        stack,
      };
    }

    // Retriable network / downstream DB blip
    const nextRetryCount = currentRetryCount + 1;
    const retryTopicIndex = Math.min(nextRetryCount - 1, RETRY_TOPICS.length - 1);
    const targetTopic = RETRY_TOPICS[retryTopicIndex];

    return {
      type: 'RETRIABLE',
      retryCount: nextRetryCount,
      targetTopic,
      reason: `Retriable failure (${errorMsg}), scheduled for ${targetTopic}`,
      stack,
    };
  }
}

export const defaultErrorClassifier = new ErrorClassifier();

