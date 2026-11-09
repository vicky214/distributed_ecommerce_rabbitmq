import { ChannelWrapper } from './RabbitMQClient.js';
import { EXCHANGES, ROUTING_KEYS } from './ExchangeTypes.js';

export interface RetryOptions {
  maxRetries?: number;
  initialDelayMs?: number;
  backoffMultiplier?: number;
}

export class RetryManager {
  private maxRetries: number;

  constructor(
    private channel: ChannelWrapper,
    options: RetryOptions = {}
  ) {
    this.maxRetries = options.maxRetries || 3;
  }

  async handleFailure(
    originalMsg: any,
    targetExchange: string,
    targetRoutingKey: string,
    errorReason: string
  ): Promise<'retried' | 'dead_lettered'> {
    const headers = { ...(originalMsg.properties.headers || {}) };
    const currentRetries = (headers['x-retry-count'] as number) || 0;

    if (currentRetries < this.maxRetries) {
      const nextRetryCount = currentRetries + 1;
      headers['x-retry-count'] = nextRetryCount;
      headers['x-last-error'] = errorReason;

      console.warn(
        `[RetryManager] Attempt ${nextRetryCount}/${this.maxRetries} for message. Routing to delayed retry queue...`
      );

      await this.channel.publish(
        EXCHANGES.RETRY,
        'retry.3s',
        originalMsg.content,
        {
          ...originalMsg.properties,
          headers,
        }
      );

      this.channel.ack(originalMsg);
      return 'retried';
    } else {

      console.error(
        `[RetryManager] Exhausted all ${this.maxRetries} retries! Moving message to terminal DLQ.`
      );

      headers['x-exhausted-retries'] = true;
      headers['x-final-error'] = errorReason;

      await this.channel.publish(
        EXCHANGES.DEAD_LETTER,
        ROUTING_KEYS.DLQ_ROUTING,
        originalMsg.content,
        {
          ...originalMsg.properties,
          headers,
        }
      );

      this.channel.ack(originalMsg);
      return 'dead_lettered';
    }
  }
}
