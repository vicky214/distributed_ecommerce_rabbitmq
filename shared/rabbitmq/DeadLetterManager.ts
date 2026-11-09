import { ChannelWrapper } from './RabbitMQClient.js';
import { QUEUES } from './ExchangeTypes.js';

export interface DeadLetterRecord {
  payload: any;
  reason: string;
  deathCount: number;
  originalExchange?: string;
  originalRoutingKey?: string;
  deadLetteredAt: string;
}

export class DeadLetterManager {
  private deadLetterMessages: DeadLetterRecord[] = [];

  constructor(private channel: ChannelWrapper) {}

  async startMonitoring(onDeadLetter?: (record: DeadLetterRecord) => void): Promise<void> {
    await this.channel.consume(QUEUES.DEAD_LETTER, (msg) => {
      if (!msg) return;

      try {
        const payload = JSON.parse(msg.content.toString());
        const xDeath = msg.properties.headers?.['x-death']?.[0] || {};

        const record: DeadLetterRecord = {
          payload,
          reason: xDeath.reason || 'rejected',
          deathCount: xDeath.count || 1,
          originalExchange: xDeath.exchange || msg.fields.exchange,
          originalRoutingKey: xDeath['routing-keys']?.[0] || msg.fields.routingKey,
          deadLetteredAt: xDeath.time || new Date().toISOString(),
        };

        this.deadLetterMessages.push(record);
        console.warn(`[DLQ ALERT] Message Dead-Lettered: ${record.reason} | Original Key: ${record.originalRoutingKey}`);

        if (onDeadLetter) {
          onDeadLetter(record);
        }

        this.channel.ack(msg);
      } catch (err) {
        console.error('[DLQ] Error processing dead letter message', err);
        this.channel.ack(msg);
      }
    });
  }

  getDeadLetterMessages(): DeadLetterRecord[] {
    return [...this.deadLetterMessages];
  }

  clear(): void {
    this.deadLetterMessages = [];
  }
}
