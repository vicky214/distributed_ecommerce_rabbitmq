import { ChannelWrapper } from './RabbitMQClient.js';
import crypto from 'node:crypto';

export class RpcClient {
  private replyQueue: string = '';
  private pendingRequests = new Map<
    string,
    { resolve: (data: any) => void; reject: (err: any) => void; timer: NodeJS.Timeout }
  >();

  constructor(private channel: ChannelWrapper) {}

  async init(): Promise<void> {

    const q = await this.channel.assertQueue('', { exclusive: true, autoDelete: true });
    this.replyQueue = q.queue;

    await this.channel.consume(
      this.replyQueue,
      (msg) => {
        if (!msg) return;

        const correlationId = msg.properties.correlationId;
        const pending = this.pendingRequests.get(correlationId);

        if (pending) {
          clearTimeout(pending.timer);
          this.pendingRequests.delete(correlationId);

          try {
            const data = JSON.parse(msg.content.toString());
            pending.resolve(data);
          } catch (err) {
            pending.reject(err);
          }
        }
        this.channel.ack(msg);
      },
      { noAck: false }
    );
  }

  async call<TRequest, TResponse>(
    exchange: string,
    routingKey: string,
    payload: TRequest,
    timeoutMs: number = 5000
  ): Promise<TResponse> {
    if (!this.replyQueue) {
      await this.init();
    }

    const correlationId = crypto.randomUUID();

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingRequests.delete(correlationId);
        reject(new Error(`RPC request timeout after ${timeoutMs}ms for ${routingKey}`));
      }, timeoutMs);

      this.pendingRequests.set(correlationId, { resolve, reject, timer });

      this.channel.publish(
        exchange,
        routingKey,
        Buffer.from(JSON.stringify(payload)),
        {
          correlationId,
          replyTo: this.replyQueue,
          contentType: 'application/json',
        }
      );
    });
  }
}
