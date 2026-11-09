import { ChannelWrapper } from './RabbitMQClient.js';

export class RpcServer {
  constructor(private channel: ChannelWrapper) {}

  async respond<TRequest, TResponse>(
    queueName: string,
    handler: (request: TRequest) => Promise<TResponse> | TResponse
  ): Promise<void> {
    await this.channel.assertQueue(queueName, { durable: true });
    await this.channel.prefetch(1);

    await this.channel.consume(queueName, async (msg) => {
      if (!msg) return;

      const replyTo = msg.properties.replyTo;
      const correlationId = msg.properties.correlationId;

      if (!replyTo) {
        console.warn(`[RpcServer] Received RPC request without replyTo property. Skipping.`);
        this.channel.ack(msg);
        return;
      }

      try {
        const requestData = JSON.parse(msg.content.toString()) as TRequest;
        const responseData = await handler(requestData);

        await this.channel.sendToQueue(
          replyTo,
          Buffer.from(JSON.stringify(responseData)),
          {
            correlationId,
            contentType: 'application/json',
          }
        );
      } catch (err: any) {
        console.error(`[RpcServer] Error handling RPC request`, err);

        await this.channel.sendToQueue(
          replyTo,
          Buffer.from(JSON.stringify({ error: err.message || 'Internal RPC Error' })),
          {
            correlationId,
            contentType: 'application/json',
          }
        );
      } finally {
        this.channel.ack(msg);
      }
    });
  }
}
