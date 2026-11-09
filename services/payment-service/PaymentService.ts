import { ChannelWrapper } from '../../shared/rabbitmq/RabbitMQClient.js';
import { EXCHANGES, QUEUES, ROUTING_KEYS } from '../../shared/rabbitmq/ExchangeTypes.js';
import { OrderCreatedEvent } from '../../shared/events/OrderEvents.js';
import { PaymentProcessedEvent } from '../../shared/events/PaymentEvents.js';

export class PaymentService {
  private simulateFailure = false;
  private simulateFatalCrash = false;

  constructor(private channel: ChannelWrapper) {}

  setSimulateFailure(fail: boolean): void {
    this.simulateFailure = fail;
  }

  setSimulateFatalCrash(crash: boolean): void {
    this.simulateFatalCrash = crash;
  }

  async start(): Promise<void> {
    console.log('[PaymentService] Initializing Payment Processor Consumers...');

    await this.channel.prefetch(1);

    await this.channel.consume(QUEUES.PAYMENT_PROCESS, async (msg) => {
      if (!msg) return;

      try {
        const order: OrderCreatedEvent = JSON.parse(msg.content.toString());
        console.log(`[PaymentService] Processing payment of $${order.totalAmount} for Order: ${order.orderId}...`);

        if (this.simulateFatalCrash) {
          console.error(`[PaymentService] FATAL ERROR: Payment Gateway Timeout / Unrecoverable exception!`);
          this.channel.nack(msg, false, false);
          return;
        }

        if (this.simulateFailure || order.totalAmount > 50000) {
          console.warn(`[PaymentService] Card Declined for Order: ${order.orderId}! Emitting ${ROUTING_KEYS.PAYMENT_FAILED}`);

          const failedEvent: PaymentProcessedEvent = {
            paymentId: `PAY-FAIL-${Date.now()}`,
            orderId: order.orderId,
            customerId: order.customerId,
            amount: order.totalAmount,
            currency: order.currency,
            status: 'FAILED',
            failureReason: 'Card issuer rejected transaction (Insufficient funds)',
            processedAt: Date.now(),
          };

          await this.channel.publish(
            EXCHANGES.PAYMENT_EVENTS,
            ROUTING_KEYS.PAYMENT_FAILED,
            Buffer.from(JSON.stringify(failedEvent)),
            { contentType: 'application/json', persistent: true }
          );

          this.channel.ack(msg);
          return;
        }

        const successEvent: PaymentProcessedEvent = {
          paymentId: `PAY-OK-${Date.now()}`,
          orderId: order.orderId,
          customerId: order.customerId,
          amount: order.totalAmount,
          currency: order.currency,
          status: 'SUCCESS',
          processedAt: Date.now(),
        };

        console.log(`[PaymentService] Payment SUCCESS for Order: ${order.orderId}. Emitting ${ROUTING_KEYS.PAYMENT_SUCCESS}`);

        await this.channel.publish(
          EXCHANGES.PAYMENT_EVENTS,
          ROUTING_KEYS.PAYMENT_SUCCESS,
          Buffer.from(JSON.stringify(successEvent)),
          { contentType: 'application/json', persistent: true }
        );

        this.channel.ack(msg);
      } catch (err) {
        console.error('[PaymentService] Error in payment consumer', err);
        this.channel.nack(msg, false, false);
      }
    });
  }
}
