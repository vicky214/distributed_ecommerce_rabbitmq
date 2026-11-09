import { ChannelWrapper } from '../../shared/rabbitmq/RabbitMQClient.js';
import { EXCHANGES, ROUTING_KEYS } from '../../shared/rabbitmq/ExchangeTypes.js';
import {
  OrderCreatedEvent,
  OrderItem,
  OrderCancelledEvent,
  OrderCompletedEvent,
} from '../../shared/events/OrderEvents.js';
import { NotificationBroadcastEvent } from '../../shared/events/NotificationEvents.js';

export type OrderState = 'PENDING' | 'CONFIRMED' | 'CANCELLED';

export interface OrderRecord {
  orderId: string;
  customerId: string;
  customerTier: 'standard' | 'gold' | 'vip';
  items: OrderItem[];
  totalAmount: number;
  state: OrderState;
  paymentReceived: boolean;
  inventoryReserved: boolean;
  createdAt: number;
  updatedAt: number;
}

export class OrderService {
  private orders = new Map<string, OrderRecord>();

  constructor(private channel: ChannelWrapper) {}

  async start(): Promise<void> {
    console.log('[OrderService] Initializing Order Orchestration Listeners...');

    const invQ = await this.channel.assertQueue('queue.orders.inventory.status', { durable: true });
    await this.channel.bindQueue(invQ.queue, EXCHANGES.INVENTORY_EVENTS, ROUTING_KEYS.INVENTORY_RESERVED);

    await this.channel.consume(invQ.queue, async (msg) => {
      if (!msg) return;
      try {
        const payload = JSON.parse(msg.content.toString());
        const order = this.orders.get(payload.orderId);
        if (order) {
          order.inventoryReserved = true;
          order.updatedAt = Date.now();
          console.log(`[OrderService] Order ${order.orderId}: Inventory reserved confirmed.`);
          await this.evaluateOrderState(order);
        }
        this.channel.ack(msg);
      } catch (err) {
        this.channel.ack(msg);
      }
    });

    const payOkQ = await this.channel.assertQueue('queue.orders.payment.status', { durable: true });
    await this.channel.bindQueue(payOkQ.queue, EXCHANGES.PAYMENT_EVENTS, ROUTING_KEYS.PAYMENT_SUCCESS);

    await this.channel.consume(payOkQ.queue, async (msg) => {
      if (!msg) return;
      try {
        const payload = JSON.parse(msg.content.toString());
        const order = this.orders.get(payload.orderId);
        if (order) {
          order.paymentReceived = true;
          order.updatedAt = Date.now();
          console.log(`[OrderService] Order ${order.orderId}: Payment received confirmed.`);
          await this.evaluateOrderState(order);
        }
        this.channel.ack(msg);
      } catch (err) {
        this.channel.ack(msg);
      }
    });

    const payFailQ = await this.channel.assertQueue('queue.orders.payment.fail', { durable: true });
    await this.channel.bindQueue(payFailQ.queue, EXCHANGES.PAYMENT_EVENTS, ROUTING_KEYS.PAYMENT_FAILED);

    await this.channel.consume(payFailQ.queue, async (msg) => {
      if (!msg) return;
      try {
        const payload = JSON.parse(msg.content.toString());
        const order = this.orders.get(payload.orderId);
        if (order && order.state === 'PENDING') {
          order.state = 'CANCELLED';
          order.updatedAt = Date.now();
          console.warn(`[OrderService] Order ${order.orderId}: SAGA ROLLBACK - Payment failed. Order CANCELLED!`);

          const notif: NotificationBroadcastEvent = {
            notificationId: `NOTIF-${Date.now()}`,
            type: 'ORDER_CANCELLED',
            title: `Order #${order.orderId} Cancelled`,
            body: `Your order was cancelled due to payment decline: ${payload.failureReason}`,
            timestamp: Date.now(),
          };

          await this.channel.publish(
            EXCHANGES.NOTIFICATIONS_FANOUT,
            '',
            Buffer.from(JSON.stringify(notif))
          );
        }
        this.channel.ack(msg);
      } catch (err) {
        this.channel.ack(msg);
      }
    });
  }

  async createOrder(params: {
    customerId: string;
    customerTier: 'standard' | 'gold' | 'vip';
    items: OrderItem[];
    shippingAddress: string;
  }): Promise<OrderRecord> {
    const orderId = `ORD-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
    const totalAmount = params.items.reduce((sum, item) => sum + item.price * item.quantity, 0);

    const record: OrderRecord = {
      orderId,
      customerId: params.customerId,
      customerTier: params.customerTier,
      items: params.items,
      totalAmount,
      state: 'PENDING',
      paymentReceived: false,
      inventoryReserved: false,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };

    this.orders.set(orderId, record);

    const event: OrderCreatedEvent = {
      orderId,
      customerId: params.customerId,
      customerTier: params.customerTier,
      items: params.items,
      totalAmount,
      currency: 'USD',
      shippingAddress: params.shippingAddress,
      createdAt: Date.now(),
    };

    console.log(`\n[OrderService] Created Order ${orderId} ($${totalAmount}). Publishing to RabbitMQ...`);

    await this.channel.publish(
      EXCHANGES.ORDER_EVENTS,
      ROUTING_KEYS.ORDER_CREATED,
      Buffer.from(JSON.stringify(event)),
      {
        contentType: 'application/json',
        persistent: true,
        priority: params.customerTier === 'vip' ? 9 : 1,
      }
    );

    if (params.customerTier === 'vip') {
      await this.channel.publish(
        EXCHANGES.ORDER_EVENTS,
        'order.vip.created',
        Buffer.from(JSON.stringify(event)),
        { priority: 9 }
      );
    }

    const notif: NotificationBroadcastEvent = {
      notificationId: `NOTIF-${Date.now()}`,
      type: 'ORDER_CONFIRMATION',
      title: `Order Confirmed: #${orderId}`,
      body: `Thank you for your order of $${totalAmount}! We are preparing it for shipment.`,
      timestamp: Date.now(),
    };

    await this.channel.publish(
      EXCHANGES.NOTIFICATIONS_FANOUT,
      '',
      Buffer.from(JSON.stringify(notif))
    );

    return record;
  }

  private async evaluateOrderState(order: OrderRecord): Promise<void> {
    if (order.paymentReceived && order.inventoryReserved && order.state === 'PENDING') {
      order.state = 'CONFIRMED';
      console.log(`\n🎉 [OrderService] ORDER ${order.orderId} FULLY CONFIRMED! (Payment + Inventory Succeeded)\n`);

      const completedEvent: OrderCompletedEvent = {
        orderId: order.orderId,
        customerId: order.customerId,
        totalAmount: order.totalAmount,
        completedAt: Date.now(),
      };

      await this.channel.publish(
        EXCHANGES.ORDER_EVENTS,
        ROUTING_KEYS.ORDER_COMPLETED,
        Buffer.from(JSON.stringify(completedEvent))
      );
    }
  }

  getOrder(orderId: string): OrderRecord | undefined {
    return this.orders.get(orderId);
  }
}
