import { ChannelWrapper } from '../../shared/rabbitmq/RabbitMQClient.js';
import { EXCHANGES, QUEUES, ROUTING_KEYS } from '../../shared/rabbitmq/ExchangeTypes.js';
import { OrderCreatedEvent } from '../../shared/events/OrderEvents.js';
import {
  InventoryReservedEvent,
  InventoryReservationFailedEvent,
  InventoryReleasedEvent,
} from '../../shared/events/InventoryEvents.js';

interface StockItem {
  productId: string;
  name: string;
  availableStock: number;
  reservedStock: number;
}

export class ProductService {
  private inventory = new Map<string, StockItem>();
  private reservations = new Map<string, { orderId: string; items: { productId: string; quantity: number }[] }>();
  private cancelledOrders = new Set<string>();

  constructor(private channel: ChannelWrapper) {
    this.seedCatalog();
  }

  private seedCatalog(): void {
    this.inventory.set('PROD-IPHONE', {
      productId: 'PROD-IPHONE',
      name: 'iPhone 16 Pro Max',
      availableStock: 50,
      reservedStock: 0,
    });
    this.inventory.set('PROD-MACBOOK', {
      productId: 'PROD-MACBOOK',
      name: 'MacBook Pro M4 Max',
      availableStock: 10,
      reservedStock: 0,
    });
    this.inventory.set('PROD-AIRPODS', {
      productId: 'PROD-AIRPODS',
      name: 'AirPods Pro 2',
      availableStock: 100,
      reservedStock: 0,
    });
  }

  async start(): Promise<void> {
    console.log('[ProductService] Initializing Product & Inventory Consumers...');

    await this.channel.consume(QUEUES.INVENTORY_RESERVE, async (msg) => {
      if (!msg) return;

      try {
        const orderEvent: OrderCreatedEvent = JSON.parse(msg.content.toString());
        await this.handleOrderCreated(orderEvent);
        this.channel.ack(msg);
      } catch (err) {
        console.error('[ProductService] Error reserving inventory', err);
        this.channel.nack(msg, false, false);
      }
    });

    await this.channel.consume(QUEUES.INVENTORY_RELEASE, async (msg) => {
      if (!msg) return;

      try {
        const payload = JSON.parse(msg.content.toString());
        await this.handlePaymentFailed(payload.orderId);
        this.channel.ack(msg);
      } catch (err) {
        console.error('[ProductService] Error releasing inventory', err);
        this.channel.ack(msg);
      }
    });
  }

  private async handleOrderCreated(order: OrderCreatedEvent): Promise<void> {
    if (this.cancelledOrders.has(order.orderId)) {
      console.warn(`[ProductService] Order ${order.orderId} was already cancelled by payment failure. Skipping stock reservation.`);
      return;
    }

    console.log(`[ProductService] Checking inventory for Order: ${order.orderId}...`);

    let canReserve = true;
    for (const item of order.items) {
      const stock = this.inventory.get(item.productId);
      if (!stock || stock.availableStock < item.quantity) {
        canReserve = false;
        break;
      }
    }

    if (canReserve) {

      for (const item of order.items) {
        const stock = this.inventory.get(item.productId)!;
        stock.availableStock -= item.quantity;
        stock.reservedStock += item.quantity;
      }

      const reservationId = `RES-${Date.now()}`;
      this.reservations.set(order.orderId, { orderId: order.orderId, items: order.items });

      const reservedEvent: InventoryReservedEvent = {
        orderId: order.orderId,
        items: order.items,
        reservationId,
        reservedAt: Date.now(),
      };

      console.log(`[ProductService] Stock RESERVED for Order: ${order.orderId}. Emitting ${ROUTING_KEYS.INVENTORY_RESERVED}`);

      await this.channel.publish(
        EXCHANGES.INVENTORY_EVENTS,
        ROUTING_KEYS.INVENTORY_RESERVED,
        Buffer.from(JSON.stringify(reservedEvent)),
        { contentType: 'application/json', persistent: true }
      );
    } else {
      console.warn(`[ProductService] Out of stock for Order: ${order.orderId}! Emitting ${ROUTING_KEYS.INVENTORY_FAILED}`);

      const failedEvent: InventoryReservationFailedEvent = {
        orderId: order.orderId,
        items: order.items,
        reason: 'Insufficient stock in warehouse',
        failedAt: Date.now(),
      };

      await this.channel.publish(
        EXCHANGES.INVENTORY_EVENTS,
        ROUTING_KEYS.INVENTORY_FAILED,
        Buffer.from(JSON.stringify(failedEvent)),
        { contentType: 'application/json', persistent: true }
      );
    }
  }

  private async handlePaymentFailed(orderId: string): Promise<void> {
    this.cancelledOrders.add(orderId);
    const reservation = this.reservations.get(orderId);
    if (!reservation) {
      console.warn(`[ProductService] Payment failed for Order ${orderId} before reservation completed. Order marked as cancelled.`);
      return;
    }

    console.warn(`[ProductService] SAGA COMPENSATION: Payment failed for Order ${orderId}. Releasing reserved stock!`);

    for (const item of reservation.items) {
      const stock = this.inventory.get(item.productId);
      if (stock) {
        stock.availableStock += item.quantity;
        stock.reservedStock -= item.quantity;
      }
    }

    this.reservations.delete(orderId);

    const releasedEvent: InventoryReleasedEvent = {
      orderId,
      reservationId: `REL-${Date.now()}`,
      reason: 'Payment transaction failed',
      releasedAt: Date.now(),
    };

    await this.channel.publish(
      EXCHANGES.INVENTORY_EVENTS,
      'inventory.released',
      Buffer.from(JSON.stringify(releasedEvent)),
      { contentType: 'application/json' }
    );
  }

  getStock(productId: string): StockItem | undefined {
    return this.inventory.get(productId);
  }
}
