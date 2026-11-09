import { test, describe, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';
import { TopologyManager } from '../shared/rabbitmq/TopologyManager.js';
import { ProductService } from '../services/product-service/ProductService.js';
import { PaymentService } from '../services/payment-service/PaymentService.js';
import { CustomerService } from '../services/customer-service/CustomerService.js';
import { NotificationService } from '../services/notification-service/NotificationService.js';
import { OrderService } from '../services/order-service/OrderService.js';

describe('E-Commerce Microservices Saga Integration', () => {
  let client: RabbitMQClient;
  let channel: any;
  let productService: ProductService;
  let paymentService: PaymentService;
  let customerService: CustomerService;
  let notificationService: NotificationService;
  let orderService: OrderService;

  beforeEach(async () => {
    RabbitMQClient.getVirtualBroker().reset();
    client = new RabbitMQClient();
    await client.connect(false);
    channel = await client.createChannel();

    await TopologyManager.setupEnterpriseTopology(channel);

    productService = new ProductService(channel);
    paymentService = new PaymentService(channel);
    customerService = new CustomerService(channel);
    notificationService = new NotificationService(channel);
    orderService = new OrderService(channel);

    await productService.start();
    await paymentService.start();
    await customerService.start();
    await notificationService.start();
    await orderService.start();
  });

  test('Happy Path: Order checkout transitions to CONFIRMED and reserves inventory', async () => {
    const initialIphoneStock = productService.getStock('PROD-IPHONE')!.availableStock;

    const order = await orderService.createOrder({
      customerId: 'CUST-101',
      customerTier: 'vip',
      shippingAddress: '123 Market St',
      items: [{ productId: 'PROD-IPHONE', name: 'iPhone 16', quantity: 2, price: 1000 }],
    });

    await new Promise((r) => setTimeout(r, 100));

    const finalOrder = orderService.getOrder(order.orderId);
    assert.equal(finalOrder?.state, 'CONFIRMED');
    assert.equal(finalOrder?.paymentReceived, true);
    assert.equal(finalOrder?.inventoryReserved, true);

    const remainingStock = productService.getStock('PROD-IPHONE')!.availableStock;
    assert.equal(remainingStock, initialIphoneStock - 2);
  });

  test('Compensating Rollback: Payment failure triggers cancellation and stock release', async () => {
    paymentService.setSimulateFailure(true);
    const initialMacbookStock = productService.getStock('PROD-MACBOOK')!.availableStock;

    const order = await orderService.createOrder({
      customerId: 'CUST-102',
      customerTier: 'standard',
      shippingAddress: '456 Tech Ave',
      items: [{ productId: 'PROD-MACBOOK', name: 'MacBook', quantity: 1, price: 3000 }],
    });

    await new Promise((r) => setTimeout(r, 100));

    const finalOrder = orderService.getOrder(order.orderId);
    assert.equal(finalOrder?.state, 'CANCELLED');

    const remainingStock = productService.getStock('PROD-MACBOOK')!.availableStock;
    assert.equal(remainingStock, initialMacbookStock);
  });
});
