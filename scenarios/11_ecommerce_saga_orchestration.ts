import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';
import { TopologyManager } from '../shared/rabbitmq/TopologyManager.js';
import { DeadLetterManager } from '../shared/rabbitmq/DeadLetterManager.js';
import { RpcClient } from '../shared/rabbitmq/RpcClient.js';
import { ProductService } from '../services/product-service/ProductService.js';
import { PaymentService } from '../services/payment-service/PaymentService.js';
import { CustomerService } from '../services/customer-service/CustomerService.js';
import { NotificationService } from '../services/notification-service/NotificationService.js';
import { OrderService } from '../services/order-service/OrderService.js';
import { CustomerCreditCheckRequest, CustomerCreditCheckResponse } from '../shared/events/CustomerEvents.js';

async function runMasterSimulation() {
  console.log('\n' + '='.repeat(80));
  console.log('   E-COMMERCE MICROSERVICES SAGA CHOREOGRAPHY OVER RABBITMQ');
  console.log('   Multi-Service Interaction with Direct, Topic, Fanout, DLQ, and RPC');
  console.log('='.repeat(80) + '\n');

  const client = new RabbitMQClient();
  await client.connect(false);
  const channel = await client.createChannel();

  await TopologyManager.setupEnterpriseTopology(channel);

  const productService = new ProductService(channel);
  const paymentService = new PaymentService(channel);
  const customerService = new CustomerService(channel);
  const notificationService = new NotificationService(channel);
  const orderService = new OrderService(channel);
  const dlqManager = new DeadLetterManager(channel);
  const rpcClient = new RpcClient(channel);

  await productService.start();
  await paymentService.start();
  await customerService.start();
  await notificationService.start();
  await orderService.start();
  await dlqManager.startMonitoring();
  await rpcClient.init();

  console.log('\n' + '-'.repeat(80));
  console.log('TRANSACTION 1: HAPPY PATH CHECKOUT (Alice - VIP Customer)');
  console.log('-'.repeat(80));

  const creditCheck = await rpcClient.call<CustomerCreditCheckRequest, CustomerCreditCheckResponse>(
    '',
    'queue.customer.rpc',
    { customerId: 'CUST-101', requiredAmount: 2500 }
  );
  console.log(`[Checkout RPC] Customer ${creditCheck.customerId} Credit Status: Approved=${creditCheck.hasSufficientCredit} (Tier: ${creditCheck.tier})`);

  const order1 = await orderService.createOrder({
    customerId: 'CUST-101',
    customerTier: 'vip',
    shippingAddress: '742 Evergreen Terrace, Springfield',
    items: [
      { productId: 'PROD-IPHONE', name: 'iPhone 16 Pro Max', quantity: 1, price: 1200 },
      { productId: 'PROD-AIRPODS', name: 'AirPods Pro 2', quantity: 1, price: 250 },
    ],
  });

  await new Promise((r) => setTimeout(r, 150));

  const order1Status = orderService.getOrder(order1.orderId);
  console.log(`[Verification] Order ${order1.orderId} Final State: ${order1Status?.state}`);
  console.log(`[Verification] iPhone Stock Remaining: ${productService.getStock('PROD-IPHONE')?.availableStock}`);

  console.log('\n' + '-'.repeat(80));
  console.log('TRANSACTION 2: SAGA ROLLBACK / COMPENSATION (Payment Declined)');
  console.log('-'.repeat(80));

  paymentService.setSimulateFailure(true);

  const initialMacbookStock = productService.getStock('PROD-MACBOOK')?.availableStock || 0;
  console.log(`[Initial Stock] MacBook Pro Available: ${initialMacbookStock}`);

  const order2 = await orderService.createOrder({
    customerId: 'CUST-102',
    customerTier: 'standard',
    shippingAddress: '221B Baker Street, London',
    items: [
      { productId: 'PROD-MACBOOK', name: 'MacBook Pro M4 Max', quantity: 2, price: 3500 },
    ],
  });

  await new Promise((r) => setTimeout(r, 150));

  const order2Status = orderService.getOrder(order2.orderId);
  console.log(`[Verification] Order ${order2.orderId} Final State: ${order2Status?.state} (Expected: CANCELLED)`);
  console.log(`[Verification] MacBook Stock After Rollback: ${productService.getStock('PROD-MACBOOK')?.availableStock} (Restored!)`);

  console.log('\n' + '-'.repeat(80));
  console.log('TRANSACTION 3: FATAL POISON PILL TO DEAD LETTER QUEUE (DLQ)');
  console.log('-'.repeat(80));

  paymentService.setSimulateFailure(false);
  paymentService.setSimulateFatalCrash(true);

  const order3 = await orderService.createOrder({
    customerId: 'CUST-101',
    customerTier: 'standard',
    shippingAddress: '404 Error Blvd',
    items: [{ productId: 'PROD-AIRPODS', name: 'AirPods Pro 2', quantity: 1, price: 250 }],
  });

  await new Promise((r) => setTimeout(r, 150));

  const dlqRecords = dlqManager.getDeadLetterMessages();
  console.log(`[Verification] Dead Letter Queue Message Count: ${dlqRecords.length}`);
  if (dlqRecords.length > 0) {
    console.log(`[Verification] DLQ Message Captured for Order: ${dlqRecords[0]?.payload.orderId} (Reason: ${dlqRecords[0]?.reason})`);
  }

  const notifCounts = notificationService.getDeliveredCounts();
  console.log('\n' + '-'.repeat(80));
  console.log('BROADCAST NOTIFICATION SUMMARY (FANOUT MULTIPLIER)');
  console.log('-'.repeat(80));
  console.log(`Emails Dispatched:      ${notifCounts.emails}`);
  console.log(`SMS Alerts Dispatched:  ${notifCounts.sms}`);
  console.log(`Slack Alerts Posted:    ${notifCounts.slack}`);

  console.log('\n' + '='.repeat(80));
  console.log('   ALL E-COMMERCE RABBITMQ SCENARIOS COMPLETED SUCCESSFULLY!');
  console.log('='.repeat(80) + '\n');
}

runMasterSimulation().catch((err) => {
  console.error('Master simulation failed:', err);
  process.exit(1);
});
