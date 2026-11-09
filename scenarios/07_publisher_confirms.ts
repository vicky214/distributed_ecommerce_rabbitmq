import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';

async function runScenario() {
  console.log('\n========================================================================');
  console.log('   SCENARIO 07: PUBLISHER CONFIRMS (RELIABLE MESSAGE DELIVERY)');
  console.log('========================================================================\n');

  const client = new RabbitMQClient();
  await client.connect(false);
  const confirmChannel = await client.createConfirmChannel();

  const QUEUE_CRITICAL_ORDERS = 'queue.critical.orders';
  await confirmChannel.assertQueue(QUEUE_CRITICAL_ORDERS, { durable: true });

  console.log('[Producer] Publishing 5 critical financial orders with Publisher Confirms enabled...');

  const startTime = performance.now();

  for (let i = 1; i <= 5; i++) {
    const orderPayload = {
      orderId: `CRITICAL-ORD-${i}`,
      amount: i * 1500,
      timestamp: Date.now(),
    };

    await confirmChannel.sendToQueue(
      QUEUE_CRITICAL_ORDERS,
      Buffer.from(JSON.stringify(orderPayload)),
      { persistent: true }
    );
    console.log(`  -> Sent Order #${i} [Persistent]`);
  }

  if (confirmChannel.waitForConfirms) {
    await confirmChannel.waitForConfirms();
  }

  const duration = (performance.now() - startTime).toFixed(2);
  console.log(`\n✔ [Broker Confirmed] All 5 messages were safely received and persisted by RabbitMQ in ${duration} ms!`);
  console.log('Zero messages lost. Safe to commit database transaction.\n');
}

runScenario();
