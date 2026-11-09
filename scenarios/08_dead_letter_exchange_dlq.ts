import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';
import { ExchangeType } from '../shared/rabbitmq/ExchangeTypes.js';

async function runScenario() {
  console.log('\n========================================================================');
  console.log('   SCENARIO 08: DEAD LETTER EXCHANGE (DLX) & DLQ FOR POISON MESSAGES');
  console.log('========================================================================\n');

  const client = new RabbitMQClient();
  await client.connect(false);
  const channel = await client.createChannel();

  const MAIN_QUEUE = 'queue.orders.incoming';
  const DLX_EXCHANGE = 'dlx.orders.exchange';
  const DLQ_QUEUE = 'queue.orders.dlq';

  await channel.assertExchange(DLX_EXCHANGE, ExchangeType.DIRECT, { durable: true });
  await channel.assertQueue(DLQ_QUEUE, { durable: true });
  await channel.bindQueue(DLQ_QUEUE, DLX_EXCHANGE, 'dead.letter.orders');

  await channel.assertQueue(MAIN_QUEUE, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': DLX_EXCHANGE,
      'x-dead-letter-routing-key': 'dead.letter.orders',
    },
  });

  const dlqCaptured: any[] = [];

  await channel.consume(DLQ_QUEUE, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());
    const xDeath = msg.properties.headers?.['x-death']?.[0];

    dlqCaptured.push({ body, xDeath });

    console.warn(`\n🚨 [DLQ MONITOR ALERT] Poison Message Captured in DLQ!`);
    console.warn(`   Order ID:        ${body.orderId}`);
    console.warn(`   Reason:          ${xDeath?.reason || 'rejected'}`);
    console.warn(`   Original Exch:   ${xDeath?.exchange || 'main'}`);
    console.warn(`   Death Count:     ${xDeath?.count || 1}`);
    console.warn(`   Dead-Lettered At:${xDeath?.time || new Date().toISOString()}`);

    channel.ack(msg);
  });

  await channel.consume(MAIN_QUEUE, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());

    if (body.isCorrupted) {
      console.error(`[Worker] Detected Corrupted/Poison Message: ${body.orderId}! Rejecting with requeue=false...`);

      channel.nack(msg, false, false);
    } else {
      console.log(`[Worker] Successfully processed valid Order: ${body.orderId} -> ACK`);
      channel.ack(msg);
    }
  });

  console.log('[Producer] Publishing Valid Order ORD-101...');
  await channel.sendToQueue(MAIN_QUEUE, Buffer.from(JSON.stringify({ orderId: 'ORD-101', isCorrupted: false })));

  console.log('[Producer] Publishing Corrupted Poison Order ORD-999 (Will fail)...');
  await channel.sendToQueue(MAIN_QUEUE, Buffer.from(JSON.stringify({ orderId: 'ORD-999', isCorrupted: true })));

  await new Promise((r) => setTimeout(r, 100));

  console.log('\n--- VERIFICATION OF DLQ CAPTURE ---');
  console.log(`Total messages in DLQ: ${dlqCaptured.length} (Captured order: ${dlqCaptured[0]?.body.orderId})`);
  console.log('Notice: Poison message was quarantined into DLQ without crashing the worker or blocking the queue!\n');
}

runScenario();
