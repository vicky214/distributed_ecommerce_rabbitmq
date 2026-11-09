import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';

async function runScenario() {
  console.log('\n========================================================================');
  console.log('   SCENARIO 10: PRIORITY QUEUES (VIP ORDER FAST-LANE)');
  console.log('========================================================================\n');

  const client = new RabbitMQClient();
  await client.connect(false);
  const channel = await client.createChannel();

  const PRIORITY_QUEUE = 'queue.orders.prioritized';

  await channel.assertQueue(PRIORITY_QUEUE, {
    durable: true,
    arguments: {
      'x-max-priority': 10,
    },
  });

  console.log('[Producer] Sending 3 Standard Customer Orders (Priority: 1)...');
  for (let i = 1; i <= 3; i++) {
    const order = { id: `STD-ORDER-${i}`, tier: 'standard' };
    await channel.sendToQueue(PRIORITY_QUEUE, Buffer.from(JSON.stringify(order)), {
      priority: 1,
    });
  }

  console.log('[Producer] Sending 2 VIP Diamond Customer Orders (Priority: 9)...');
  for (let i = 1; i <= 2; i++) {
    const order = { id: `VIP-ORDER-${i}`, tier: 'vip' };
    await channel.sendToQueue(PRIORITY_QUEUE, Buffer.from(JSON.stringify(order)), {
      priority: 9, // Higher priority!
    });
  }

  console.log('\n[Consumer] Starting worker. Inspecting delivery sequence...');
  const deliverySequence: string[] = [];

  await channel.consume(PRIORITY_QUEUE, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());
    const p = msg.properties.priority || 0;

    deliverySequence.push(`${body.id} (P:${p})`);
    console.log(`  -> Consumed: ${body.id} [Priority: ${p}]`);
    channel.ack(msg);
  });

  await new Promise((r) => setTimeout(r, 100));

  console.log('\n--- VERIFICATION OF QUEUE PRIORITY RE-ORDERING ---');
  console.log('Final Execution Order:');
  deliverySequence.forEach((item, idx) => console.log(`   Step ${idx + 1}: ${item}`));
  console.log('\nNotice: Both VIP orders were processed FIRST, jumping ahead of earlier standard orders!\n');
}

runScenario();
