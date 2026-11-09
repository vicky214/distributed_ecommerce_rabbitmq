import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';
import { ExchangeType } from '../shared/rabbitmq/ExchangeTypes.js';

async function runScenario() {
  console.log('\n========================================================================');
  console.log('   SCENARIO 02: PUBLISH/SUBSCRIBE VIA FANOUT EXCHANGE');
  console.log('========================================================================\n');

  const client = new RabbitMQClient();
  await client.connect(false);
  const channel = await client.createChannel();

  const FANOUT_EXCHANGE = 'exchange.broadcast.fanout';
  const QUEUE_EMAIL = 'queue.fanout.email';
  const QUEUE_SMS = 'queue.fanout.sms';
  const QUEUE_AUDIT = 'queue.fanout.audit';

  await channel.assertExchange(FANOUT_EXCHANGE, ExchangeType.FANOUT, { durable: true });

  await channel.assertQueue(QUEUE_EMAIL, { durable: true });
  await channel.bindQueue(QUEUE_EMAIL, FANOUT_EXCHANGE, '');

  await channel.assertQueue(QUEUE_SMS, { durable: true });
  await channel.bindQueue(QUEUE_SMS, FANOUT_EXCHANGE, '');

  await channel.assertQueue(QUEUE_AUDIT, { durable: true });
  await channel.bindQueue(QUEUE_AUDIT, FANOUT_EXCHANGE, '');

  const received: Record<string, string[]> = { email: [], sms: [], audit: [] };

  await channel.consume(QUEUE_EMAIL, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());
    received['email']!.push(body.event);
    console.log(`[Email Consumer] Received broadcast: "${body.event}" -> ACK`);
    channel.ack(msg);
  });

  await channel.consume(QUEUE_SMS, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());
    received['sms']!.push(body.event);
    console.log(`[SMS Consumer] Received broadcast: "${body.event}" -> ACK`);
    channel.ack(msg);
  });

  await channel.consume(QUEUE_AUDIT, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());
    received['audit']!.push(body.event);
    console.log(`[Audit Logger Consumer] Received broadcast: "${body.event}" -> ACK`);
    channel.ack(msg);
  });

  console.log('[Producer] Publishing 1 broadcast message: "FLASH_SALE_LIVE_50%_OFF"...');
  await channel.publish(
    FANOUT_EXCHANGE,
    'this.routing.key.is.completely.ignored.by.fanout', // Fanout ignores this!
    Buffer.from(JSON.stringify({ event: 'FLASH_SALE_LIVE_50%_OFF', timestamp: Date.now() }))
  );

  await new Promise((r) => setTimeout(r, 100));

  console.log('\n--- VERIFICATION OF FANOUT BROADCAST ---');
  console.log(`Email Queue received count: ${received['email']!.length}`);
  console.log(`SMS Queue received count:   ${received['sms']!.length}`);
  console.log(`Audit Queue received count: ${received['audit']!.length}`);
  console.log('Notice: 1 published message was automatically replicated by RabbitMQ to all 3 queues!\n');
}

runScenario();
