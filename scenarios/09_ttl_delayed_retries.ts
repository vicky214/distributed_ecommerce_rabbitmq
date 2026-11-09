import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';
import { ExchangeType } from '../shared/rabbitmq/ExchangeTypes.js';

async function runScenario() {
  console.log('\n========================================================================');
  console.log('   SCENARIO 09: DELAYED RETRIES VIA MESSAGE TTL & DEAD-LETTER LOOP');
  console.log('========================================================================\n');

  const client = new RabbitMQClient();
  await client.connect(false);
  const channel = await client.createChannel();

  const MAIN_EXCHANGE = 'exchange.orders.main';
  const MAIN_QUEUE = 'queue.orders.active';
  const RETRY_EXCHANGE = 'exchange.retry.delayed';
  const RETRY_QUEUE_500MS = 'queue.retry.delay.500ms';
  const TERMINAL_DLQ = 'queue.terminal.dlq';

  await channel.assertExchange(MAIN_EXCHANGE, ExchangeType.DIRECT, { durable: true });
  await channel.assertQueue(MAIN_QUEUE, { durable: true });
  await channel.bindQueue(MAIN_QUEUE, MAIN_EXCHANGE, 'order.process');

  await channel.assertExchange(RETRY_EXCHANGE, ExchangeType.DIRECT, { durable: true });
  await channel.assertQueue(RETRY_QUEUE_500MS, {
    durable: true,
    arguments: {
      'x-message-ttl': 500, // 500ms delay buffer
      'x-dead-letter-exchange': MAIN_EXCHANGE,
      'x-dead-letter-routing-key': 'order.process', // Dead letters back to main worker!
    },
  });
  await channel.bindQueue(RETRY_QUEUE_500MS, RETRY_EXCHANGE, 'retry.500ms');

  await channel.assertQueue(TERMINAL_DLQ, { durable: true });

  let attempts = 0;
  const maxRetries = 2;

  await channel.consume(MAIN_QUEUE, async (msg) => {
    if (!msg) return;
    attempts++;
    const body = JSON.parse(msg.content.toString());
    const retryCount = (msg.properties.headers?.['x-retry-count'] as number) || 0;

    console.log(`[Worker] Attempt #${attempts} (Header Retry Count: ${retryCount}) processing: ${body.orderId}...`);

    if (retryCount < maxRetries) {
      console.warn(`[Worker] Temporary 3rd-party failure! Routing to 500ms delay queue...`);

      await channel.publish(
        RETRY_EXCHANGE,
        'retry.500ms',
        msg.content,
        {
          ...msg.properties,
          headers: {
            ...(msg.properties.headers || {}),
            'x-retry-count': retryCount + 1,
          },
        }
      );
      channel.ack(msg);
    } else {
      console.error(`[Worker] Exhausted max retries (${maxRetries})! Moving to Terminal DLQ.`);
      await channel.sendToQueue(TERMINAL_DLQ, msg.content, {
        headers: { 'x-exhausted': true, 'x-total-attempts': attempts },
      });
      channel.ack(msg);
    }
  });

  console.log('[Producer] Publishing initial order ORD-RETRY-777 to main queue...');
  await channel.publish(MAIN_EXCHANGE, 'order.process', Buffer.from(JSON.stringify({ orderId: 'ORD-RETRY-777' })));

  await new Promise((r) => setTimeout(r, 1500));

  console.log('\n--- VERIFICATION OF DELAYED RETRY LOOP ---');
  console.log(`Total attempts executed: ${attempts} (1 initial + 2 retries = 3 attempts)`);
  console.log('Notice: Delayed queue acted as a non-blocking backoff buffer without sleeping threads!\n');
}

runScenario();
