import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';
import { ExchangeType } from '../shared/rabbitmq/ExchangeTypes.js';

async function runScenario() {
  console.log('\n========================================================================');
  console.log('   SCENARIO 04: TOPIC EXCHANGE ROUTING (WILDCARDS "*" AND "#")');
  console.log('========================================================================\n');

  const client = new RabbitMQClient();
  await client.connect(false);
  const channel = await client.createChannel();

  const TOPIC_EXCHANGE = 'exchange.logs.topic';
  const QUEUE_EU_ORDERS = 'queue.topic.eu';
  const QUEUE_CRITICAL = 'queue.topic.critical';
  const QUEUE_ALL_ORDERS = 'queue.topic.all_orders';

  await channel.assertExchange(TOPIC_EXCHANGE, ExchangeType.TOPIC, { durable: true });

  await channel.assertQueue(QUEUE_EU_ORDERS, { durable: true });
  await channel.bindQueue(QUEUE_EU_ORDERS, TOPIC_EXCHANGE, 'order.eu.*');

  await channel.assertQueue(QUEUE_CRITICAL, { durable: true });
  await channel.bindQueue(QUEUE_CRITICAL, TOPIC_EXCHANGE, '#.critical');

  await channel.assertQueue(QUEUE_ALL_ORDERS, { durable: true });
  await channel.bindQueue(QUEUE_ALL_ORDERS, TOPIC_EXCHANGE, 'order.#');

  const euReceived: string[] = [];
  const criticalReceived: string[] = [];
  const allOrdersReceived: string[] = [];

  await channel.consume(QUEUE_EU_ORDERS, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());
    euReceived.push(body.id);
    console.log(`[EU Queue (order.eu.*)] Matched: "${msg.fields.routingKey}" (Payload: ${body.id})`);
    channel.ack(msg);
  });

  await channel.consume(QUEUE_CRITICAL, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());
    criticalReceived.push(body.id);
    console.log(`[Critical Queue (#.critical)] Matched: "${msg.fields.routingKey}" (Payload: ${body.id})`);
    channel.ack(msg);
  });

  await channel.consume(QUEUE_ALL_ORDERS, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());
    allOrdersReceived.push(body.id);
    console.log(`[All Orders Queue (order.#)] Matched: "${msg.fields.routingKey}" (Payload: ${body.id})`);
    channel.ack(msg);
  });

  console.log('--- Publishing 3 test messages with diverse routing keys ---');

  console.log('\nPublishing [1] with routing key: "order.eu.info"');
  await channel.publish(TOPIC_EXCHANGE, 'order.eu.info', Buffer.from(JSON.stringify({ id: 'MSG-1' })));

  console.log('Publishing [2] with routing key: "order.us.california.critical"');
  await channel.publish(TOPIC_EXCHANGE, 'order.us.california.critical', Buffer.from(JSON.stringify({ id: 'MSG-2' })));

  console.log('Publishing [3] with routing key: "payment.gateway.critical"');
  await channel.publish(TOPIC_EXCHANGE, 'payment.gateway.critical', Buffer.from(JSON.stringify({ id: 'MSG-3' })));

  await new Promise((r) => setTimeout(r, 100));

  console.log('\n--- VERIFICATION OF TOPIC WILDCARD MATCHING ---');
  console.log(`EU Queue (order.eu.*) got:             ${euReceived.join(', ')} (Expected: MSG-1)`);
  console.log(`Critical Queue (#.critical) got:       ${criticalReceived.join(', ')} (Expected: MSG-2, MSG-3)`);
  console.log(`All Orders Queue (order.#) got:        ${allOrdersReceived.join(', ')} (Expected: MSG-1, MSG-2)`);
  console.log('Notice: Star matched 1 word; Hash matched multiple words!\n');
}

runScenario();
