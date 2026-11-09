import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';
import { ExchangeType } from '../shared/rabbitmq/ExchangeTypes.js';

async function runScenario() {
  console.log('\n========================================================================');
  console.log('   SCENARIO 03: DIRECT EXCHANGE ROUTING (EXACT STRING MATCHING)');
  console.log('========================================================================\n');

  const client = new RabbitMQClient();
  await client.connect(false);
  const channel = await client.createChannel();

  const DIRECT_EXCHANGE = 'exchange.payment.direct';
  const QUEUE_ACCOUNTING = 'queue.direct.accounting';
  const QUEUE_FRAUD = 'queue.direct.fraud';

  await channel.assertExchange(DIRECT_EXCHANGE, ExchangeType.DIRECT, { durable: true });

  await channel.assertQueue(QUEUE_ACCOUNTING, { durable: true });
  await channel.bindQueue(QUEUE_ACCOUNTING, DIRECT_EXCHANGE, 'payment.success');

  await channel.assertQueue(QUEUE_FRAUD, { durable: true });
  await channel.bindQueue(QUEUE_FRAUD, DIRECT_EXCHANGE, 'payment.failed');

  const accountingReceived: string[] = [];
  const fraudReceived: string[] = [];

  await channel.consume(QUEUE_ACCOUNTING, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());
    accountingReceived.push(body.paymentId);
    console.log(`[Accounting Queue] Processed SUCCESSFUL payment: ${body.paymentId} ($${body.amount})`);
    channel.ack(msg);
  });

  await channel.consume(QUEUE_FRAUD, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());
    fraudReceived.push(body.paymentId);
    console.warn(`[Fraud/Risk Queue] Alert: FAILED payment under review: ${body.paymentId} (Reason: ${body.reason})`);
    channel.ack(msg);
  });

  console.log('[Producer] Publishing Payment #1 with routing key "payment.success"...');
  await channel.publish(
    DIRECT_EXCHANGE,
    'payment.success',
    Buffer.from(JSON.stringify({ paymentId: 'PAY-1001', amount: 250 }))
  );

  console.log('[Producer] Publishing Payment #2 with routing key "payment.failed"...');
  await channel.publish(
    DIRECT_EXCHANGE,
    'payment.failed',
    Buffer.from(JSON.stringify({ paymentId: 'PAY-1002', amount: 9999, reason: 'Stolen card reported' }))
  );

  await new Promise((r) => setTimeout(r, 100));

  console.log('\n--- VERIFICATION OF DIRECT ROUTING ---');
  console.log(`Accounting Queue got: ${accountingReceived.join(', ')}`);
  console.log(`Fraud Review Queue got: ${fraudReceived.join(', ')}`);
  console.log('Notice: Direct exchange cleanly segregated messages based on exact key match!\n');
}

runScenario();
