import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';
import { ExchangeType } from '../shared/rabbitmq/ExchangeTypes.js';

async function runScenario() {
  console.log('\n========================================================================');
  console.log('   SCENARIO 05: HEADERS EXCHANGE (METADATA MATCHING WITH x-match)');
  console.log('========================================================================\n');

  const client = new RabbitMQClient();
  await client.connect(false);
  const channel = await client.createChannel();

  const HEADERS_EXCHANGE = 'exchange.compliance.headers';
  const QUEUE_FINANCIAL_REPORTS = 'queue.headers.financial_pdf';
  const QUEUE_ANY_REPORTS = 'queue.headers.any_report';

  await channel.assertExchange(HEADERS_EXCHANGE, ExchangeType.HEADERS, { durable: true });

  await channel.assertQueue(QUEUE_FINANCIAL_REPORTS, { durable: true });
  await channel.bindQueue(QUEUE_FINANCIAL_REPORTS, HEADERS_EXCHANGE, '', {
    'x-match': 'all',
    format: 'pdf',
    department: 'finance',
  });

  await channel.assertQueue(QUEUE_ANY_REPORTS, { durable: true });
  await channel.bindQueue(QUEUE_ANY_REPORTS, HEADERS_EXCHANGE, '', {
    'x-match': 'any',
    format: 'pdf',
    department: 'marketing',
  });

  const financialReceived: string[] = [];
  const anyReceived: string[] = [];

  await channel.consume(QUEUE_FINANCIAL_REPORTS, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());
    financialReceived.push(body.title);
    console.log(`[Queue 1: x-match ALL] Received: "${body.title}" (Headers matched ALL criteria)`);
    channel.ack(msg);
  });

  await channel.consume(QUEUE_ANY_REPORTS, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());
    anyReceived.push(body.title);
    console.log(`[Queue 2: x-match ANY] Received: "${body.title}" (Headers matched AT LEAST ONE criteria)`);
    channel.ack(msg);
  });

  console.log('--- Publishing 3 messages with different header combinations ---');

  console.log('\n[Producer] Publishing Doc 1: headers { format: "pdf", department: "finance" }');
  await channel.publish(
    HEADERS_EXCHANGE,
    '',
    Buffer.from(JSON.stringify({ title: 'Q3_Financial_Audit.pdf' })),
    { headers: { format: 'pdf', department: 'finance' } }
  );

  console.log('[Producer] Publishing Doc 2: headers { format: "csv", department: "finance" }');
  await channel.publish(
    HEADERS_EXCHANGE,
    '',
    Buffer.from(JSON.stringify({ title: 'Raw_Ledger_Data.csv' })),
    { headers: { format: 'csv', department: 'finance' } }
  );

  console.log('[Producer] Publishing Doc 3: headers { format: "png", department: "marketing" }');
  await channel.publish(
    HEADERS_EXCHANGE,
    '',
    Buffer.from(JSON.stringify({ title: 'Marketing_Banner.png' })),
    { headers: { format: 'png', department: 'marketing' } }
  );

  await new Promise((r) => setTimeout(r, 100));

  console.log('\n--- VERIFICATION OF HEADERS MATCHING ---');
  console.log(`Queue 1 (x-match ALL: pdf & finance) got:   ${financialReceived.join(', ')}`);
  console.log(`Queue 2 (x-match ANY: pdf OR marketing) got: ${anyReceived.join(', ')}`);
  console.log('Notice: Doc 2 (csv & finance) was dropped because neither condition met!\n');
}

runScenario();
