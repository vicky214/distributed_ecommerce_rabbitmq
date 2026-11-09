import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';

async function runScenario() {
  console.log('\n========================================================================');
  console.log('   SCENARIO 01: WORKER QUEUES & FAIR DISPATCH (CHANNEL PREFETCH)');
  console.log('========================================================================\n');

  const client = new RabbitMQClient();
  await client.connect(false);
  const channel = await client.createChannel();

  const QUEUE_TASKS = 'queue.heavy.tasks';
  await channel.assertQueue(QUEUE_TASKS, { durable: true });

  const worker1Processed: string[] = [];
  const worker2Processed: string[] = [];

  await channel.prefetch(1);
  await channel.consume(QUEUE_TASKS, async (msg) => {
    if (!msg) return;
    const task = JSON.parse(msg.content.toString());
    console.log(`[Worker 1 - FAST] Starting ${task.name}...`);
    await new Promise((r) => setTimeout(r, 10));
    worker1Processed.push(task.name);
    console.log(`[Worker 1 - FAST] Completed ${task.name} -> ACK`);
    channel.ack(msg);
  });

  await channel.consume(QUEUE_TASKS, async (msg) => {
    if (!msg) return;
    const task = JSON.parse(msg.content.toString());
    console.log(`[Worker 2 - SLOW] Starting ${task.name}...`);
    await new Promise((r) => setTimeout(r, 50));
    worker2Processed.push(task.name);
    console.log(`[Worker 2 - SLOW] Completed ${task.name} -> ACK`);
    channel.ack(msg);
  });

  console.log('[Producer] Dispatching 6 tasks to work queue...');
  for (let i = 1; i <= 6; i++) {
    const task = { id: i, name: `ImageProcessingTask-#${i}` };
    await channel.sendToQueue(QUEUE_TASKS, Buffer.from(JSON.stringify(task)), {
      persistent: true, // Message durability: saved to disk
    });
  }

  await new Promise((r) => setTimeout(r, 250));

  console.log('\n--- RESULTS OF FAIR DISPATCH ---');
  console.log(`Worker 1 (FAST) processed: ${worker1Processed.length} tasks (${worker1Processed.join(', ')})`);
  console.log(`Worker 2 (SLOW) processed: ${worker2Processed.length} tasks (${worker2Processed.join(', ')})`);
  console.log('Notice: Fast worker pulled more tasks dynamically because prefetch(1) prevented starvation!\n');
}

runScenario();
