import { test, describe, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';
import { ExchangeType } from '../shared/rabbitmq/ExchangeTypes.js';

describe('Dead Letter Exchanges, TTL & Priority Queues', () => {
  let client: RabbitMQClient;
  let channel: any;

  beforeEach(async () => {
    RabbitMQClient.getVirtualBroker().reset();
    client = new RabbitMQClient();
    await client.connect(false);
    channel = await client.createChannel();
  });

  test('DLX: Nack with requeue=false automatically dead-letters message to DLQ', async () => {
    const DLX = 'dlx.test';
    const DLQ = 'q.dlq';
    const MAIN_Q = 'q.main';

    await channel.assertExchange(DLX, ExchangeType.DIRECT);
    await channel.assertQueue(DLQ);
    await channel.bindQueue(DLQ, DLX, 'dlq.route');

    await channel.assertQueue(MAIN_Q, {
      arguments: {
        'x-dead-letter-exchange': DLX,
        'x-dead-letter-routing-key': 'dlq.route',
      },
    });

    const dlqItems: string[] = [];
    await channel.consume(DLQ, (m: any) => {
      dlqItems.push(m.content.toString());
      channel.ack(m);
    });

    await channel.consume(MAIN_Q, (m: any) => {

      channel.nack(m, false, false);
    });

    await channel.sendToQueue(MAIN_Q, Buffer.from('poison-pill'));

    await new Promise((r) => setTimeout(r, 50));
    assert.deepEqual(dlqItems, ['poison-pill']);
  });

  test('Message TTL: Expired messages automatically dead-letter to DLX', async () => {
    const DLX = 'dlx.ttl';
    const DLQ = 'q.ttl.dlq';
    const TTL_Q = 'q.ttl.delayed';

    await channel.assertExchange(DLX, ExchangeType.DIRECT);
    await channel.assertQueue(DLQ);
    await channel.bindQueue(DLQ, DLX, 'ttl.expired');

    await channel.assertQueue(TTL_Q, {
      arguments: {
        'x-message-ttl': 50,
        'x-dead-letter-exchange': DLX,
        'x-dead-letter-routing-key': 'ttl.expired',
      },
    });

    const dlqItems: string[] = [];
    await channel.consume(DLQ, (m: any) => {
      dlqItems.push(m.content.toString());
      channel.ack(m);
    });

    await channel.sendToQueue(TTL_Q, Buffer.from('time-bomb'));

    assert.equal(dlqItems.length, 0);

    await new Promise((r) => setTimeout(r, 100));
    assert.equal(dlqItems.length, 1);
    assert.equal(dlqItems[0], 'time-bomb');
  });

  test('Priority Queue: Higher priority messages jump ahead in queue', async () => {
    const PRIO_Q = 'q.prio';
    await channel.assertQueue(PRIO_Q, {
      arguments: { 'x-max-priority': 10 },
    });

    await channel.sendToQueue(PRIO_Q, Buffer.from('low-1'), { priority: 1 });
    await channel.sendToQueue(PRIO_Q, Buffer.from('low-2'), { priority: 1 });

    await channel.sendToQueue(PRIO_Q, Buffer.from('vip-urgent'), { priority: 9 });

    const order: string[] = [];
    await channel.consume(PRIO_Q, (m: any) => {
      order.push(m.content.toString());
      channel.ack(m);
    });

    await new Promise((r) => setTimeout(r, 50));

    assert.deepEqual(order, ['vip-urgent', 'low-1', 'low-2']);
  });
});
