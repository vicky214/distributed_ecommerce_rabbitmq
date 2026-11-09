import { test, describe, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';
import { ExchangeType } from '../shared/rabbitmq/ExchangeTypes.js';

describe('RabbitMQ Exchanges & Routing Mechanics', () => {
  let client: RabbitMQClient;
  let channel: any;

  beforeEach(async () => {
    RabbitMQClient.getVirtualBroker().reset();
    client = new RabbitMQClient();
    await client.connect(false);
    channel = await client.createChannel();
  });

  test('Direct Exchange: Exact routing key match only', async () => {
    await channel.assertExchange('ex.direct', ExchangeType.DIRECT);
    await channel.assertQueue('q.exact');
    await channel.bindQueue('q.exact', 'ex.direct', 'order.created');

    const received: string[] = [];
    await channel.consume('q.exact', (msg: any) => {
      received.push(msg.fields.routingKey);
      channel.ack(msg);
    });

    await channel.publish('ex.direct', 'order.created', Buffer.from('match'));

    await channel.publish('ex.direct', 'order.updated', Buffer.from('no-match'));

    await new Promise((r) => setTimeout(r, 50));
    assert.deepEqual(received, ['order.created']);
  });

  test('Fanout Exchange: Broadcasts to all queues, ignoring routing key', async () => {
    await channel.assertExchange('ex.fanout', ExchangeType.FANOUT);
    await channel.assertQueue('q.fanout.1');
    await channel.assertQueue('q.fanout.2');
    await channel.bindQueue('q.fanout.1', 'ex.fanout', '');
    await channel.bindQueue('q.fanout.2', 'ex.fanout', '');

    let count1 = 0;
    let count2 = 0;
    await channel.consume('q.fanout.1', (m: any) => { count1++; channel.ack(m); });
    await channel.consume('q.fanout.2', (m: any) => { count2++; channel.ack(m); });

    await channel.publish('ex.fanout', 'any.random.routing.key', Buffer.from('broadcast'));

    await new Promise((r) => setTimeout(r, 50));
    assert.equal(count1, 1);
    assert.equal(count2, 1);
  });

  test('Topic Exchange: Wildcard matching with * and #', async () => {
    await channel.assertExchange('ex.topic', ExchangeType.TOPIC);
    await channel.assertQueue('q.star');
    await channel.assertQueue('q.hash');

    await channel.bindQueue('q.star', 'ex.topic', 'order.*.created');

    await channel.bindQueue('q.hash', 'ex.topic', 'order.#');

    const starMsgs: string[] = [];
    const hashMsgs: string[] = [];
    await channel.consume('q.star', (m: any) => { starMsgs.push(m.fields.routingKey); channel.ack(m); });
    await channel.consume('q.hash', (m: any) => { hashMsgs.push(m.fields.routingKey); channel.ack(m); });

    await channel.publish('ex.topic', 'order.eu.created', Buffer.from('1'));

    await channel.publish('ex.topic', 'order.us.california.urgent', Buffer.from('2'));

    await new Promise((r) => setTimeout(r, 50));
    assert.deepEqual(starMsgs, ['order.eu.created']);
    assert.deepEqual(hashMsgs, ['order.eu.created', 'order.us.california.urgent']);
  });

  test('Headers Exchange: x-match all vs any', async () => {
    await channel.assertExchange('ex.headers', ExchangeType.HEADERS);
    await channel.assertQueue('q.all');
    await channel.bindQueue('q.all', 'ex.headers', '', {
      'x-match': 'all',
      tier: 'gold',
      region: 'us',
    });

    const received: any[] = [];
    await channel.consume('q.all', (m: any) => { received.push(m); channel.ack(m); });

    await channel.publish('ex.headers', '', Buffer.from('no'), { headers: { tier: 'gold' } });

    await channel.publish('ex.headers', '', Buffer.from('yes'), { headers: { tier: 'gold', region: 'us' } });

    await new Promise((r) => setTimeout(r, 50));
    assert.equal(received.length, 1);
    assert.equal(received[0].content.toString(), 'yes');
  });
});
