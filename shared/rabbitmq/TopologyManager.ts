import { ChannelWrapper } from './RabbitMQClient.js';
import { EXCHANGES, QUEUES, ROUTING_KEYS, ExchangeType } from './ExchangeTypes.js';

export class TopologyManager {

  static async setupEnterpriseTopology(channel: ChannelWrapper): Promise<void> {
    console.log('[TopologyManager] Establishing Declarative RabbitMQ Topology...');

    await channel.assertExchange(EXCHANGES.DEAD_LETTER, ExchangeType.DIRECT, { durable: true });
    await channel.assertQueue(QUEUES.DEAD_LETTER, { durable: true });
    await channel.bindQueue(QUEUES.DEAD_LETTER, EXCHANGES.DEAD_LETTER, ROUTING_KEYS.DLQ_ROUTING);

    await channel.assertExchange(EXCHANGES.RETRY, ExchangeType.DIRECT, { durable: true });

    await channel.assertQueue(QUEUES.RETRY_DELAY_3S, {
      durable: true,
      arguments: {
        'x-message-ttl': 3000, // 3-second delay
        'x-dead-letter-exchange': EXCHANGES.ORDER_EVENTS,
        'x-dead-letter-routing-key': ROUTING_KEYS.ORDER_CREATED,
      },
    });
    await channel.bindQueue(QUEUES.RETRY_DELAY_3S, EXCHANGES.RETRY, 'retry.3s');

    await channel.assertExchange(EXCHANGES.ORDER_EVENTS, ExchangeType.TOPIC, { durable: true });
    await channel.assertExchange(EXCHANGES.PAYMENT_EVENTS, ExchangeType.TOPIC, { durable: true });
    await channel.assertExchange(EXCHANGES.INVENTORY_EVENTS, ExchangeType.TOPIC, { durable: true });
    await channel.assertExchange(EXCHANGES.NOTIFICATIONS_FANOUT, ExchangeType.FANOUT, { durable: true });
    await channel.assertExchange(EXCHANGES.AUDIT_HEADERS, ExchangeType.HEADERS, { durable: true });
    await channel.assertExchange(EXCHANGES.RPC_DIRECT, ExchangeType.DIRECT, { durable: true });

    await channel.assertQueue(QUEUES.ORDERS_PROCESS, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': EXCHANGES.DEAD_LETTER,
        'x-dead-letter-routing-key': ROUTING_KEYS.DLQ_ROUTING,
      },
    });
    await channel.bindQueue(QUEUES.ORDERS_PROCESS, EXCHANGES.ORDER_EVENTS, ROUTING_KEYS.ORDER_ALL);

    await channel.assertQueue(QUEUES.ORDERS_VIP, {
      durable: true,
      arguments: {
        'x-max-priority': 10,
        'x-dead-letter-exchange': EXCHANGES.DEAD_LETTER,
        'x-dead-letter-routing-key': ROUTING_KEYS.DLQ_ROUTING,
      },
    });
    await channel.bindQueue(QUEUES.ORDERS_VIP, EXCHANGES.ORDER_EVENTS, 'order.vip.*');

    await channel.assertQueue(QUEUES.PAYMENT_PROCESS, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': EXCHANGES.DEAD_LETTER,
        'x-dead-letter-routing-key': ROUTING_KEYS.DLQ_ROUTING,
      },
    });
    await channel.bindQueue(QUEUES.PAYMENT_PROCESS, EXCHANGES.ORDER_EVENTS, ROUTING_KEYS.ORDER_CREATED);

    await channel.assertQueue(QUEUES.INVENTORY_RESERVE, { durable: true });
    await channel.bindQueue(QUEUES.INVENTORY_RESERVE, EXCHANGES.ORDER_EVENTS, ROUTING_KEYS.ORDER_CREATED);

    await channel.assertQueue(QUEUES.INVENTORY_RELEASE, { durable: true });
    await channel.bindQueue(QUEUES.INVENTORY_RELEASE, EXCHANGES.PAYMENT_EVENTS, ROUTING_KEYS.PAYMENT_FAILED);

    await channel.assertQueue(QUEUES.NOTIFICATIONS_EMAIL, { durable: true });
    await channel.bindQueue(QUEUES.NOTIFICATIONS_EMAIL, EXCHANGES.NOTIFICATIONS_FANOUT, '');

    await channel.assertQueue(QUEUES.NOTIFICATIONS_SMS, { durable: true });
    await channel.bindQueue(QUEUES.NOTIFICATIONS_SMS, EXCHANGES.NOTIFICATIONS_FANOUT, '');

    await channel.assertQueue(QUEUES.NOTIFICATIONS_SLACK, { durable: true });
    await channel.bindQueue(QUEUES.NOTIFICATIONS_SLACK, EXCHANGES.NOTIFICATIONS_FANOUT, '');

    await channel.assertQueue(QUEUES.AUDIT_COMPLIANCE, { durable: true });
    await channel.bindQueue(QUEUES.AUDIT_COMPLIANCE, EXCHANGES.AUDIT_HEADERS, '', {
      'x-match': 'all',
      'audit-required': 'true',
      tier: 'financial',
    });

    console.log('[TopologyManager] Topology established successfully.');
  }
}
