export enum ExchangeType {
  DIRECT = 'direct',
  TOPIC = 'topic',
  FANOUT = 'fanout',
  HEADERS = 'headers',
}

export const EXCHANGES = {

  ORDER_EVENTS: 'ecommerce.order.events',

  PAYMENT_EVENTS: 'ecommerce.payment.events',

  INVENTORY_EVENTS: 'ecommerce.inventory.events',

  NOTIFICATIONS_FANOUT: 'ecommerce.notifications.fanout',

  AUDIT_HEADERS: 'ecommerce.audit.headers',

  DEAD_LETTER: 'ecommerce.dlx',

  RETRY: 'ecommerce.retry',

  RPC_DIRECT: 'ecommerce.rpc.direct',
} as const;

export const QUEUES = {

  ORDERS_PROCESS: 'queue.orders.process',
  ORDERS_VIP: 'queue.orders.vip', // Priority queue (x-max-priority)

  PAYMENT_PROCESS: 'queue.payment.process',
  PAYMENT_REFUND: 'queue.payment.refund',

  INVENTORY_RESERVE: 'queue.inventory.reserve',
  INVENTORY_RELEASE: 'queue.inventory.release',

  NOTIFICATIONS_EMAIL: 'queue.notifications.email',
  NOTIFICATIONS_SMS: 'queue.notifications.sms',
  NOTIFICATIONS_SLACK: 'queue.notifications.slack',

  DEAD_LETTER: 'queue.ecommerce.dlq',
  RETRY_DELAY_3S: 'queue.ecommerce.retry.3s',
  RETRY_DELAY_10S: 'queue.ecommerce.retry.10s',

  AUDIT_COMPLIANCE: 'queue.audit.compliance',
} as const;

export const ROUTING_KEYS = {

  ORDER_CREATED: 'order.created',
  ORDER_CANCELLED: 'order.cancelled',
  ORDER_COMPLETED: 'order.completed',
  ORDER_ALL: 'order.#', // Matches all order events

  PAYMENT_SUCCESS: 'payment.success',
  PAYMENT_FAILED: 'payment.failed',
  PAYMENT_ALL: 'payment.#',

  INVENTORY_RESERVED: 'inventory.reserved',
  INVENTORY_FAILED: 'inventory.failed',
  INVENTORY_ALL: 'inventory.#',

  DLQ_ROUTING: 'dlq.message',
  RETRY_ROUTING: 'retry.message',
} as const;
