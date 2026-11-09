import { EventEmitter } from 'node:events';

export interface MessageProperties {
  contentType?: string;
  contentEncoding?: string;
  headers?: Record<string, any>;
  deliveryMode?: number;
  priority?: number;
  correlationId?: string;
  replyTo?: string;
  expiration?: string | number;
  messageId?: string;
  timestamp?: number;
  type?: string;
  userId?: string;
  appId?: string;
}

export interface MessageFields {
  deliveryTag: number;
  redelivered: boolean;
  exchange: string;
  routingKey: string;
  consumerTag?: string;
}

export interface AmqpMessage {
  content: Buffer;
  fields: MessageFields;
  properties: MessageProperties;
}

export interface QueueOptions {
  durable?: boolean;
  exclusive?: boolean;
  autoDelete?: boolean;
  arguments?: {
    'x-dead-letter-exchange'?: string;
    'x-dead-letter-routing-key'?: string;
    'x-message-ttl'?: number;
    'x-max-priority'?: number;
    [key: string]: any;
  };
}

export interface ExchangeOptions {
  durable?: boolean;
  autoDelete?: boolean;
  alternateExchange?: string;
  arguments?: Record<string, any>;
}

interface Binding {
  queueName: string;
  pattern: string;
  headers?: Record<string, any>;
}

interface ConsumerRegistration {
  consumerTag: string;
  callback: (msg: AmqpMessage | null) => void;
  noAck: boolean;
  unackedCount: number;
}

export class InMemoryAmqpBroker extends EventEmitter {
  private exchanges = new Map<string, { type: string; options: ExchangeOptions }>();
  private queues = new Map<string, { options: QueueOptions; messages: AmqpMessage[] }>();
  private bindings: Array<{ exchange: string; binding: Binding }> = [];
  private consumers = new Map<string, ConsumerRegistration[]>();
  private deliveryTagCounter = 0;
  private inFlightMessages = new Map<number, { queueName: string; msg: AmqpMessage }>();
  private prefetchLimits = new Map<string, number>();
  private timers: NodeJS.Timeout[] = [];

  constructor() {
    super();

    this.exchanges.set('', { type: 'direct', options: { durable: true } });
  }

  assertExchange(exchange: string, type: string, options: ExchangeOptions = {}): void {
    if (!this.exchanges.has(exchange)) {
      this.exchanges.set(exchange, { type, options });
    }
  }

  assertQueue(queueName: string, options: QueueOptions = {}): { queue: string; messageCount: number; consumerCount: number } {
    const resolvedName =
      queueName && queueName.trim().length > 0
        ? queueName
        : `amq.gen-${Math.random().toString(36).substring(2, 12)}`;

    if (!this.queues.has(resolvedName)) {
      this.queues.set(resolvedName, { options, messages: [] });

      this.bindQueue(resolvedName, '', resolvedName);
    }
    const q = this.queues.get(resolvedName)!;
    const consumerCount = (this.consumers.get(resolvedName) || []).length;
    return { queue: resolvedName, messageCount: q.messages.length, consumerCount };
  }

  bindQueue(queueName: string, exchange: string, pattern: string = '', args?: Record<string, any>): void {
    this.bindings.push({
      exchange,
      binding: {
        queueName,
        pattern,
        headers: args,
      },
    });
  }

  unbindQueue(queueName: string, exchange: string, pattern: string = ''): void {
    this.bindings = this.bindings.filter(
      (b) => !(b.exchange === exchange && b.binding.queueName === queueName && b.binding.pattern === pattern)
    );
  }

  publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    properties: MessageProperties = {}
  ): boolean {
    const ex = this.exchanges.get(exchange);
    if (!ex && exchange !== '') {
      throw new Error(`Exchange '${exchange}' not found`);
    }

    const type = exchange === '' ? 'direct' : ex!.type;
    const matchingQueues = this.routeMessage(type, exchange, routingKey, properties.headers || {});

    if (matchingQueues.length === 0 && ex?.options.alternateExchange) {
      return this.publish(ex.options.alternateExchange, routingKey, content, properties);
    }

    this.deliveryTagCounter++;
    const deliveryTag = this.deliveryTagCounter;

    const msg: AmqpMessage = {
      content,
      fields: {
        deliveryTag,
        redelivered: false,
        exchange,
        routingKey,
      },
      properties: {
        timestamp: Date.now(),
        ...properties,
      },
    };

    for (const qName of matchingQueues) {
      this.enqueueMessage(qName, { ...msg });
    }

    return true;
  }

  private enqueueMessage(queueName: string, msg: AmqpMessage): void {
    const queueData = this.queues.get(queueName);
    if (!queueData) return;

    const maxPriority = queueData.options.arguments?.['x-max-priority'];
    if (maxPriority && msg.properties.priority) {
      const p = Math.min(msg.properties.priority, maxPriority);
      let inserted = false;
      for (let i = 0; i < queueData.messages.length; i++) {
        if ((queueData.messages[i]!.properties.priority || 0) < p) {
          queueData.messages.splice(i, 0, msg);
          inserted = true;
          break;
        }
      }
      if (!inserted) queueData.messages.push(msg);
    } else {
      queueData.messages.push(msg);
    }

    const ttl =
      typeof msg.properties.expiration === 'number'
        ? msg.properties.expiration
        : typeof msg.properties.expiration === 'string'
        ? parseInt(msg.properties.expiration, 10)
        : queueData.options.arguments?.['x-message-ttl'];

    if (ttl && ttl > 0) {
      const timer = setTimeout(() => {
        this.expireMessage(queueName, msg.fields.deliveryTag);
      }, ttl);
      if (timer.unref) timer.unref();
      this.timers.push(timer);
    }

    this.dispatchMessages(queueName);
  }

  private expireMessage(queueName: string, deliveryTag: number): void {
    const queueData = this.queues.get(queueName);
    if (!queueData) return;

    const idx = queueData.messages.findIndex((m) => m.fields.deliveryTag === deliveryTag);
    if (idx !== -1) {
      const [expiredMsg] = queueData.messages.splice(idx, 1);
      if (expiredMsg) {
        this.deadLetterMessage(queueData.options, expiredMsg, 'expired');
      }
    }
  }

  private deadLetterMessage(queueOptions: QueueOptions, msg: AmqpMessage, reason: string): void {
    const dlx = queueOptions.arguments?.['x-dead-letter-exchange'];
    if (!dlx) return;

    const dlxRoutingKey = queueOptions.arguments?.['x-dead-letter-routing-key'] || msg.fields.routingKey;

    const dlxHeaders = {
      ...(msg.properties.headers || {}),
      'x-death': [
        {
          reason,
          queue: queueOptions.arguments?.['x-dead-letter-queue'],
          time: new Date().toISOString(),
          exchange: msg.fields.exchange,
          'routing-keys': [msg.fields.routingKey],
        },
      ],
    };

    this.publish(dlx, dlxRoutingKey, msg.content, {
      ...msg.properties,
      headers: dlxHeaders,
    });
  }

  consume(
    queueName: string,
    callback: (msg: AmqpMessage | null) => void,
    options: { noAck?: boolean; consumerTag?: string; prefetch?: number } = {}
  ): string {
    const consumerTag = options.consumerTag || `ctag_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
    let list = this.consumers.get(queueName);
    if (!list) {
      list = [];
      this.consumers.set(queueName, list);
    }

    list.push({
      consumerTag,
      callback,
      noAck: !!options.noAck,
      unackedCount: 0,
    });

    if (options.prefetch) {
      this.prefetchLimits.set(consumerTag, options.prefetch);
    }

    process.nextTick(() => this.dispatchMessages(queueName));
    return consumerTag;
  }

  private dispatchMessages(queueName: string): void {
    const queueData = this.queues.get(queueName);
    const consumerList = this.consumers.get(queueName);

    if (!queueData || !consumerList || consumerList.length === 0) return;

    while (queueData.messages.length > 0) {

      const availableConsumer = consumerList.find((c) => {
        if (c.noAck) return true;
        const limit = this.prefetchLimits.get(c.consumerTag) || Infinity;
        return c.unackedCount < limit;
      });

      if (!availableConsumer) {
        break;
      }

      const msg = queueData.messages.shift()!;
      msg.fields.consumerTag = availableConsumer.consumerTag;

      if (!availableConsumer.noAck) {
        availableConsumer.unackedCount++;
        this.inFlightMessages.set(msg.fields.deliveryTag, { queueName, msg });
      }

      availableConsumer.callback(msg);
    }
  }

  ack(deliveryTag: number): void {
    const inFlight = this.inFlightMessages.get(deliveryTag);
    if (!inFlight) return;

    this.inFlightMessages.delete(deliveryTag);
    this.decrementUnacked(inFlight.queueName, inFlight.msg.fields.consumerTag);
    this.dispatchMessages(inFlight.queueName);
  }

  nack(deliveryTag: number, allUpTo: boolean = false, requeue: boolean = true): void {
    const inFlight = this.inFlightMessages.get(deliveryTag);
    if (!inFlight) return;

    this.inFlightMessages.delete(deliveryTag);
    this.decrementUnacked(inFlight.queueName, inFlight.msg.fields.consumerTag);

    const queueData = this.queues.get(inFlight.queueName);
    if (!queueData) return;

    if (requeue) {

      inFlight.msg.fields.redelivered = true;
      queueData.messages.unshift(inFlight.msg);
    } else {

      this.deadLetterMessage(queueData.options, inFlight.msg, 'rejected');
    }

    this.dispatchMessages(inFlight.queueName);
  }

  reject(deliveryTag: number, requeue: boolean = false): void {
    this.nack(deliveryTag, false, requeue);
  }

  private decrementUnacked(queueName: string, consumerTag?: string): void {
    if (!consumerTag) return;
    const consumers = this.consumers.get(queueName);
    if (!consumers) return;
    const c = consumers.find((con) => con.consumerTag === consumerTag);
    if (c && c.unackedCount > 0) {
      c.unackedCount--;
    }
  }

  setPrefetch(consumerTag: string, count: number): void {
    this.prefetchLimits.set(consumerTag, count);
  }

  private routeMessage(
    type: string,
    exchange: string,
    routingKey: string,
    headers: Record<string, any>
  ): string[] {
    const matchedQueues = new Set<string>();
    const matchingBindings = this.bindings.filter((b) => b.exchange === exchange);

    for (const b of matchingBindings) {
      if (type === 'fanout') {
        matchedQueues.add(b.binding.queueName);
      } else if (type === 'direct') {
        if (b.binding.pattern === routingKey) {
          matchedQueues.add(b.binding.queueName);
        }
      } else if (type === 'topic') {
        if (this.matchTopicPattern(b.binding.pattern, routingKey)) {
          matchedQueues.add(b.binding.queueName);
        }
      } else if (type === 'headers') {
        if (this.matchHeaders(b.binding.headers || {}, headers)) {
          matchedQueues.add(b.binding.queueName);
        }
      }
    }

    return Array.from(matchedQueues);
  }

  private matchTopicPattern(pattern: string, routingKey: string): boolean {
    if (pattern === '#' || pattern === routingKey) return true;

    const patternParts = pattern.split('.');
    const keyParts = routingKey.split('.');

    return this.matchTopicRecursive(patternParts, keyParts, 0, 0);
  }

  private matchTopicRecursive(
    pattern: string[],
    key: string[],
    pIdx: number,
    kIdx: number
  ): boolean {
    if (pIdx === pattern.length && kIdx === key.length) return true;
    if (pIdx === pattern.length) return false;

    const p = pattern[pIdx]!;

    if (p === '#') {

      if (pIdx === pattern.length - 1) return true;
      for (let i = kIdx; i <= key.length; i++) {
        if (this.matchTopicRecursive(pattern, key, pIdx + 1, i)) {
          return true;
        }
      }
      return false;
    }

    if (kIdx === key.length) return false;

    if (p === '*' || p === key[kIdx]) {
      return this.matchTopicRecursive(pattern, key, pIdx + 1, kIdx + 1);
    }

    return false;
  }

  private matchHeaders(bindingHeaders: Record<string, any>, messageHeaders: Record<string, any>): boolean {
    const xMatch = bindingHeaders['x-match'] || 'all';
    const keysToCheck = Object.keys(bindingHeaders).filter((k) => k !== 'x-match');

    if (keysToCheck.length === 0) return true;

    if (xMatch === 'all') {
      return keysToCheck.every((key) => messageHeaders[key] === bindingHeaders[key]);
    } else if (xMatch === 'any') {
      return keysToCheck.some((key) => messageHeaders[key] === bindingHeaders[key]);
    }
    return false;
  }

  reset(): void {
    this.timers.forEach((t) => clearTimeout(t));
    this.timers = [];
    this.queues.clear();
    this.bindings = [];
    this.consumers.clear();
    this.inFlightMessages.clear();
    this.prefetchLimits.clear();
    this.exchanges.clear();
    this.exchanges.set('', { type: 'direct', options: { durable: true } });
  }
}
