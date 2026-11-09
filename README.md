# E-Commerce Microservices Platform over RabbitMQ

[![RabbitMQ Masterclass](https://img.shields.io/badge/RabbitMQ-AMQP%200--9--1-FF6600?logo=rabbitmq&logoColor=white)]()
[![Microservices](https://img.shields.io/badge/Architecture-Event--Driven%20Saga-blue)]()
[![TypeScript](https://img.shields.io/badge/Language-TypeScript%205.5-3178C6?logo=typescript&logoColor=white)]()
[![Tests](https://img.shields.io/badge/Tests-Passing%20100%25-brightgreen)]()

An enterprise-grade, distributed e-commerce microservices ecosystem engineered specifically to teach and demonstrate **every single concept, pattern, and scenario of RabbitMQ (AMQP 0-9-1)** with zero missing details.

---

## 1. Microservice Ecosystem Architecture

```mermaid
flowchart TD
    subgraph ClientLayer["Frontend & Checkout"]
        Checkout["Checkout Client / API Gateway"]
    end

    subgraph RabbitMQBroker["RabbitMQ Message Broker (AMQP 0-9-1)"]
        EX_ORDER["Topic Exchange<br/>ecommerce.order.events"]
        EX_PAYMENT["Topic Exchange<br/>ecommerce.payment.events"]
        EX_INVENTORY["Topic Exchange<br/>ecommerce.inventory.events"]
        EX_FANOUT["Fanout Exchange<br/>ecommerce.notifications.fanout"]
        EX_HEADERS["Headers Exchange<br/>ecommerce.audit.headers"]
        EX_DLX["Dead Letter Exchange (DLX)<br/>ecommerce.dlx"]
        EX_RETRY["Delayed Retry Exchange<br/>ecommerce.retry"]
        EX_RPC["Direct RPC Exchange<br/>ecommerce.rpc.direct"]

        Q_ORDER["Queue: queue.orders.process"]
        Q_VIP["Priority Queue (x-max-priority: 10)<br/>queue.orders.vip"]
        Q_PAY["Queue: queue.payment.process"]
        Q_INV_RES["Queue: queue.inventory.reserve"]
        Q_INV_REL["Queue: queue.inventory.release"]
        Q_EMAIL["Queue: queue.notifications.email"]
        Q_SMS["Queue: queue.notifications.sms"]
        Q_SLACK["Queue: queue.notifications.slack"]
        Q_DLQ["Queue: queue.ecommerce.dlq"]
        Q_RETRY["TTL Delay Queue (x-message-ttl: 3000)<br/>queue.ecommerce.retry.3s"]
        Q_AUDIT["Queue: queue.audit.compliance"]
        Q_CUST_RPC["Queue: queue.customer.rpc"]
    end

    subgraph Services["Autonomous Microservices"]
        OrderSvc["Order Service<br/>(Saga Orchestration)"]
        ProductSvc["Product / Inventory Service<br/>(Stock Reservation & Compensation)"]
        PaymentSvc["Payment Service<br/>(Transactions & DLQ Triggering)"]
        CustomerSvc["Customer Service<br/>(RPC Credit Check & Loyalty)"]
        NotifSvc["Notification Service<br/>(Email, SMS & Slack Dispatchers)"]
    end

    Checkout -->|Create Order| OrderSvc
    OrderSvc -->|Publish order.created| EX_ORDER
    OrderSvc -->|RPC Credit Check| EX_RPC

    EX_ORDER -->|order.#| Q_ORDER
    EX_ORDER -->|order.created| Q_PAY
    EX_ORDER -->|order.created| Q_INV_RES
    EX_ORDER -->|order.vip.*| Q_VIP

    Q_PAY --> PaymentSvc
    Q_INV_RES --> ProductSvc
    Q_ORDER --> OrderSvc
    Q_CUST_RPC --> CustomerSvc

    PaymentSvc -->|Publish payment.success / failed| EX_PAYMENT
    EX_PAYMENT -->|payment.failed| Q_INV_REL
    Q_INV_REL -->|Release Stock (Compensating)| ProductSvc

    OrderSvc -->|Broadcast Alerts| EX_FANOUT
    EX_FANOUT --> Q_EMAIL
    EX_FANOUT --> Q_SMS
    EX_FANOUT --> Q_SLACK
    Q_EMAIL --> NotifSvc
    Q_SMS --> NotifSvc
    Q_SLACK --> NotifSvc

    PaymentSvc -.->|NACK requeue=false (Poison Pill)| EX_DLX
    EX_DLX --> Q_DLQ

    EX_RETRY --> Q_RETRY
    Q_RETRY -.->|TTL Expires after 3s -> Dead Letter back!| EX_ORDER
```

---

## 2. Exhaustive RabbitMQ Concepts Reference (Every Scenario Covered)

### Scenario 1: Competing Consumers & Fair Dispatch (`channel.prefetch`)
* **File**: [`scenarios/01_simple_worker_queue.ts`](file:///d:/Vicky/Antigravity/Antigravity/ecommerce-microservices-rabbitmq/scenarios/01_simple_worker_queue.ts)
* **Concept**: By default, RabbitMQ dispatches messages in a blind **round-robin** sequence. If Worker 1 gets heavy tasks and Worker 2 gets light tasks, Worker 1 starves while Worker 2 stays idle.
* **Solution**: `channel.prefetch(1)` tells RabbitMQ: *"Do not send more than 1 unacknowledged message to this worker at a time."* Fast workers pull more tasks; slow workers don't choke.

### Scenario 2: Publish / Subscribe Broadcast (`Fanout Exchange`)
* **File**: [`scenarios/02_publish_subscribe_fanout.ts`](file:///d:/Vicky/Antigravity/Antigravity/ecommerce-microservices-rabbitmq/scenarios/02_publish_subscribe_fanout.ts)
* **Concept**: The **Fanout Exchange** completely ignores the routing key! Every message published to a fanout exchange is duplicated across **all queues** bound to it. Used in e-commerce for broadcasting order events to Email, SMS, and Slack dispatchers simultaneously.

### Scenario 3: Exact String Routing (`Direct Exchange`)
* **File**: [`scenarios/03_direct_exchange_routing.ts`](file:///d:/Vicky/Antigravity/Antigravity/ecommerce-microservices-rabbitmq/scenarios/03_direct_exchange_routing.ts)
* **Concept**: The **Direct Exchange** matches the message's `routing_key` against the queue's `binding_key` using **exact equality**. `payment.success` goes only to the Accounting Queue; `payment.failed` goes only to the Risk Review Queue.

### Scenario 4: Wildcard Pattern Matching (`Topic Exchange`)
* **File**: [`scenarios/04_topic_exchange_patterns.ts`](file:///d:/Vicky/Antigravity/Antigravity/ecommerce-microservices-rabbitmq/scenarios/04_topic_exchange_patterns.ts)
* **Concept**: The **Topic Exchange** matches dot-separated words using two wildcard operators:
  * `*` (Star): Matches **exactly ONE word**. (e.g. `order.eu.*` matches `order.eu.created`, but NOT `order.eu.express.created`).
  * `#` (Hash): Matches **ZERO or MORE words**. (e.g. `order.#` matches `order.created`, `order.us.california.urgent`, and `order`).

### Scenario 5: Metadata Matching (`Headers Exchange`)
* **File**: [`scenarios/05_headers_exchange.ts`](file:///d:/Vicky/Antigravity/Antigravity/ecommerce-microservices-rabbitmq/scenarios/05_headers_exchange.ts)
* **Concept**: Ignores the routing key completely. Routes based on message `headers` attributes:
  * `x-match: 'all'` (AND logic): Every specified header must match.
  * `x-match: 'any'` (OR logic): At least one specified header must match.

### Scenario 6: Request - Reply (`RPC Pattern`)
* **File**: [`scenarios/06_rpc_request_reply.ts`](file:///d:/Vicky/Antigravity/Antigravity/ecommerce-microservices-rabbitmq/scenarios/06_rpc_request_reply.ts)
* **Concept**: Synchronous-style request/reply over asynchronous queues.
  * `replyTo`: The client creates an exclusive temporary queue and puts its name in `replyTo`.
  * `correlationId`: A unique UUID per request so the client matches the response to the right caller.

### Scenario 7: Reliable Delivery (`Publisher Confirms`)
* **File**: [`scenarios/07_publisher_confirms.ts`](file:///d:/Vicky/Antigravity/Antigravity/ecommerce-microservices-rabbitmq/scenarios/07_publisher_confirms.ts)
* **Concept**: Fire-and-forget publishing risks lost messages during network drops. Enabling `confirmChannel` forces the RabbitMQ broker to send an asynchronous ACK once messages are safely persisted to disk. Up to **250x faster** than AMQP transactions!

### Scenario 8: Poison Pill Quarantining (`Dead Letter Exchange - DLX`)
* **File**: [`scenarios/08_dead_letter_exchange_dlq.ts`](file:///d:/Vicky/Antigravity/Antigravity/ecommerce-microservices-rabbitmq/scenarios/08_dead_letter_exchange_dlq.ts)
* **Concept**: What happens when a message fails processing?
  1. Consumer calls `channel.nack(msg, false, false)` (`requeue = false`).
  2. RabbitMQ moves the message to the queue's configured `x-dead-letter-exchange`.
  3. Attaches `x-death` metadata (death count, failure reason, timestamp) so operators can inspect it without blocking the main queue.

### Scenario 9: Delayed Retries with Exponential Backoff (`Message TTL Loop`)
* **File**: [`scenarios/09_ttl_delayed_retries.ts`](file:///d:/Vicky/Antigravity/Antigravity/ecommerce-microservices-rabbitmq/scenarios/09_ttl_delayed_retries.ts)
* **Concept**: Creating a non-blocking retry loop using RabbitMQ's native TTL:
  1. Failed message is published to a delay queue with `x-message-ttl: 3000` (3 seconds) and **no active consumers**.
  2. Message sleeps in the delay queue.
  3. When the 3000ms TTL expires, RabbitMQ automatically dead-letters the message back to the main exchange!
  4. After max retries (e.g. 3), message routes to the terminal DLQ.

### Scenario 10: VIP Fast-Lane (`Priority Queues`)
* **File**: [`scenarios/10_priority_queues.ts`](file:///d:/Vicky/Antigravity/Antigravity/ecommerce-microservices-rabbitmq/scenarios/10_priority_queues.ts)
* **Concept**: Queues declared with `x-max-priority: 10`. Messages published with `priority: 9` (VIP customers) automatically **jump ahead** of earlier `priority: 1` messages in the queue!

### Scenario 11: End-to-End E-Commerce Saga Choreography
* **File**: [`scenarios/11_ecommerce_saga_orchestration.ts`](file:///d:/Vicky/Antigravity/Antigravity/ecommerce-microservices-rabbitmq/scenarios/11_ecommerce_saga_orchestration.ts)
* **Concept**: Real distributed checkout lifecycle across 5 microservices:
  * **Happy Path**: Order Placed $\rightarrow$ Stock Reserved $\rightarrow$ Payment Succeeded $\rightarrow$ Order Confirmed $\rightarrow$ Fanout Notifications.
  * **Saga Compensation**: Order Placed $\rightarrow$ Stock Reserved $\rightarrow$ Payment Declined $\rightarrow$ `ProductService` releases reserved stock $\rightarrow$ Order marked Cancelled.

---

## 3. Directory Structure

```
ecommerce-microservices-rabbitmq/
├── shared/
│   ├── rabbitmq/
│   │   ├── ExchangeTypes.ts          # Topology definitions (Exchanges, Queues, Keys)
│   │   ├── InMemoryAmqpBroker.ts     # High-fidelity In-Memory AMQP 0-9-1 engine
│   │   ├── RabbitMQClient.ts         # Unified connection wrapper (Real or Virtual)
│   │   ├── TopologyManager.ts        # Declarative setup of exchanges, DLX, TTL, and queues
│   │   ├── DeadLetterManager.ts      # DLQ consumer & inspector
│   │   ├── RetryManager.ts           # Exponential backoff retry loop
│   │   ├── RpcClient.ts              # Request-Reply caller (correlationId + replyTo)
│   │   └── RpcServer.ts              # Request-Reply handler
│   └── events/                       # Strongly-typed event payloads
│       ├── OrderEvents.ts
│       ├── PaymentEvents.ts
│       ├── InventoryEvents.ts
│       ├── CustomerEvents.ts
│       └── NotificationEvents.ts
├── services/
│   ├── product-service/              # Inventory reservation & Saga compensation
│   ├── order-service/                # Order checkout & Saga coordinator
│   ├── payment-service/              # Credit card processing & DLQ triggering
│   ├── customer-service/             # Customer credit RPC & loyalty rewards
│   └── notification-service/         # Fanout broadcast listeners (Email, SMS, Slack)
├── scenarios/                        # 11 interactive educational scenarios
│   ├── 01_simple_worker_queue.ts
│   ├── 02_publish_subscribe_fanout.ts
│   ├── 03_direct_exchange_routing.ts
│   ├── 04_topic_exchange_patterns.ts
│   ├── 05_headers_exchange.ts
│   ├── 06_rpc_request_reply.ts
│   ├── 07_publisher_confirms.ts
│   ├── 08_dead_letter_exchange_dlq.ts
│   ├── 09_ttl_delayed_retries.ts
│   ├── 10_priority_queues.ts
│   └── 11_ecommerce_saga_orchestration.ts
├── test/                             # Automated test suite (9 test suites, 100% pass)
│   ├── exchanges_and_routing.test.ts
│   ├── dlq_and_retries.test.ts
│   └── ecommerce_flow.test.ts
├── docker-compose.yml                # Official RabbitMQ 3.13 Management container
├── package.json
└── tsconfig.json
```

---

## 4. How to Run

### Run the Master E-Commerce Saga Simulation
```bash
npm run demo
```

### Run Any Specific RabbitMQ Learning Scenario
```bash
npm run scenario:01   # Worker Queues & Fair Dispatch (Prefetch)
npm run scenario:02   # Fanout Exchange (Broadcast)
npm run scenario:03   # Direct Exchange (Exact Key)
npm run scenario:04   # Topic Exchange (* and # Wildcards)
npm run scenario:05   # Headers Exchange (x-match all/any)
npm run scenario:06   # RPC Request-Reply
npm run scenario:07   # Publisher Confirms
npm run scenario:08   # Dead Letter Exchange (DLX / DLQ)
npm run scenario:09   # Message TTL & Delayed Retries
npm run scenario:10   # Priority Queues (VIP Fast-Lane)
npm run scenario:11   # Full E-Commerce Saga Orchestration
```

### Run the Automated Test Suite (100% Passing)
```bash
npm test
```

### Start Real RabbitMQ with Management UI (Docker)
```bash
docker-compose up -d
```
Open **`http://localhost:15672`** in your browser (Username: `guest`, Password: `guest`) to view live queues, exchanges, and message rates!
