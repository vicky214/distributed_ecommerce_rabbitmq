import { ChannelWrapper } from '../../shared/rabbitmq/RabbitMQClient.js';
import { RpcServer } from '../../shared/rabbitmq/RpcServer.js';
import { EXCHANGES, ROUTING_KEYS } from '../../shared/rabbitmq/ExchangeTypes.js';
import {
  CustomerCreditCheckRequest,
  CustomerCreditCheckResponse,
  CustomerLoyaltyUpdatedEvent,
} from '../../shared/events/CustomerEvents.js';

interface CustomerProfile {
  id: string;
  name: string;
  creditBalance: number;
  creditScore: number;
  tier: 'standard' | 'gold' | 'vip';
  loyaltyPoints: number;
}

export class CustomerService {
  private customers = new Map<string, CustomerProfile>();
  private rpcServer: RpcServer;

  constructor(private channel: ChannelWrapper) {
    this.rpcServer = new RpcServer(channel);
    this.seedCustomers();
  }

  private seedCustomers(): void {
    this.customers.set('CUST-101', {
      id: 'CUST-101',
      name: 'Alice Johnson',
      creditBalance: 5000,
      creditScore: 780,
      tier: 'vip',
      loyaltyPoints: 1200,
    });
    this.customers.set('CUST-102', {
      id: 'CUST-102',
      name: 'Bob Smith',
      creditBalance: 500,
      creditScore: 620,
      tier: 'standard',
      loyaltyPoints: 50,
    });
  }

  async start(): Promise<void> {
    console.log('[CustomerService] Starting RPC Server & Customer Listeners...');

    await this.rpcServer.respond<CustomerCreditCheckRequest, CustomerCreditCheckResponse>(
      'queue.customer.rpc',
      async (req) => {
        console.log(`[CustomerService RPC] Received Credit Inquiry for Customer ${req.customerId} (Requested: $${req.requiredAmount})...`);
        const customer = this.customers.get(req.customerId);

        if (!customer) {
          return {
            customerId: req.customerId,
            hasSufficientCredit: false,
            currentBalance: 0,
            creditScore: 0,
            tier: 'standard',
          };
        }

        const hasSufficientCredit = customer.creditBalance >= req.requiredAmount;
        return {
          customerId: customer.id,
          hasSufficientCredit,
          currentBalance: customer.creditBalance,
          creditScore: customer.creditScore,
          tier: customer.tier,
        };
      }
    );

    const queue = await this.channel.assertQueue('queue.customer.loyalty', { durable: true });
    await this.channel.bindQueue(queue.queue, EXCHANGES.PAYMENT_EVENTS, ROUTING_KEYS.PAYMENT_SUCCESS);

    await this.channel.consume(queue.queue, async (msg) => {
      if (!msg) return;

      try {
        const payment = JSON.parse(msg.content.toString());
        const customer = this.customers.get(payment.customerId);
        if (customer) {
          const pointsEarned = Math.floor(payment.amount / 10);
          customer.loyaltyPoints += pointsEarned;
          console.log(`[CustomerService] Awarded ${pointsEarned} loyalty points to ${customer.name}. New balance: ${customer.loyaltyPoints}`);

          const loyaltyEvent: CustomerLoyaltyUpdatedEvent = {
            customerId: customer.id,
            orderId: payment.orderId,
            pointsAdded: pointsEarned,
            totalPoints: customer.loyaltyPoints,
          };

          await this.channel.publish(
            'ecommerce.customer.events',
            'customer.loyalty.updated',
            Buffer.from(JSON.stringify(loyaltyEvent)),
            { contentType: 'application/json' }
          );
        }
        this.channel.ack(msg);
      } catch (err) {
        this.channel.ack(msg);
      }
    });
  }

  getCustomer(id: string): CustomerProfile | undefined {
    return this.customers.get(id);
  }
}
