import { RabbitMQClient } from '../shared/rabbitmq/RabbitMQClient.js';
import { RpcClient } from '../shared/rabbitmq/RpcClient.js';
import { RpcServer } from '../shared/rabbitmq/RpcServer.js';

interface CreditRequest {
  customerId: string;
  requestedCredit: number;
}

interface CreditResponse {
  customerId: string;
  approved: boolean;
  creditLimit: number;
}

async function runScenario() {
  console.log('\n========================================================================');
  console.log('   SCENARIO 06: REQUEST-REPLY PATTERN (RPC WITH CORRELATION ID)');
  console.log('========================================================================\n');

  const client = new RabbitMQClient();
  await client.connect(false);

  const serverChannel = await client.createChannel();
  const clientChannel = await client.createChannel();

  const RPC_QUEUE = 'queue.rpc.credit_check';

  const rpcServer = new RpcServer(serverChannel);
  await rpcServer.respond<CreditRequest, CreditResponse>(RPC_QUEUE, async (req) => {
    console.log(`[RPC Server] Processing Credit Check for ${req.customerId} (Request: $${req.requestedCredit})...`);

    const approved = req.requestedCredit <= 5000;
    return {
      customerId: req.customerId,
      approved,
      creditLimit: 5000,
    };
  });

  const rpcClient = new RpcClient(clientChannel);
  await rpcClient.init();

  console.log('[RPC Client] Sending 2 concurrent RPC requests to Credit Server...\n');

  const promise1 = rpcClient.call<CreditRequest, CreditResponse>('', RPC_QUEUE, {
    customerId: 'CUST-ALICE',
    requestedCredit: 1200,
  });

  const promise2 = rpcClient.call<CreditRequest, CreditResponse>('', RPC_QUEUE, {
    customerId: 'CUST-BOB',
    requestedCredit: 8500,
  });

  const [res1, res2] = await Promise.all([promise1, promise2]);

  console.log('\n--- RPC RESPONSES RECEIVED BY CLIENT ---');
  console.log(`Response 1: Alice -> Approved: ${res1.approved} (Credit Limit: $${res1.creditLimit})`);
  console.log(`Response 2: Bob   -> Approved: ${res2.approved} (Credit Limit: $${res2.creditLimit})`);
  console.log('Notice: Client matched responses to the correct requests via correlationId!\n');
}

runScenario();
