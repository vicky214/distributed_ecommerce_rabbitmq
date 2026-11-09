export interface PaymentProcessedEvent {
  paymentId: string;
  orderId: string;
  customerId: string;
  amount: number;
  currency: string;
  status: 'SUCCESS' | 'FAILED';
  failureReason?: string;
  processedAt: number;
}

export interface RefundProcessedEvent {
  refundId: string;
  orderId: string;
  amount: number;
  reason: string;
  refundedAt: number;
}
