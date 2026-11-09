export interface OrderItem {
  productId: string;
  name: string;
  quantity: number;
  price: number;
}

export interface OrderCreatedEvent {
  orderId: string;
  customerId: string;
  customerTier: 'standard' | 'gold' | 'vip';
  items: OrderItem[];
  totalAmount: number;
  currency: string;
  shippingAddress: string;
  createdAt: number;
}

export interface OrderCancelledEvent {
  orderId: string;
  customerId: string;
  reason: string;
  cancelledAt: number;
}

export interface OrderCompletedEvent {
  orderId: string;
  customerId: string;
  totalAmount: number;
  completedAt: number;
}
