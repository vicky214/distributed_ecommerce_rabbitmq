export interface NotificationBroadcastEvent {
  notificationId: string;
  recipientEmail?: string;
  recipientPhone?: string;
  type: 'ORDER_CONFIRMATION' | 'PAYMENT_RECEIPT' | 'ORDER_SHIPPED' | 'ORDER_CANCELLED' | 'STOCK_ALERT';
  title: string;
  body: string;
  timestamp: number;
  metadata?: Record<string, unknown>;
}
