import { OrderItem } from './OrderEvents.js';

export interface InventoryReservedEvent {
  orderId: string;
  items: OrderItem[];
  reservationId: string;
  reservedAt: number;
}

export interface InventoryReservationFailedEvent {
  orderId: string;
  items: OrderItem[];
  reason: string;
  failedAt: number;
}

export interface InventoryReleasedEvent {
  orderId: string;
  reservationId: string;
  reason: string;
  releasedAt: number;
}
