export interface CustomerCreditCheckRequest {
  customerId: string;
  requiredAmount: number;
}

export interface CustomerCreditCheckResponse {
  customerId: string;
  hasSufficientCredit: boolean;
  currentBalance: number;
  creditScore: number;
  tier: 'standard' | 'gold' | 'vip';
}

export interface CustomerLoyaltyUpdatedEvent {
  customerId: string;
  orderId: string;
  pointsAdded: number;
  totalPoints: number;
}
