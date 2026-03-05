
import type { SubscriptionTier } from '../../access/subscription.types';

export interface AuthenticatedUser {
  id: string;
  userId: string;
  email?: string;
  role: string;
  subscriptionTier: SubscriptionTier;
  tokenVersion?: number;
}
