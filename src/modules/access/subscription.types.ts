export type SubscriptionTier = 'free' | 'premium' | 'enterprise';

export type AccessFeature = 'optimizer' | 'advancedAnalytics' | 'apiAccess';

export type SubscriptionFeatures = {
  optimizer: boolean;
  advancedAnalytics: boolean;
  apiAccess: boolean;
};

export type SubscriptionLimits = {
  maxProtocols: number | null;
  maxTrackedChains: number | null;
  apiMonthlyQuota: number | null;
};

export type SubscriptionPolicy = {
  tier: SubscriptionTier;
  features: SubscriptionFeatures;
  limits: SubscriptionLimits;
};

export type SubscriptionApiUsage = {
  period: string;
  used: number;
  quota: number | null;
  remaining: number | null;
  resetAt: string;
};

export type SubscriptionInfo = {
  tier: SubscriptionTier;
  features: SubscriptionFeatures;
  limits: SubscriptionLimits;
  apiUsage: SubscriptionApiUsage;
};

export type UserAccessProfile = SubscriptionInfo & {
  userId: string;
};
