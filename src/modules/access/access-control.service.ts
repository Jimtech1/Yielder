import {
  ForbiddenException,
  Injectable,
  NotFoundException,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import {
  User,
  UserApiUsage,
  UserDocument,
} from '../auth/schemas/user.schema';
import {
  AccessFeature,
  SubscriptionInfo,
  SubscriptionPolicy,
  SubscriptionTier,
  UserAccessProfile,
} from './subscription.types';

@Injectable()
export class AccessControlService {
  private readonly policies: Record<SubscriptionTier, SubscriptionPolicy>;

  constructor(
    @InjectModel(User.name) private readonly userModel: Model<UserDocument>,
    private readonly configService: ConfigService,
  ) {
    this.policies = this.buildPolicies();
  }

  normalizeTier(rawTier: unknown): SubscriptionTier {
    const normalized = typeof rawTier === 'string' ? rawTier.trim().toLowerCase() : '';
    if (normalized === 'premium' || normalized === 'enterprise') {
      return normalized;
    }
    return 'free';
  }

  resolvePolicyForTier(rawTier: unknown): SubscriptionPolicy {
    const tier = this.normalizeTier(rawTier);
    return this.policies[tier];
  }

  async getSubscriptionInfo(userId: string): Promise<SubscriptionInfo> {
    const user = await this.userModel
      .findById(userId)
      .select('_id subscriptionTier apiUsage')
      .lean()
      .exec();

    if (!user) {
      throw new NotFoundException('User not found');
    }

    return this.buildSubscriptionInfo({
      tier: this.normalizeTier(user.subscriptionTier),
      apiUsage: this.parseUserApiUsage(user.apiUsage),
    });
  }

  async getUserAccessProfile(userId: string): Promise<UserAccessProfile> {
    const user = await this.userModel
      .findById(userId)
      .select('_id subscriptionTier apiUsage')
      .lean()
      .exec();

    if (!user) {
      throw new UnauthorizedException('User not found');
    }

    const tier = this.normalizeTier(user.subscriptionTier);
    const subscription = this.buildSubscriptionInfo({
      tier,
      apiUsage: this.parseUserApiUsage(user.apiUsage),
    });

    return {
      userId,
      ...subscription,
    };
  }

  async assertFeatureForUser(
    userId: string,
    feature: AccessFeature,
    message?: string,
  ): Promise<UserAccessProfile> {
    const profile = await this.getUserAccessProfile(userId);
    this.assertFeature(profile, feature, message);
    return profile;
  }

  assertFeature(
    profile: Pick<UserAccessProfile, 'tier' | 'features'>,
    feature: AccessFeature,
    message?: string,
  ): void {
    const enabled = this.featureEnabled(profile.features, feature);
    if (enabled) {
      return;
    }

    if (message) {
      throw new ForbiddenException(message);
    }

    throw new ForbiddenException(this.defaultFeatureMessage(feature, profile.tier));
  }

  assertDistinctLimit(params: {
    profile: Pick<UserAccessProfile, 'tier'>;
    currentValues: string[];
    candidateValue: string;
    limit: number | null;
    resourceLabel: string;
    message?: string;
  }): void {
    if (params.limit === null) {
      return;
    }

    const candidate = params.candidateValue.trim().toLowerCase();
    if (!candidate) {
      return;
    }

    const values = new Set(
      params.currentValues
        .map((value) => value.trim().toLowerCase())
        .filter((value) => value.length > 0),
    );

    if (values.has(candidate)) {
      return;
    }

    if (values.size < params.limit) {
      return;
    }

    const resourceText = params.resourceLabel.trim() || 'resources';
    const defaultMessage =
      `Your ${profileLabel(params.profile.tier)} plan supports up to ${params.limit} ` +
      `${resourceText}. Upgrade your plan to increase this limit.`;
    throw new ForbiddenException(params.message || defaultMessage);
  }

  async consumeApiQuota(userId: string, units = 1): Promise<{
    used: number;
    quota: number | null;
    remaining: number | null;
    period: string;
  }> {
    const consumedUnits = Number.isFinite(units) ? Math.max(Math.floor(units), 1) : 1;
    const user = await this.userModel.findById(userId).exec();
    if (!user) {
      throw new UnauthorizedException('User not found');
    }

    const tier = this.normalizeTier(user.subscriptionTier);
    const policy = this.resolvePolicyForTier(tier);
    const features = policy.features;
    if (!features.apiAccess) {
      throw new ForbiddenException(this.defaultFeatureMessage('apiAccess', tier));
    }

    const period = this.currentUsagePeriod();
    const currentUsage = this.parseUserApiUsage(user.apiUsage);
    const baseUsed = currentUsage.period === period ? currentUsage.used : 0;
    const nextUsed = baseUsed + consumedUnits;
    const quota = policy.limits.apiMonthlyQuota;

    if (quota !== null && nextUsed > quota) {
      const remaining = Math.max(quota - baseUsed, 0);
      throw new ForbiddenException(
        `Monthly API quota exceeded (${baseUsed}/${quota} used). ` +
          `Remaining this month: ${remaining}. Upgrade your plan for higher API limits.`,
      );
    }

    user.apiUsage = {
      period,
      used: nextUsed,
      lastRequestAt: new Date(),
    };
    await user.save();

    return {
      used: nextUsed,
      quota,
      remaining: quota === null ? null : Math.max(quota - nextUsed, 0),
      period,
    };
  }

  async updateUserSubscriptionTier(params: {
    targetUserId: string;
    tier: SubscriptionTier;
    updatedByUserId: string;
  }): Promise<{
    userId: string;
    subscription: SubscriptionInfo;
  }> {
    const tier = this.normalizeTier(params.tier);
    const user = await this.userModel
      .findByIdAndUpdate(
        params.targetUserId,
        {
          $set: {
            subscriptionTier: tier,
            subscriptionUpdatedAt: new Date(),
            subscriptionUpdatedBy: params.updatedByUserId,
          },
        },
        { new: true },
      )
      .exec();

    if (!user) {
      throw new NotFoundException('User not found');
    }

    const subscription = this.buildSubscriptionInfo({
      tier,
      apiUsage: this.parseUserApiUsage(user.apiUsage),
    });

    return {
      userId: String((user as unknown as { _id: unknown })._id),
      subscription,
    };
  }

  async updateUserSubscriptionTierByEmail(params: {
    email: string;
    tier: SubscriptionTier;
    updatedByUserId: string;
  }): Promise<{
    userId: string;
    email: string;
    subscription: SubscriptionInfo;
  }> {
    const normalizedEmail = params.email.trim().toLowerCase();
    if (!normalizedEmail) {
      throw new NotFoundException('User not found');
    }

    const tier = this.normalizeTier(params.tier);
    const user = await this.userModel
      .findOneAndUpdate(
        { email: normalizedEmail },
        {
          $set: {
            subscriptionTier: tier,
            subscriptionUpdatedAt: new Date(),
            subscriptionUpdatedBy: params.updatedByUserId,
          },
        },
        { new: true },
      )
      .exec();

    if (!user) {
      throw new NotFoundException('User not found');
    }

    const subscription = this.buildSubscriptionInfo({
      tier,
      apiUsage: this.parseUserApiUsage(user.apiUsage),
    });

    return {
      userId: String((user as unknown as { _id: unknown })._id),
      email: typeof user.email === 'string' ? user.email : normalizedEmail,
      subscription,
    };
  }

  buildSubscriptionInfoFromUser(user: {
    subscriptionTier?: unknown;
    apiUsage?: unknown;
  }): SubscriptionInfo {
    return this.buildSubscriptionInfo({
      tier: this.normalizeTier(user.subscriptionTier),
      apiUsage: this.parseUserApiUsage(user.apiUsage),
    });
  }

  private buildSubscriptionInfo(params: {
    tier: SubscriptionTier;
    apiUsage: UserApiUsage;
  }): SubscriptionInfo {
    const policy = this.resolvePolicyForTier(params.tier);
    const period = this.currentUsagePeriod();
    const used = params.apiUsage.period === period ? params.apiUsage.used : 0;
    const quota = policy.limits.apiMonthlyQuota;

    return {
      tier: policy.tier,
      features: policy.features,
      limits: policy.limits,
      apiUsage: {
        period,
        used,
        quota,
        remaining: quota === null ? null : Math.max(quota - used, 0),
        resetAt: this.nextUsagePeriodStartIso(),
      },
    };
  }

  private buildPolicies(): Record<SubscriptionTier, SubscriptionPolicy> {
    const defaultPolicies: Record<SubscriptionTier, SubscriptionPolicy> = {
      free: {
        tier: 'free',
        features: {
          optimizer: true,
          advancedAnalytics: false,
          apiAccess: false,
        },
        limits: {
          maxProtocols: 3,
          maxTrackedChains: 2,
          apiMonthlyQuota: 0,
        },
      },
      premium: {
        tier: 'premium',
        features: {
          optimizer: true,
          advancedAnalytics: true,
          apiAccess: true,
        },
        limits: {
          maxProtocols: 10,
          maxTrackedChains: 5,
          apiMonthlyQuota: 10000,
        },
      },
      enterprise: {
        tier: 'enterprise',
        features: {
          optimizer: true,
          advancedAnalytics: true,
          apiAccess: true,
        },
        limits: {
          maxProtocols: null,
          maxTrackedChains: null,
          apiMonthlyQuota: null,
        },
      },
    };

    const configuredPolicies: Record<SubscriptionTier, SubscriptionPolicy> = {
      free: this.applyPolicyOverrides(defaultPolicies.free, 'FREE'),
      premium: this.applyPolicyOverrides(defaultPolicies.premium, 'PREMIUM'),
      enterprise: this.applyPolicyOverrides(defaultPolicies.enterprise, 'ENTERPRISE'),
    };

    if (this.isTestnetNetwork()) {
      return {
        free: this.buildTestnetOpenPolicy('free'),
        premium: this.buildTestnetOpenPolicy('premium'),
        enterprise: this.buildTestnetOpenPolicy('enterprise'),
      };
    }

    return configuredPolicies;
  }

  private applyPolicyOverrides(
    policy: SubscriptionPolicy,
    envTier: 'FREE' | 'PREMIUM' | 'ENTERPRISE',
  ): SubscriptionPolicy {
    const maxProtocols = this.readOptionalLimit(
      `SUBSCRIPTION_${envTier}_MAX_PROTOCOLS`,
      policy.limits.maxProtocols,
    );
    const maxTrackedChains = this.readOptionalLimit(
      `SUBSCRIPTION_${envTier}_MAX_TRACKED_CHAINS`,
      policy.limits.maxTrackedChains,
    );
    const apiMonthlyQuota = this.readOptionalLimit(
      `SUBSCRIPTION_${envTier}_API_MONTHLY_QUOTA`,
      policy.limits.apiMonthlyQuota,
    );

    return {
      tier: policy.tier,
      features: {
        optimizer: this.readBoolean(
          `SUBSCRIPTION_${envTier}_ENABLE_OPTIMIZER`,
          policy.features.optimizer,
        ),
        advancedAnalytics: this.readBoolean(
          `SUBSCRIPTION_${envTier}_ENABLE_ADVANCED_ANALYTICS`,
          policy.features.advancedAnalytics,
        ),
        apiAccess: this.readBoolean(
          `SUBSCRIPTION_${envTier}_ENABLE_API_ACCESS`,
          policy.features.apiAccess,
        ),
      },
      limits: {
        maxProtocols,
        maxTrackedChains,
        apiMonthlyQuota,
      },
    };
  }

  private readOptionalLimit(envKey: string, fallback: number | null): number | null {
    const raw = this.configService.get<string>(envKey);
    if (raw === undefined || raw === null || raw.trim().length === 0) {
      return fallback;
    }

    if (raw.trim().toLowerCase() === 'unlimited') {
      return null;
    }

    const parsed = Number.parseInt(raw, 10);
    if (!Number.isFinite(parsed)) {
      return fallback;
    }
    if (parsed <= 0) {
      return null;
    }
    return parsed;
  }

  private readBoolean(envKey: string, fallback: boolean): boolean {
    const raw = this.configService.get<string>(envKey);
    if (!raw) {
      return fallback;
    }

    const normalized = raw.trim().toLowerCase();
    if (['true', '1', 'yes', 'on'].includes(normalized)) {
      return true;
    }
    if (['false', '0', 'no', 'off'].includes(normalized)) {
      return false;
    }
    return fallback;
  }

  private isTestnetNetwork(): boolean {
    const raw = this.configService.get<string>('STELLAR_NETWORK');
    if (typeof raw !== 'string' || raw.trim().length === 0) {
      return false;
    }
    return raw.trim().toLowerCase() === 'testnet';
  }

  private buildTestnetOpenPolicy(tier: SubscriptionTier): SubscriptionPolicy {
    return {
      tier,
      features: {
        optimizer: true,
        advancedAnalytics: true,
        apiAccess: true,
      },
      limits: {
        maxProtocols: null,
        maxTrackedChains: null,
        apiMonthlyQuota: null,
      },
    };
  }

  private parseUserApiUsage(value: unknown): UserApiUsage {
    if (!value || typeof value !== 'object') {
      return { period: '', used: 0 };
    }

    const record = value as Record<string, unknown>;
    const period =
      typeof record.period === 'string'
        ? record.period.trim()
        : '';
    const usedRaw = Number(record.used);
    const used = Number.isFinite(usedRaw) && usedRaw >= 0 ? Math.floor(usedRaw) : 0;
    const lastRequestAt = record.lastRequestAt instanceof Date
      ? record.lastRequestAt
      : typeof record.lastRequestAt === 'string'
        ? new Date(record.lastRequestAt)
        : undefined;

    return {
      period,
      used,
      ...(lastRequestAt && !Number.isNaN(lastRequestAt.getTime())
        ? { lastRequestAt }
        : {}),
    };
  }

  private currentUsagePeriod(now = new Date()): string {
    const year = now.getUTCFullYear();
    const month = String(now.getUTCMonth() + 1).padStart(2, '0');
    return `${year}-${month}`;
  }

  private nextUsagePeriodStartIso(now = new Date()): string {
    const year = now.getUTCFullYear();
    const month = now.getUTCMonth();
    const nextMonthStart = new Date(Date.UTC(year, month + 1, 1, 0, 0, 0, 0));
    return nextMonthStart.toISOString();
  }

  private featureEnabled(features: { optimizer: boolean; advancedAnalytics: boolean; apiAccess: boolean }, feature: AccessFeature): boolean {
    switch (feature) {
      case 'optimizer':
        return features.optimizer;
      case 'advancedAnalytics':
        return features.advancedAnalytics;
      case 'apiAccess':
        return features.apiAccess;
      default:
        return false;
    }
  }

  private defaultFeatureMessage(feature: AccessFeature, tier: SubscriptionTier): string {
    switch (feature) {
      case 'optimizer':
        return `Optimizer flow is available on Premium plans. Current plan: ${profileLabel(tier)}.`;
      case 'advancedAnalytics':
        return `Advanced analytics is available on Premium plans. Current plan: ${profileLabel(tier)}.`;
      case 'apiAccess':
        return `API access is available on Premium plans. Current plan: ${profileLabel(tier)}.`;
      default:
        return 'Your current plan does not include this feature.';
    }
  }
}

function profileLabel(tier: SubscriptionTier): string {
  return tier.charAt(0).toUpperCase() + tier.slice(1);
}
