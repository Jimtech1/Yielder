import {
  BadRequestException,
  Injectable,
  ServiceUnavailableException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { AccessControlService } from '../access/access-control.service';
import { AuthenticatedUser } from '../auth/interfaces/authenticated-user.interface';
import {
  CreateBillingPortalDto,
  CreateCheckoutSessionDto,
} from './dto/create-checkout-session.dto';

type BillingPlan = 'premium' | 'enterprise';

@Injectable()
export class BillingService {
  constructor(
    private readonly configService: ConfigService,
    private readonly accessControlService: AccessControlService,
  ) {}

  createCheckoutSession(
    user: AuthenticatedUser,
    dto: CreateCheckoutSessionDto,
  ) {
    const requestedPlan: BillingPlan = dto.plan ?? 'premium';
    const currentTier = this.accessControlService.normalizeTier(user.subscriptionTier);
    const targetTier = requestedPlan === 'enterprise' ? 'enterprise' : 'premium';
    const currentRank = this.getTierRank(currentTier);
    const targetRank = this.getTierRank(targetTier);

    if (targetRank <= currentRank) {
      throw new BadRequestException(
        `Your current plan (${currentTier}) already includes ${targetTier} access.`,
      );
    }

    const checkoutBaseUrl = this.resolveCheckoutBaseUrl(requestedPlan);
    const checkoutUrl = this.buildSignedRedirectUrl(checkoutBaseUrl, {
      uid: user.userId,
      email: user.email,
      current_tier: currentTier,
      target_plan: targetTier,
      success_url: this.sanitizeOptionalUrl(dto.successUrl),
      cancel_url: this.sanitizeOptionalUrl(dto.cancelUrl),
    });

    return {
      provider: 'external-link',
      action: 'checkout',
      currentTier,
      targetPlan: targetTier,
      checkoutUrl,
    };
  }

  createPortalSession(user: AuthenticatedUser, dto: CreateBillingPortalDto) {
    const currentTier = this.accessControlService.normalizeTier(user.subscriptionTier);
    const portalBaseUrl = this.configService.get<string>('BILLING_PORTAL_URL') || '';
    const normalizedPortalBaseUrl = portalBaseUrl.trim();
    if (!normalizedPortalBaseUrl) {
      throw new ServiceUnavailableException(
        'Billing portal is not configured. Contact support to manage your subscription.',
      );
    }

    const portalUrl = this.buildSignedRedirectUrl(normalizedPortalBaseUrl, {
      uid: user.userId,
      email: user.email,
      current_tier: currentTier,
      return_url:
        this.sanitizeOptionalUrl(dto.returnUrl) ||
        this.sanitizeOptionalUrl(
          this.configService.get<string>('BILLING_DEFAULT_RETURN_URL') || '',
        ),
    });

    return {
      provider: 'external-link',
      action: 'portal',
      currentTier,
      portalUrl,
    };
  }

  private resolveCheckoutBaseUrl(plan: BillingPlan): string {
    const exactUrl =
      plan === 'enterprise'
        ? this.configService.get<string>('BILLING_ENTERPRISE_CHECKOUT_URL')
        : this.configService.get<string>('BILLING_PREMIUM_CHECKOUT_URL');
    const fallbackUrl = this.configService.get<string>('BILLING_CHECKOUT_URL');
    const baseUrl = (exactUrl || fallbackUrl || '').trim();

    if (!baseUrl) {
      throw new ServiceUnavailableException(
        `Billing checkout URL is not configured for ${plan}.`,
      );
    }

    return baseUrl;
  }

  private sanitizeOptionalUrl(value?: string): string | null {
    if (typeof value !== 'string') {
      return null;
    }

    const normalized = value.trim();
    if (!normalized || normalized.length > 2048) {
      return null;
    }

    try {
      const parsed = new URL(normalized);
      if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
        return null;
      }
      return parsed.toString();
    } catch {
      return null;
    }
  }

  private buildSignedRedirectUrl(
    baseUrl: string,
    params: Record<string, string | null | undefined>,
  ): string {
    let targetUrl: URL;
    try {
      targetUrl = new URL(baseUrl);
    } catch {
      throw new ServiceUnavailableException(
        'Billing URL is invalid. Verify billing environment configuration.',
      );
    }

    for (const [key, value] of Object.entries(params)) {
      if (!value) {
        continue;
      }
      targetUrl.searchParams.set(key, value);
    }

    return targetUrl.toString();
  }

  private getTierRank(tier: 'free' | 'premium' | 'enterprise'): number {
    if (tier === 'enterprise') {
      return 2;
    }
    if (tier === 'premium') {
      return 1;
    }
    return 0;
  }
}
