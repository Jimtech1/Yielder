import { Injectable } from '@nestjs/common';
import { PriceFeedService } from './price-feed.service';

@Injectable()
export class ValuationService {
  private readonly stellarNetwork = (process.env.STELLAR_NETWORK || 'testnet').trim().toLowerCase();
  private readonly usdPeggedSymbols = new Set(['USDC', 'USDT', 'DAI', 'USD', 'USDS', 'FUSD']);

  constructor(private priceFeedService: PriceFeedService) {}

  async calculatePortfolioValue(wallets: any[]): Promise<number> {
    const pricesByAsset = await this.buildPriceMap(wallets);
    let total = 0;

    for (const wallet of wallets) {
      if (!wallet.balances) continue;
      for (const balance of wallet.balances) {
        const amount = this.toAmount(balance.amount);
        if (amount <= 0) {
          continue;
        }

        const price = this.resolvePrice(balance.asset, pricesByAsset);
        total += amount * price;
      }
    }

    return total;
  }

  async getAssetBreakdown(wallets: any[]) {
    const pricesByAsset = await this.buildPriceMap(wallets);
    const breakdown = new Map<string, { amount: number; value: number }>();

    for (const wallet of wallets) {
      if (!wallet.balances) continue;
      for (const balance of wallet.balances) {
        const amount = this.toAmount(balance.amount);
        if (amount <= 0) {
          continue;
        }

        const price = this.resolvePrice(balance.asset, pricesByAsset);
        const value = amount * price;

        if (breakdown.has(balance.asset)) {
          const existing = breakdown.get(balance.asset)!;
          breakdown.set(balance.asset, {
            amount: existing.amount + amount,
            value: existing.value + value,
          });
        } else {
          breakdown.set(balance.asset, { amount, value });
        }
      }
    }

    return Array.from(breakdown.entries()).map(([asset, data]) => ({
      asset,
      ...data,
    }));
  }

  private async buildPriceMap(wallets: any[]): Promise<Map<string, number>> {
    const assetsForPricing = new Set<string>();

    for (const wallet of wallets) {
      const balances = Array.isArray(wallet?.balances) ? wallet.balances : [];
      for (const balance of balances) {
        const normalizedAsset = this.normalizeAssetForPricing(balance?.asset);
        if (normalizedAsset) {
          assetsForPricing.add(normalizedAsset);
        }
      }
    }

    if (assetsForPricing.size === 0) {
      return new Map<string, number>();
    }

    return this.priceFeedService.getPrices(Array.from(assetsForPricing));
  }

  private resolvePrice(rawAsset: unknown, pricesByAsset: Map<string, number>): number {
    const normalizedAsset = this.normalizeAssetForPricing(rawAsset);
    if (!normalizedAsset) {
      return 0;
    }

    const marketPrice = Number(pricesByAsset.get(normalizedAsset) || 0);
    if (Number.isFinite(marketPrice) && marketPrice > 0) {
      return marketPrice;
    }

    return this.resolveNominalFallbackPrice(normalizedAsset);
  }

  private normalizeAssetForPricing(rawAsset: unknown): string {
    if (typeof rawAsset !== 'string') {
      return '';
    }

    const normalized = rawAsset.trim();
    if (!normalized) {
      return '';
    }

    if (normalized.toLowerCase() === 'native') {
      return 'XLM';
    }

    const [firstToken] = normalized.split(':');
    const [symbol] = firstToken.split('-');
    return (symbol || '').trim().toUpperCase();
  }

  private resolveNominalFallbackPrice(normalizedAsset: string): number {
    if (this.stellarNetwork !== 'testnet') {
      return 0;
    }

    // Friendbot/testnet UX: value balances even when oracle feeds are unavailable.
    if (normalizedAsset === 'XLM' || this.usdPeggedSymbols.has(normalizedAsset)) {
      return 1;
    }

    return 0;
  }

  private toAmount(rawAmount: unknown): number {
    const amount = Number.parseFloat(String(rawAmount ?? '0'));
    return Number.isFinite(amount) ? amount : 0;
  }
}
