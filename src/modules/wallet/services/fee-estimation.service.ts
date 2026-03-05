import { Injectable, Logger } from '@nestjs/common';
import * as StellarSdk from 'stellar-sdk';

@Injectable()
export class FeeEstimationService {
  private readonly logger = new Logger(FeeEstimationService.name);
  private readonly server: StellarSdk.Horizon.Server;

  constructor() {
    const horizonUrl = process.env.STELLAR_HORIZON_URL || 'https://horizon-testnet.stellar.org';
    this.server = new StellarSdk.Horizon.Server(horizonUrl);
  }

  async getStellarFeeStats(): Promise<{ min: number; mode: number; max: number }> {
    try {
      const payload = await this.server.feeStats();
      const chargedStats = payload?.fee_charged;
      const min = this.toPositiveInt(chargedStats?.min, 100);
      const mode = this.toPositiveInt(chargedStats?.mode, min);
      const p95 = this.toPositiveInt(chargedStats?.p95, mode);
      const max = Math.max(p95, this.toPositiveInt(chargedStats?.max, p95), mode, min);

      return { min, mode, max };
    } catch (error: unknown) {
      this.logger.warn(
        `Failed to fetch live Stellar fee stats, using defaults. ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      return {
        min: 100,
        mode: 1000,
        max: 10000,
      };
    }
  }

  async estimateFee(chain: string, operationType: string): Promise<string> {
    if (chain === 'stellar') {
      const stats = await this.getStellarFeeStats();
      const normalizedOperation = (operationType || '').trim().toLowerCase();
      const multiplier =
        normalizedOperation === 'path_payment_strict_send'
          ? 1.4
          : normalizedOperation === 'claim_claimable_balance'
          ? 1.2
          : 1;
      const estimated = Math.round(stats.mode * multiplier);
      return Math.min(Math.max(estimated, stats.min), stats.max).toString();
    }
    return '0';
  }

  private toPositiveInt(value: unknown, fallback: number): number {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return fallback;
    }
    return Math.floor(parsed);
  }
}
