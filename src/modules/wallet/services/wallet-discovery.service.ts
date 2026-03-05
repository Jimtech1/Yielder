import { Injectable, Logger } from '@nestjs/common';
import * as StellarSdk from 'stellar-sdk';

type NormalizedBalance = {
  asset: string;
  assetType: string;
  amount: string;
  lockedAmount?: string;
};

type HorizonTarget = {
  url: string;
  server: StellarSdk.Horizon.Server;
};

type StellarNetwork = 'testnet' | 'mainnet';

type HorizonAccountRecord = {
  sequence: string;
  balances: unknown[];
};

@Injectable()
export class WalletDiscoveryService {
  private readonly logger = new Logger(WalletDiscoveryService.name);
  private readonly missingAccountWarnings = new Set<string>();
  private readonly networkMismatchWarnings = new Set<string>();
  private readonly network: StellarNetwork;
  private readonly fallbackNetwork: StellarNetwork | null;
  private readonly primaryTargets: HorizonTarget[];
  private readonly fallbackTargets: HorizonTarget[];

  constructor() {
    const configuredNetwork = (process.env.STELLAR_NETWORK || 'testnet').toLowerCase();
    this.network =
      configuredNetwork === 'mainnet' || configuredNetwork === 'public'
        ? 'mainnet'
        : 'testnet';
    this.fallbackNetwork = this.network === 'mainnet' ? 'testnet' : 'mainnet';

    const primaryUrls = this.resolveConfiguredHorizonUrls(this.network);
    const fallbackUrls = this.resolveConfiguredHorizonUrls(this.fallbackNetwork).filter(
      (url) => !primaryUrls.includes(url),
    );

    this.primaryTargets = this.createHorizonTargets(primaryUrls);
    this.fallbackTargets = this.createHorizonTargets(fallbackUrls);
  }

  async discoverAccountInfo(publicKey: string) {
    const primaryAccount = await this.tryLoadAccountAcrossTargets(publicKey, this.primaryTargets);
    if (primaryAccount) {
      return {
        exists: true,
        balances: this.normalizeBalances(this.extractBalances(primaryAccount)),
        sequence: primaryAccount.sequence,
      };
    }

    if (this.fallbackNetwork && this.fallbackTargets.length > 0) {
      const fallbackAccount = await this.tryLoadAccountAcrossTargets(publicKey, this.fallbackTargets);
      if (fallbackAccount) {
        this.logNetworkMismatchWarning(publicKey);
        return {
          exists: true,
          balances: this.normalizeBalances(this.extractBalances(fallbackAccount)),
          sequence: fallbackAccount.sequence,
        };
      }
    }

    this.logMissingAccountWarning(publicKey);
    return { exists: false };
  }

  private normalizeBalances(balances: any[]): NormalizedBalance[] {
    return balances.map((balance) => {
      const assetType = (balance.asset_type || 'unknown').toString();
      const amount = (balance.balance || '0').toString();
      const lockedAmount =
        balance.selling_liabilities && balance.selling_liabilities !== '0'
          ? balance.selling_liabilities.toString()
          : undefined;

      if (assetType === 'native') {
        return {
          asset: 'XLM',
          assetType,
          amount,
          ...(lockedAmount ? { lockedAmount } : {}),
        };
      }

      if (assetType.startsWith('credit_')) {
        const code = balance.asset_code || 'UNKNOWN';
        const issuer = balance.asset_issuer || 'UNKNOWN';
        return {
          asset: `${code}:${issuer}`,
          assetType: 'credit',
          amount,
          ...(lockedAmount ? { lockedAmount } : {}),
        };
      }

      if (assetType === 'liquidity_pool_shares') {
        return {
          asset: `LP:${balance.liquidity_pool_id || 'UNKNOWN'}`,
          assetType: 'liquidity_pool',
          amount,
          ...(lockedAmount ? { lockedAmount } : {}),
        };
      }

      return {
        asset: assetType,
        assetType,
        amount,
        ...(lockedAmount ? { lockedAmount } : {}),
      };
    });
  }

  private extractBalances(account: unknown): any[] {
    const candidate = account as { balances?: unknown };
    return Array.isArray(candidate.balances) ? (candidate.balances as any[]) : [];
  }

  private async tryLoadAccountAcrossTargets(
    publicKey: string,
    targets: HorizonTarget[],
  ): Promise<HorizonAccountRecord | null> {
    let sawNotFound = false;
    let lastError: unknown = null;

    for (const target of targets) {
      try {
        return await target.server.loadAccount(publicKey);
      } catch (error: unknown) {
        if (this.isAccountNotFoundError(error)) {
          sawNotFound = true;
          continue;
        }

        lastError = error;
        this.logger.debug(
          `Horizon discovery request failed on ${target.url}: ${this.getErrorMessage(error)}`,
        );
      }
    }

    if (sawNotFound) {
      return null;
    }

    if (lastError) {
      this.logger.error(`Failed to discover account info: ${this.getErrorMessage(lastError)}`);
      throw lastError;
    }

    return null;
  }

  private isAccountNotFoundError(error: unknown): boolean {
    if (!error || typeof error !== 'object') {
      return false;
    }

    const withResponse = error as { response?: { status?: number } };
    if (withResponse.response?.status === 404) {
      return true;
    }

    const withMessage = error as { message?: unknown };
    return typeof withMessage.message === 'string'
      ? withMessage.message.toLowerCase().includes('not found')
      : false;
  }

  private getErrorMessage(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }
    return String(error);
  }

  private logNetworkMismatchWarning(publicKey: string): void {
    if (!this.fallbackNetwork || this.fallbackTargets.length === 0) {
      return;
    }

    const key = `${publicKey}:${this.fallbackNetwork}`;
    if (this.networkMismatchWarnings.has(key)) {
      return;
    }

    this.networkMismatchWarnings.add(key);
    this.logger.warn(
      `Stellar account ${publicKey} not found on ${this.network} (${this.describeTargets(this.primaryTargets)}) but exists on ${this.fallbackNetwork} (${this.describeTargets(this.fallbackTargets)}); update STELLAR_NETWORK and STELLAR_HORIZON_URL(S) for consistent indexing`,
    );
  }

  private logMissingAccountWarning(publicKey: string): void {
    if (this.missingAccountWarnings.has(publicKey)) {
      return;
    }

    this.missingAccountWarnings.add(publicKey);
    this.logger.warn(
      `Stellar account ${publicKey} not found on ${this.network} (${this.describeTargets(this.primaryTargets)}); likely unfunded or wrong network`,
    );
  }

  private resolveConfiguredHorizonUrls(network: StellarNetwork): string[] {
    const legacyHorizonUrls = this.resolveUrlList(process.env.STELLAR_HORIZON_URLS, []);
    const isConfiguredNetwork = this.network === network;

    const defaultUrl =
      network === 'mainnet'
        ? process.env.STELLAR_HORIZON_URL_MAINNET ||
          (isConfiguredNetwork ? process.env.STELLAR_HORIZON_URL : undefined) ||
          'https://horizon.stellar.org'
        : process.env.STELLAR_HORIZON_URL_TESTNET ||
          (isConfiguredNetwork ? process.env.STELLAR_HORIZON_URL : undefined) ||
          'https://horizon-testnet.stellar.org';
    const envSpecificList =
      network === 'mainnet'
        ? process.env.STELLAR_HORIZON_URLS_MAINNET
        : process.env.STELLAR_HORIZON_URLS_TESTNET;
    const fallbackList =
      legacyHorizonUrls.length > 0 && isConfiguredNetwork
        ? legacyHorizonUrls
        : [defaultUrl];

    return this.resolveUrlList(envSpecificList, fallbackList);
  }

  private resolveUrlList(rawValue: string | undefined, fallback: string[]): string[] {
    const parsed = (rawValue || '')
      .split(',')
      .map((value) => value.trim())
      .filter((value) => value.length > 0);
    const finalList = parsed.length > 0 ? parsed : fallback;
    return [...new Set(finalList)];
  }

  private createHorizonTargets(urls: string[]): HorizonTarget[] {
    return urls.map((url) => ({ url, server: new StellarSdk.Horizon.Server(url) }));
  }

  private describeTargets(targets: HorizonTarget[]): string {
    if (targets.length === 0) {
      return 'no configured Horizon endpoints';
    }
    return targets.map((target) => target.url).join(', ');
  }
}
