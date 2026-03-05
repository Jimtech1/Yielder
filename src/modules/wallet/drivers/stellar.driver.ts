import { Injectable, Logger } from '@nestjs/common';
import * as StellarSdk from 'stellar-sdk';
import { IWalletDriver, WalletDriverRequestOptions } from '../interfaces/wallet-driver.interface';

type HorizonTarget = {
  url: string;
  server: StellarSdk.Horizon.Server;
};

type AccountLookupOptions = {
  suppressInvalidAddressWarning?: boolean;
  suppressMissingAccountWarning?: boolean;
  suppressNetworkMismatchWarning?: boolean;
};

@Injectable()
export class StellarDriver implements IWalletDriver {
  private logger = new Logger(StellarDriver.name);
  private readonly invalidAddressWarnings = new Set<string>();
  private readonly missingAccountWarnings = new Set<string>();
  private readonly networkMismatchWarnings = new Set<string>();
  private readonly network: string;
  private readonly fallbackNetwork: string | null;
  private readonly primaryTargets: HorizonTarget[];
  private readonly fallbackTargets: HorizonTarget[];

  constructor() {
    const configuredNetwork = (process.env.STELLAR_NETWORK || 'testnet').toLowerCase();
    this.network =
      configuredNetwork === 'mainnet' || configuredNetwork === 'public'
        ? 'mainnet'
        : 'testnet';
    this.fallbackNetwork = this.network === 'mainnet' ? 'testnet' : 'mainnet';

    const testnetUrls = this.resolveConfiguredHorizonUrls('testnet');
    const mainnetUrls = this.resolveConfiguredHorizonUrls('mainnet');
    const primaryUrls = this.network === 'mainnet' ? mainnetUrls : testnetUrls;
    const fallbackUrls = (this.network === 'mainnet' ? testnetUrls : mainnetUrls).filter(
      (url) => !primaryUrls.includes(url),
    );

    this.primaryTargets = this.createHorizonTargets(primaryUrls);
    this.fallbackTargets = this.createHorizonTargets(fallbackUrls);
  }

  async generateWallet() {
    const keypair = StellarSdk.Keypair.random();
    return {
      address: keypair.publicKey(),
      publicKey: keypair.publicKey(),
      secret: keypair.secret(),
    };
  }

  async validateAddress(address: string): Promise<boolean> {
    return StellarSdk.StrKey.isValidEd25519PublicKey(address);
  }

  async getBalance(address: string, options?: WalletDriverRequestOptions) {
    try {
      const account = await this.withAccountNetworkFallback(
        address,
        (server) => server.loadAccount(address),
        this.resolveAccountLookupOptions(options),
      );
      if (!account) {
        return [];
      }

      return account.balances.map((b) => {
        if (b.asset_type === 'native') {
          return { asset: 'XLM', balance: b.balance };
        }

        const code = 'asset_code' in b ? b.asset_code : 'UNKNOWN';
        const issuer = 'asset_issuer' in b ? b.asset_issuer : 'UNKNOWN';
        return {
          asset: `${code}:${issuer}`,
          balance: b.balance,
        };
      });
    } catch (error: unknown) {
      this.logger.error(`Failed to get balance for ${address}: ${this.getErrorMessage(error)}`);
      return [];
    }
  }

  async getTransactions(
    address: string,
    limit = 20,
    options?: WalletDriverRequestOptions,
  ) {
    try {
      const txs = await this.withAccountNetworkFallback(
        address,
        (server) =>
          server.transactions()
            .forAccount(address)
            .order('desc')
            .limit(limit)
            .call(),
        this.resolveAccountLookupOptions(options),
      );
      if (!txs) {
        return [];
      }

      return txs.records;
    } catch (error: unknown) {
      this.logger.error(`Failed to get transactions for ${address}: ${this.getErrorMessage(error)}`);
      return [];
    }
  }

  async verifySignature(message: string, signature: string, address: string): Promise<boolean> {
    try {
        const keypair = StellarSdk.Keypair.fromPublicKey(address);
        return keypair.verify(Buffer.from(message), Buffer.from(signature, 'base64'));
    } catch (error) {
        this.logger.error(`Signature verification failed: ${error.message}`);
        return false;
    }
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
    if (typeof withMessage.message === 'string') {
      return withMessage.message.toLowerCase().includes('not found');
    }

    return false;
  }

  private getErrorMessage(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }

    return String(error);
  }

  private async withAccountNetworkFallback<T>(
    address: string,
    fn: (server: StellarSdk.Horizon.Server) => Promise<T>,
    options: AccountLookupOptions = {},
  ): Promise<T | null> {
    if (!(await this.validateAddress(address))) {
      if (!options.suppressInvalidAddressWarning) {
        this.logInvalidAddressWarning(address);
      }
      return null;
    }

    const primaryResult = await this.tryAccountRequestAcrossTargets(this.primaryTargets, fn);
    if (primaryResult) {
      return primaryResult;
    }

    if (!this.fallbackNetwork || this.fallbackTargets.length === 0) {
      if (!options.suppressMissingAccountWarning) {
        this.logMissingAccountWarning(address);
      }
      return null;
    }

    const fallbackResult = await this.tryAccountRequestAcrossTargets(this.fallbackTargets, fn);
    if (fallbackResult) {
      if (!options.suppressNetworkMismatchWarning) {
        this.logNetworkMismatchWarning(address);
      }
      return fallbackResult;
    }

    if (!options.suppressMissingAccountWarning) {
      this.logMissingAccountWarning(address);
    }
    return null;
  }

  private resolveAccountLookupOptions(
    options?: WalletDriverRequestOptions,
  ): AccountLookupOptions {
    if (!options?.suppressWarnings) {
      return {};
    }

    return {
      suppressInvalidAddressWarning: true,
      suppressMissingAccountWarning: true,
      suppressNetworkMismatchWarning: true,
    };
  }

  private async tryAccountRequestAcrossTargets<T>(
    targets: HorizonTarget[],
    fn: (server: StellarSdk.Horizon.Server) => Promise<T>,
  ): Promise<T | null> {
    let sawNotFound = false;
    let lastError: unknown = null;

    for (const target of targets) {
      try {
        return await fn(target.server);
      } catch (error: unknown) {
        if (this.isAccountNotFoundError(error)) {
          sawNotFound = true;
          continue;
        }

        lastError = error;
        this.logger.debug(
          `Horizon request failed on ${target.url}: ${this.getErrorMessage(error)}`,
        );
      }
    }

    if (sawNotFound) {
      return null;
    }

    if (lastError) {
      throw lastError;
    }

    return null;
  }

  private logNetworkMismatchWarning(address: string): void {
    if (!this.fallbackNetwork || this.fallbackTargets.length === 0) {
      return;
    }

    const key = `${address}:${this.fallbackNetwork}`;
    if (this.networkMismatchWarnings.has(key)) {
      return;
    }

    this.networkMismatchWarnings.add(key);
    this.logger.warn(
      `Stellar account ${address} not found on ${this.network} (${this.describeTargets(this.primaryTargets)}) but exists on ${this.fallbackNetwork} (${this.describeTargets(this.fallbackTargets)}); update STELLAR_NETWORK and STELLAR_HORIZON_URL(S) for consistent indexing`,
    );
  }

  private logMissingAccountWarning(address: string): void {
    if (this.missingAccountWarnings.has(address)) {
      return;
    }

    this.missingAccountWarnings.add(address);
    this.logger.warn(
      `Stellar account ${address} not found on ${this.network} (${this.describeTargets(this.primaryTargets)}); likely unfunded or wrong network, skipping indexing for now`,
    );
  }

  private logInvalidAddressWarning(address: string): void {
    if (this.invalidAddressWarnings.has(address)) {
      return;
    }

    this.invalidAddressWarnings.add(address);
    this.logger.warn(
      `Skipping Stellar requests for invalid public key: ${address}; wallet record should be corrected or archived`,
    );
  }

  private resolveConfiguredHorizonUrls(network: 'testnet' | 'mainnet'): string[] {
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
