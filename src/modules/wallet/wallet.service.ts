import {
  Injectable,
  BadRequestException,
  Logger,
  Inject,
  forwardRef,
} from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import * as StellarSdk from 'stellar-sdk';
import { StellarDriver } from './drivers/stellar.driver';
import { EvmDriver } from './drivers/evm.driver';
import { AddWatchWalletDto } from './dto/add-watch-wallet.dto';
import { BuildClaimBalanceDto } from './dto/build-claim-balance.dto';
import { BuildWithdrawalDto } from './dto/build-withdrawal.dto';
import { RecordActivityTransactionDto } from './dto/record-activity-transaction.dto';
import { IWalletDriver } from './interfaces/wallet-driver.interface';
import { Transaction, TransactionDocument } from './schemas/transaction.schema';
import { Wallet, WalletDocument } from './schemas/wallet.schema';
import { WalletDiscoveryService } from './services/wallet-discovery.service';
import { FeeEstimationService } from './services/fee-estimation.service';
import { DeFiService } from '../defi/defi.service';
import { PlatformFeeAction } from '../platform-fee/schemas/platform-fee-record.schema';
import { PlatformFeeService } from '../platform-fee/platform-fee.service';
import { AccessControlService } from '../access/access-control.service';

type ClaimableBalanceRecord = {
  id: string;
  asset: string;
  amount: string;
  sponsor?: string;
  last_modified_time?: string;
  last_modified_ledger: number;
  claimants: Array<{
    destination: string;
    predicate: unknown;
  }>;
};

type StellarNetworkName = 'testnet' | 'mainnet';

@Injectable()
export class WalletService {
  private readonly logger = new Logger(WalletService.name);
  private readonly server: StellarSdk.Horizon.Server;
  private readonly walletBalanceRefreshTtlMs = this.resolveWalletBalanceRefreshTtlMs(
    process.env.WALLET_BALANCE_REFRESH_TTL_MS,
  );

  constructor(
    @InjectModel(Wallet.name) private walletModel: Model<WalletDocument>,
    @InjectModel(Transaction.name) private transactionModel: Model<TransactionDocument>,
    private stellarDriver: StellarDriver,
    private evmDriver: EvmDriver,
    private discoveryService: WalletDiscoveryService,
    private feeEstimationService: FeeEstimationService,
    private platformFeeService: PlatformFeeService,
    private accessControlService: AccessControlService,
    @Inject(forwardRef(() => DeFiService))
    private defiService: DeFiService,
  ) {
    const horizonUrl = this.resolveHorizonUrl();
    this.server = new StellarSdk.Horizon.Server(horizonUrl);
  }

  /**
   * Connects a user-owned wallet (authenticated).
   * Ensures the wallet exists in our records.
   */
  async connectWallet(userId: string, publicKey: string, chain = 'stellar', label?: string) {
    const normalizedChain = this.normalizeWalletChain(chain);
    const driver = this.getDriver(normalizedChain);
    if (!(await driver.validateAddress(publicKey))) {
      throw new BadRequestException('Invalid address');
    }

    let wallet = await this.walletModel.findOne({
      userId,
      chain: normalizedChain,
      address: publicKey,
    } as any);

    let balances: any[] = [];
    if (normalizedChain === 'stellar') {
      const info = await this.discoveryService.discoverAccountInfo(publicKey);
      balances = info.exists ? (info.balances ?? []) : [];
    }

    if (!wallet) {
      await this.ensureTrackedChainLimit(userId, normalizedChain);
      wallet = new this.walletModel({
        userId,
        chain: normalizedChain,
        address: publicKey,
        publicKey,
        label: label || 'My Wallet',
        // Wallets added via /wallet/connect are user-connected wallets, not watch-only entries.
        isWatchOnly: false,
        balances,
      });
      await wallet.save();
      return wallet;
    }

    wallet.publicKey = publicKey;
    if (label?.trim()) {
      wallet.label = label.trim();
    }
    // Reconnecting a wallet should always mark it as connected (non-watch-only).
    wallet.isWatchOnly = false;
    if (normalizedChain === 'stellar') {
      wallet.balances = balances;
    }
    await wallet.save();

    return wallet;
  }

  /**
   * Adds a watch-only wallet to the user's profile.
   */
  async addWatchWallet(userId: string, dto: AddWatchWalletDto) {
    const normalizedChain = this.normalizeWalletChain(dto.chain);
    const driver = this.getDriver(normalizedChain);
    if (!(await driver.validateAddress(dto.address))) {
      throw new BadRequestException('Invalid address');
    }

    const existing = await this.walletModel.findOne({
      userId,
      chain: normalizedChain,
      address: dto.address,
    } as any);
    if (existing) throw new BadRequestException('Wallet already added');
    await this.ensureTrackedChainLimit(userId, normalizedChain);

    let balances: any[] = [];
    if (normalizedChain === 'stellar') {
      const info = await this.discoveryService.discoverAccountInfo(dto.address);
      balances = info.exists ? (info.balances ?? []) : [];
    }

    const wallet = new this.walletModel({
      userId,
      chain: normalizedChain,
      address: dto.address,
      publicKey: dto.address, // For Stellar, address is pubKey
      label: dto.label || 'Watch Wallet',
      isWatchOnly: true,
      balances,
    });

    return wallet.save();
  }

  async getWallets(userId: string) {
    const wallets = await this.walletModel.find({ userId, isArchived: false } as any).exec();
    return this.refreshStellarWalletBalances(wallets);
  }

  async getWallet(userId: string, walletId: string) {
    const wallet = await this.walletModel.findOne({ _id: walletId, userId } as any).exec();
    if (!wallet) throw new BadRequestException('Wallet not found');
    await this.refreshStellarWalletBalances([wallet]);
    return wallet;
  }

  async deleteWallet(userId: string, walletId: string) {
    // Soft delete or hard delete? Schema has isArchived.
    // Let's use soft delete.
    await this.walletModel.updateOne({ _id: walletId, userId } as any, { isArchived: true }).exec();
    return { message: 'Wallet archived' };
  }

  async recordActivityTransaction(
    userId: string,
    dto: RecordActivityTransactionDto,
  ): Promise<{
    id: string;
    walletId: string;
    hash: string;
    chain: string;
    type: string;
    status: 'pending' | 'completed' | 'failed';
    timestamp: Date;
  }> {
    const normalizedChain = this.normalizeWalletChain(dto.chain || 'stellar');
    const normalizedAddress = dto.address.trim();
    const driver = this.getDriver(normalizedChain);

    if (!(await driver.validateAddress(normalizedAddress))) {
      throw new BadRequestException('Invalid address');
    }

    const wallet = await this.resolveOrCreateWalletForActivity(
      userId,
      normalizedChain,
      normalizedAddress,
    );
    const normalizedType = dto.type.trim().toLowerCase();
    const normalizedStatus = this.normalizeRecordedActivityStatus(dto.status);
    const hash = this.resolveRecordedActivityHash(
      dto.txHash,
      dto.referenceId,
      normalizedChain,
      normalizedType,
    );
    const from =
      this.normalizeOptionalString(dto.from) ||
      (normalizedType.includes('withdraw') ? normalizedAddress : undefined);
    const to =
      this.normalizeOptionalString(dto.to) ||
      (normalizedType.includes('deposit') ? normalizedAddress : undefined);
    const metadata =
      dto.metadata && typeof dto.metadata === 'object' && !Array.isArray(dto.metadata)
        ? dto.metadata
        : {};
    const timestamp = new Date();

    const record = await this.transactionModel
      .findOneAndUpdate(
        { chain: normalizedChain, hash } as any,
        {
          $set: {
            walletId: wallet._id,
            hash,
            chain: normalizedChain,
            type: normalizedType,
            from,
            to,
            amount: dto.amount,
            asset: dto.asset.trim(),
            fee: this.normalizeOptionalString(dto.fee) || '0',
            status: normalizedStatus,
            timestamp,
            metadata,
          },
        },
        { upsert: true, new: true },
      )
      .exec();

    if (!record) {
      throw new BadRequestException('Unable to record activity transaction');
    }

    return {
      id: String((record as any)._id),
      walletId: String(wallet._id),
      hash,
      chain: normalizedChain,
      type: normalizedType,
      status: normalizedStatus,
      timestamp,
    };
  }

  async getClaimableBalances(address: string, limit?: number, asset?: string) {
    if (!(await this.stellarDriver.validateAddress(address))) {
      throw new BadRequestException('Invalid Stellar address');
    }

    const safeLimit = Math.min(Math.max(limit ?? 20, 1), 50);
    const records = await this.fetchClaimableBalanceRecords(address, safeLimit, asset);
    return {
      address,
      count: records.length,
      claimableBalances: records.map((record) => ({
        id: record.id,
        asset: record.asset,
        amount: record.amount,
        sponsor: record.sponsor,
        lastModifiedTime: record.last_modified_time,
        lastModifiedLedger: record.last_modified_ledger,
        claimants: record.claimants.map((claimant: { destination: string; predicate: unknown }) => ({
          destination: claimant.destination,
          predicate: claimant.predicate,
        })),
      })),
    };
  }

  async buildClaimBalanceTransaction(
    userId: string,
    sourcePublicKey: string,
    balanceId: string,
    memo?: string,
    options?: BuildClaimBalanceDto,
  ): Promise<{
    xdr: string;
    sourcePublicKey: string;
    balanceId: string;
    network: 'testnet' | 'mainnet';
    fee: string;
    platformFee?: {
      action: PlatformFeeAction;
      feeBps: number;
      amount: string;
      asset: string;
      assetSymbol: string;
      collectorAddress: string;
      feeRecordId?: string;
    };
    optimizerFeeSettlement?: Record<string, unknown>;
  }> {
    if (!(await this.stellarDriver.validateAddress(sourcePublicKey))) {
      throw new BadRequestException('Invalid source Stellar address');
    }

    if (!balanceId || balanceId.length < 32) {
      throw new BadRequestException('Invalid claimable balance ID');
    }

    const claimableRecord = await this.getClaimableBalanceById(balanceId);
    const claimAsset = claimableRecord?.asset || 'native';
    const claimAmount = claimableRecord?.amount || '0';

    const { account, network } = await this.loadAccountWithNetworkFallback(sourcePublicKey);
    const fee = await this.feeEstimationService.estimateFee(
      'stellar',
      'claim_claimable_balance',
    );

    const builder = new StellarSdk.TransactionBuilder(account, {
      fee,
      networkPassphrase: this.getNetworkPassphrase(network),
    });

    builder.addOperation(
      StellarSdk.Operation.claimClaimableBalance({
        balanceId,
      }),
    );

    let platformFeeQuote = this.platformFeeService.buildQuote({
      action: 'claim_rewards',
      chainKey: 'stellar',
      payerAddress: sourcePublicKey,
      asset: claimAsset,
      assetSymbol: this.extractAssetSymbol(claimAsset),
      amount: claimAmount,
      decimals: 7,
    });

    if (platformFeeQuote) {
      const claimStellarAsset = this.parseAsset(claimAsset);
      const collectorCanReceive = await this.canStellarAccountReceiveAsset(
        platformFeeQuote.collectorAddress,
        claimStellarAsset,
        network,
      );
      if (!collectorCanReceive) {
        this.logger.warn(
          `Skipping claim_rewards platform fee for ${balanceId}: collector ${platformFeeQuote.collectorAddress} cannot receive ${claimAsset}.`,
        );
        platformFeeQuote = null;
      } else {
        builder.addOperation(
          StellarSdk.Operation.payment({
            destination: platformFeeQuote.collectorAddress,
            asset: claimStellarAsset,
            amount: platformFeeQuote.feeAmount,
          }),
        );
      }
    }

    if (memo) {
      builder.addMemo(StellarSdk.Memo.text(memo.slice(0, 28)));
    }

    builder.setTimeout(180);
    const transaction = builder.build();
    const platformFeeReference = platformFeeQuote
      ? this.platformFeeService.buildReference([
          'claim_rewards',
          userId,
          sourcePublicKey,
          balanceId,
          transaction.toXDR(),
        ])
      : undefined;
    const platformFeeRecord = platformFeeQuote
      ? await this.platformFeeService.accrueFee({
          userId,
          quote: platformFeeQuote,
          reference: platformFeeReference,
          metadata: {
            balanceId,
          },
        })
      : null;

    const optimizerFeeSettlement = await this.maybeSettleOptimizerFeeOnRealization(userId, {
      optimizerExecutionId: options?.optimizerExecutionId,
      autoSettleOptimizerFee: options?.autoSettleOptimizerFee,
      realizedProfitAmount: options?.realizedProfitAmount || claimableRecord?.amount,
      performanceFeeBps: options?.performanceFeeBps,
      settleFeeOnBackend: options?.settleFeeOnBackend,
      payerAddress: sourcePublicKey,
      assetSymbol: this.extractAssetSymbol(claimableRecord?.asset || 'native'),
    });

    return {
      xdr: transaction.toXDR(),
      sourcePublicKey,
      balanceId,
      network,
      fee,
      ...(platformFeeQuote
        ? {
            platformFee: {
              action: platformFeeQuote.action,
              feeBps: platformFeeQuote.feeBps,
              amount: platformFeeQuote.feeAmount,
              asset: platformFeeQuote.asset,
              assetSymbol: platformFeeQuote.assetSymbol,
              collectorAddress: platformFeeQuote.collectorAddress,
              ...(platformFeeRecord ? { feeRecordId: platformFeeRecord.id } : {}),
            },
          }
        : {}),
      ...(optimizerFeeSettlement ? { optimizerFeeSettlement } : {}),
    };
  }

  async buildWithdrawalTransaction(userId: string, dto: BuildWithdrawalDto): Promise<{
    transactionXdr: string;
    sourcePublicKey: string;
    destinationPublicKey: string;
    amount: string;
    asset: string;
    network: 'testnet' | 'mainnet';
    fee: string;
    platformFee?: {
      action: PlatformFeeAction;
      feeBps: number;
      amount: string;
      asset: string;
      assetSymbol: string;
      collectorAddress: string;
      feeRecordId?: string;
    };
    optimizerFeeSettlement?: Record<string, unknown>;
  }> {
    if (!(await this.stellarDriver.validateAddress(dto.sourcePublicKey))) {
      throw new BadRequestException('Invalid source Stellar address');
    }

    if (!(await this.stellarDriver.validateAddress(dto.destinationPublicKey))) {
      throw new BadRequestException('Invalid destination Stellar address');
    }

    if (Number(dto.amount) <= 0) {
      throw new BadRequestException('Amount must be greater than zero');
    }

    const asset = this.parseAsset(dto.asset);
    const { account, network } = await this.loadAccountWithNetworkFallback(dto.sourcePublicKey);
    const fee = await this.feeEstimationService.estimateFee('stellar', 'payment');

    const builder = new StellarSdk.TransactionBuilder(account, {
      fee,
      networkPassphrase: this.getNetworkPassphrase(network),
    });

    builder.addOperation(
      StellarSdk.Operation.payment({
        destination: dto.destinationPublicKey,
        asset,
        amount: dto.amount,
      }),
    );

    const platformFeeQuote = this.platformFeeService.buildQuote({
      action: 'withdrawal',
      chainKey: 'stellar',
      payerAddress: dto.sourcePublicKey,
      asset: dto.asset,
      assetSymbol: this.extractAssetSymbol(dto.asset),
      amount: dto.amount,
      decimals: 7,
    });

    if (platformFeeQuote) {
      builder.addOperation(
        StellarSdk.Operation.payment({
          destination: platformFeeQuote.collectorAddress,
          asset,
          amount: platformFeeQuote.feeAmount,
        }),
      );
    }

    if (dto.memo) {
      builder.addMemo(StellarSdk.Memo.text(dto.memo.slice(0, 28)));
    }

    builder.setTimeout(180);
    const transaction = builder.build();
    const platformFeeReference = platformFeeQuote
      ? this.platformFeeService.buildReference([
          'withdrawal',
          userId,
          dto.sourcePublicKey,
          dto.destinationPublicKey,
          dto.amount,
          dto.asset,
          transaction.toXDR(),
        ])
      : undefined;
    const platformFeeRecord = platformFeeQuote
      ? await this.platformFeeService.accrueFee({
          userId,
          quote: platformFeeQuote,
          reference: platformFeeReference,
          metadata: {
            destinationPublicKey: dto.destinationPublicKey,
            memo: dto.memo || null,
          },
        })
      : null;

    const optimizerFeeSettlement = await this.maybeSettleOptimizerFeeOnRealization(userId, {
      optimizerExecutionId: dto.optimizerExecutionId,
      autoSettleOptimizerFee: dto.autoSettleOptimizerFee,
      realizedProfitAmount: dto.realizedProfitAmount || dto.amount,
      performanceFeeBps: dto.performanceFeeBps,
      settleFeeOnBackend: dto.settleFeeOnBackend,
      payerAddress: dto.sourcePublicKey,
      assetSymbol: this.extractAssetSymbol(dto.asset),
    });

    return {
      transactionXdr: transaction.toXDR(),
      sourcePublicKey: dto.sourcePublicKey,
      destinationPublicKey: dto.destinationPublicKey,
      amount: dto.amount,
      asset: dto.asset,
      network,
      fee,
      ...(platformFeeQuote
        ? {
            platformFee: {
              action: platformFeeQuote.action,
              feeBps: platformFeeQuote.feeBps,
              amount: platformFeeQuote.feeAmount,
              asset: platformFeeQuote.asset,
              assetSymbol: platformFeeQuote.assetSymbol,
              collectorAddress: platformFeeQuote.collectorAddress,
              ...(platformFeeRecord ? { feeRecordId: platformFeeRecord.id } : {}),
            },
          }
        : {}),
      ...(optimizerFeeSettlement ? { optimizerFeeSettlement } : {}),
    };
  }

  // --- Legacy / Helper methods ---

  private getDriver(chain: string): IWalletDriver {
    const normalized = this.normalizeWalletChain(chain);
    if (normalized === 'stellar') {
      return this.stellarDriver;
    }
    return this.evmDriver;
  }

  private normalizeWalletChain(chain: string): string {
    const normalized = (chain || '').trim().toLowerCase();
    if (!normalized) {
      return 'stellar';
    }

    if (normalized === 'stellar') {
      return 'stellar';
    }

    if (
      normalized === 'evm' ||
      normalized === 'ethereum' ||
      normalized === 'sepolia' ||
      normalized === 'polygon' ||
      normalized === 'arbitrum' ||
      normalized === 'base' ||
      normalized === 'axelar'
    ) {
      return normalized;
    }

    throw new BadRequestException(`Unsupported chain: ${chain}`);
  }

  private async ensureTrackedChainLimit(userId: string, candidateChain: string): Promise<void> {
    const profile = await this.accessControlService.getUserAccessProfile(userId);
    const maxTrackedChains = profile.limits.maxTrackedChains;
    if (maxTrackedChains === null) {
      return;
    }

    const trackedChains = (
      (await this.walletModel.distinct('chain', {
        userId,
        isArchived: false,
      } as any)) as string[]
    )
      .map((chain) => this.safeNormalizeTrackedChain(chain))
      .filter((chain, index, values) => values.indexOf(chain) === index);

    this.accessControlService.assertDistinctLimit({
      profile,
      currentValues: trackedChains,
      candidateValue: candidateChain,
      limit: maxTrackedChains,
      resourceLabel: maxTrackedChains === 1 ? 'tracked chain' : 'tracked chains',
    });
  }

  private parseAsset(asset: string): StellarSdk.Asset {
    const normalized = asset.trim();

    if (!normalized || normalized.toUpperCase() === 'XLM' || normalized.toLowerCase() === 'native') {
      return StellarSdk.Asset.native();
    }

    const [code, issuer] = normalized.split(':');
    if (!code || !issuer) {
      throw new BadRequestException(
        `Invalid asset format "${asset}". Use "XLM" or "CODE:ISSUER".`,
      );
    }

    if (!StellarSdk.StrKey.isValidEd25519PublicKey(issuer)) {
      throw new BadRequestException(`Invalid issuer in asset "${asset}"`);
    }

    return new StellarSdk.Asset(code, issuer);
  }

  private matchesStellarBalanceAsset(
    balance: Record<string, unknown>,
    asset: StellarSdk.Asset,
  ): boolean {
    const assetType =
      typeof balance.asset_type === 'string' ? balance.asset_type.trim().toLowerCase() : '';

    if (asset.isNative()) {
      return assetType === 'native';
    }

    const balanceCode =
      typeof balance.asset_code === 'string' ? balance.asset_code.trim().toUpperCase() : '';
    const balanceIssuer =
      typeof balance.asset_issuer === 'string' ? balance.asset_issuer.trim() : '';

    return (
      (assetType === 'credit_alphanum4' || assetType === 'credit_alphanum12') &&
      balanceCode === asset.getCode().trim().toUpperCase() &&
      balanceIssuer === asset.getIssuer().trim()
    );
  }

  private async canStellarAccountReceiveAsset(
    accountId: string,
    asset: StellarSdk.Asset,
    network: StellarNetworkName,
  ): Promise<boolean> {
    if (asset.isNative()) {
      return true;
    }
    if (!StellarSdk.StrKey.isValidEd25519PublicKey(accountId)) {
      return false;
    }

    try {
      const server =
        network === this.resolveStellarNetwork()
          ? this.server
          : new StellarSdk.Horizon.Server(this.resolveHorizonUrl(network));
      const account = await server.loadAccount(accountId);
      const balances = Array.isArray((account as any).balances)
        ? ((account as any).balances as Array<Record<string, unknown>>)
        : [];
      return balances.some((balance) => this.matchesStellarBalanceAsset(balance, asset));
    } catch (error: unknown) {
      this.logger.warn(
        `Could not verify collector trustline for ${accountId}: ${this.errorMessage(error)}`,
      );
      return false;
    }
  }

  private shouldSettleOptimizerFee(options: {
    optimizerExecutionId?: string;
    autoSettleOptimizerFee?: boolean;
  }): boolean {
    if (!options.optimizerExecutionId?.trim()) {
      return false;
    }
    if (options.autoSettleOptimizerFee === false) {
      return false;
    }
    return true;
  }

  private async maybeSettleOptimizerFeeOnRealization(
    userId: string,
    params: {
      optimizerExecutionId?: string;
      autoSettleOptimizerFee?: boolean;
      realizedProfitAmount?: string;
      performanceFeeBps?: number;
      settleFeeOnBackend?: boolean;
      payerAddress: string;
      assetSymbol: string;
    },
  ): Promise<Record<string, unknown> | undefined> {
    if (!this.shouldSettleOptimizerFee(params)) {
      return undefined;
    }

    const realizedProfitAmount = params.realizedProfitAmount?.trim();
    if (!realizedProfitAmount || Number(realizedProfitAmount) <= 0) {
      this.logger.warn(
        'Optimizer fee settlement skipped: realizedProfitAmount is required and must be > 0',
      );
      return undefined;
    }

    try {
      return (await this.defiService.settleOptimizerFee(userId, {
        optimizerExecutionId: params.optimizerExecutionId as string,
        realizedProfitAmount,
        ...(typeof params.performanceFeeBps === 'number'
          ? { performanceFeeBps: params.performanceFeeBps }
          : {}),
        assetSymbol: params.assetSymbol,
        chainKey: 'stellar',
        payerAddress: params.payerAddress,
        ...(typeof params.settleFeeOnBackend === 'boolean'
          ? { settleOnBackend: params.settleFeeOnBackend }
          : {}),
      })) as unknown as Record<string, unknown>;
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : 'unknown error';
      this.logger.warn(`Optimizer fee settlement failed: ${message}`);
      return undefined;
    }
  }

  private async getClaimableBalanceById(
    balanceId: string,
  ): Promise<{ asset: string; amount: string } | null> {
    try {
      const response = (await (this.server.claimableBalances() as any)
        .claimableBalance(balanceId)
        .call()) as Record<string, unknown>;
      const asset = typeof response.asset === 'string' ? response.asset : 'native';
      const amount = typeof response.amount === 'string' ? response.amount : '0';
      return { asset, amount };
    } catch (error: unknown) {
      this.logger.debug(
        `Unable to fetch claimable balance details for ${balanceId}: ${
          error instanceof Error ? error.message : 'unknown error'
        }`,
      );
      return null;
    }
  }

  private async fetchClaimableBalanceRecords(
    address: string,
    safeLimit: number,
    asset?: string,
  ): Promise<ClaimableBalanceRecord[]> {
    let query = this.server.claimableBalances().claimant(address).limit(safeLimit).order('desc');

    if (asset) {
      query = query.asset(this.parseAsset(asset));
    }

    let lastError: unknown = null;
    for (let attempt = 0; attempt <= 1; attempt += 1) {
      try {
        const response = (await query.call()) as { records?: ClaimableBalanceRecord[] };
        return Array.isArray(response.records) ? response.records : [];
      } catch (error: unknown) {
        lastError = error;
        if (attempt >= 1 || !this.isRetryableHorizonError(error)) {
          break;
        }
        await this.sleep(250 * (attempt + 1));
      }
    }

    this.logger.warn(
      `Claimable balances lookup failed for ${address}. Returning empty result. ${
        this.errorMessage(lastError)
      }`,
    );
    return [];
  }

  private isRetryableHorizonError(error: unknown): boolean {
    const status = this.extractHorizonStatusCode(error);
    if (status === 408 || status === 425 || status === 429) {
      return true;
    }
    if (typeof status === 'number' && status >= 500) {
      return true;
    }

    const code = this.extractErrorCode(error);
    return (
      code === 'ECONNABORTED' ||
      code === 'ECONNRESET' ||
      code === 'EAI_AGAIN' ||
      code === 'ENOTFOUND' ||
      code === 'ETIMEDOUT'
    );
  }

  private extractHorizonStatusCode(error: unknown): number | undefined {
    if (!error || typeof error !== 'object') {
      return undefined;
    }

    const raw = error as Record<string, unknown>;
    const response =
      raw.response && typeof raw.response === 'object'
        ? (raw.response as Record<string, unknown>)
        : null;
    const candidates = [
      response?.status,
      response?.statusCode,
      raw.status,
      raw.statusCode,
    ];

    for (const candidate of candidates) {
      if (typeof candidate === 'number' && Number.isFinite(candidate)) {
        return candidate;
      }
    }
    return undefined;
  }

  private extractErrorCode(error: unknown): string | undefined {
    if (!error || typeof error !== 'object') {
      return undefined;
    }
    const rawCode = (error as Record<string, unknown>).code;
    return typeof rawCode === 'string' ? rawCode : undefined;
  }

  private errorMessage(error: unknown): string {
    if (error instanceof Error && error.message) {
      return error.message;
    }
    if (typeof error === 'string' && error.trim()) {
      return error.trim();
    }
    return 'unknown error';
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private extractAssetSymbol(asset: string): string {
    const normalized = asset.trim();
    if (!normalized || normalized.toLowerCase() === 'native') {
      return 'XLM';
    }

    const [firstToken] = normalized.split(':');
    const [symbol] = firstToken.split('-');
    return (symbol || 'XLM').trim().toUpperCase();
  }

  private getNetworkPassphrase(network: StellarNetworkName = this.resolveStellarNetwork()): string {
    return network === 'mainnet' ? StellarSdk.Networks.PUBLIC : StellarSdk.Networks.TESTNET;
  }

  private resolveStellarNetwork(rawNetwork?: string): StellarNetworkName {
    const normalized = (rawNetwork || process.env.STELLAR_NETWORK || 'testnet').toLowerCase();
    return normalized === 'mainnet' || normalized === 'public' ? 'mainnet' : 'testnet';
  }

  private resolveFallbackStellarNetwork(network: StellarNetworkName): StellarNetworkName {
    return network === 'mainnet' ? 'testnet' : 'mainnet';
  }

  private resolveHorizonUrl(network: StellarNetworkName = this.resolveStellarNetwork()): string {
    const configuredNetwork = this.resolveStellarNetwork();
    if (network === configuredNetwork) {
      const explicitUrl = process.env.STELLAR_HORIZON_URL?.trim();
      if (explicitUrl) {
        return explicitUrl;
      }
    }

    if (network === 'mainnet') {
      return (
        process.env.STELLAR_HORIZON_URL_MAINNET?.trim() || 'https://horizon.stellar.org'
      );
    }

    return (
      process.env.STELLAR_HORIZON_URL_TESTNET?.trim() || 'https://horizon-testnet.stellar.org'
    );
  }

  private async loadAccountWithNetworkFallback(
    address: string,
  ): Promise<{
    account: StellarSdk.Horizon.AccountResponse;
    network: StellarNetworkName;
  }> {
    const primaryNetwork = this.resolveStellarNetwork();
    try {
      const account = await this.server.loadAccount(address);
      return { account, network: primaryNetwork };
    } catch (primaryError: unknown) {
      if (!this.isAccountNotFoundError(primaryError)) {
        throw primaryError;
      }

      const fallbackNetwork = this.resolveFallbackStellarNetwork(primaryNetwork);
      const fallbackServer = new StellarSdk.Horizon.Server(this.resolveHorizonUrl(fallbackNetwork));
      try {
        const account = await fallbackServer.loadAccount(address);
        this.logger.warn(
          `Stellar account ${address} was not found on configured ${primaryNetwork}, using ${fallbackNetwork} for transaction build`,
        );
        return { account, network: fallbackNetwork };
      } catch (fallbackError: unknown) {
        if (this.isAccountNotFoundError(fallbackError)) {
          throw new BadRequestException(
            `Stellar account ${address} was not found on ${primaryNetwork} or ${fallbackNetwork}`,
          );
        }
        throw fallbackError;
      }
    }
  }

  private isAccountNotFoundError(error: unknown): boolean {
    const status = this.extractHorizonStatusCode(error);
    if (status === 404) {
      return true;
    }

    if (!error || typeof error !== 'object') {
      return false;
    }

    const message = (error as { message?: unknown }).message;
    return typeof message === 'string' && message.toLowerCase().includes('not found');
  }

  private async refreshStellarWalletBalances(wallets: WalletDocument[]): Promise<WalletDocument[]> {
    if (!Array.isArray(wallets) || wallets.length === 0) {
      return wallets;
    }

    await Promise.all(
      wallets.map(async (wallet) => {
        const chain = (wallet.chain || '').trim().toLowerCase();
        if (chain !== 'stellar' || !wallet.address) {
          return;
        }

        if (!this.shouldRefreshStellarWallet(wallet)) {
          return;
        }

        try {
          const info = await this.discoveryService.discoverAccountInfo(wallet.address);
          if (!info.exists) {
            return;
          }

          const nextBalances = Array.isArray(info.balances) ? info.balances : [];
          if (!this.balancesDiffer(wallet.balances, nextBalances)) {
            return;
          }

          wallet.balances = nextBalances as any;
          await wallet.save();
        } catch (error: unknown) {
          const message = error instanceof Error ? error.message : String(error);
          this.logger.warn(
            `Failed to refresh Stellar balances for ${wallet.address}: ${message}`,
          );
        }
      }),
    );

    return wallets;
  }

  private shouldRefreshStellarWallet(wallet: WalletDocument): boolean {
    const withUpdatedAt = wallet as WalletDocument & { updatedAt?: Date };
    const lastUpdatedAt =
      withUpdatedAt.updatedAt instanceof Date ? withUpdatedAt.updatedAt.getTime() : 0;
    if (lastUpdatedAt <= 0) {
      return true;
    }

    return Date.now() - lastUpdatedAt >= this.walletBalanceRefreshTtlMs;
  }

  private balancesDiffer(existing: unknown, next: unknown): boolean {
    const existingSerialized = JSON.stringify(Array.isArray(existing) ? existing : []);
    const nextSerialized = JSON.stringify(Array.isArray(next) ? next : []);
    return existingSerialized !== nextSerialized;
  }

  private resolveWalletBalanceRefreshTtlMs(rawValue?: string): number {
    const parsed = Number.parseInt((rawValue || '').trim(), 10);
    if (!Number.isFinite(parsed)) {
      return 15_000;
    }

    return Math.min(Math.max(parsed, 2_000), 300_000);
  }

  private safeNormalizeTrackedChain(chain: string): string {
    const normalized = (chain || '').trim().toLowerCase();
    if (!normalized) {
      return 'stellar';
    }
    return normalized;
  }

  private async resolveOrCreateWalletForActivity(
    userId: string,
    chain: string,
    address: string,
  ): Promise<WalletDocument> {
    const existingWallet = await this.walletModel.findOne({ userId, chain, address } as any).exec();
    if (existingWallet) {
      if (existingWallet.isArchived) {
        existingWallet.isArchived = false;
        await existingWallet.save();
      }
      return existingWallet;
    }

    await this.ensureTrackedChainLimit(userId, chain);

    const createdWallet = new this.walletModel({
      userId,
      chain,
      address,
      publicKey: address,
      label: 'Activity Wallet',
      isWatchOnly: true,
      balances: [],
    });
    await createdWallet.save();
    return createdWallet;
  }

  private normalizeRecordedActivityStatus(status?: string): 'pending' | 'completed' | 'failed' {
    const normalized = (status || '').trim().toLowerCase();
    if (normalized === 'failed' || normalized === 'error' || normalized === 'rejected') {
      return 'failed';
    }
    if (normalized === 'completed' || normalized === 'confirmed' || normalized === 'success') {
      return 'completed';
    }
    return 'pending';
  }

  private resolveRecordedActivityHash(
    txHash: string | undefined,
    referenceId: string | undefined,
    chain: string,
    type: string,
  ): string {
    const normalizedHash = this.normalizeOptionalString(txHash);
    if (normalizedHash) {
      return normalizedHash;
    }

    const normalizedReference = this.normalizeOptionalString(referenceId);
    if (normalizedReference) {
      return `activity_${normalizedReference.replace(/[^a-zA-Z0-9_.:-]/g, '_')}`;
    }

    const nonce = Math.random().toString(36).slice(2, 10);
    return `activity_${chain}_${type}_${Date.now()}_${nonce}`;
  }

  private normalizeOptionalString(value?: string): string {
    if (!value || typeof value !== 'string') {
      return '';
    }
    return value.trim();
  }
}
