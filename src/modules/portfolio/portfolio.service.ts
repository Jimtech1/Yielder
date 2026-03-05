import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { DeFiPosition, DeFiPositionDocument } from '../defi/schemas/defi-position.schema';
import { Transaction, TransactionDocument } from '../wallet/schemas/transaction.schema';
import { Wallet, WalletDocument } from '../wallet/schemas/wallet.schema';
import { PortfolioSnapshot, PortfolioSnapshotDocument } from './schemas/portfolio-snapshot.schema';
import { PriceFeedService } from './price-feed.service';
import { ValuationService } from './valuation.service';
import { WalletService } from '../wallet/wallet.service';

type SupportedChain = 'Stellar' | 'Ethereum' | 'Polygon' | 'Arbitrum';

type PortfolioAsset = {
  id: string;
  name: string;
  symbol: string;
  logo: string;
  balance: number;
  value: number;
  price: number;
  change24h: number;
  chain: SupportedChain;
  apy: number;
};

type RawDeFiPosition = {
  _id: unknown;
  walletId: unknown;
  protocol: string;
  type: string;
  assetId: string;
  principal?: unknown;
  currentValue?: unknown;
  apy?: number;
  status?: string;
};

type PortfolioPosition = {
  id: string;
  protocol: string;
  type: string;
  asset: string;
  balance: number;
  apy: number;
  status: string;
  chain: SupportedChain;
};

type AnalyticsTier = 'basic' | 'advanced';

type PortfolioAnalyticsResponse = {
  periodDays: number;
  generatedAt: string;
  tier: AnalyticsTier;
  overview: {
    portfolioValue: number;
    walletCount: number;
    assetCount: number;
    trackedChainCount: number;
    activeProtocolCount: number;
    activePositionCount: number;
    transactionCount: number;
    averageWalletValue: number;
    largestAsset: {
      symbol: string;
      value: number;
      weightPercent: number;
    } | null;
  };
  performance: {
    startValue: number;
    endValue: number;
    absPnL: number;
    roiPercent: number;
    volatilityPercent: number | null;
    maxDrawdownPercent: number | null;
    bestPeriodReturnPercent: number | null;
    worstPeriodReturnPercent: number | null;
    avgPeriodReturnPercent: number | null;
    positivePeriodRatioPercent: number | null;
    currentDrawdownPercent: number | null;
    valueRange: {
      min: number;
      max: number;
    };
  };
  allocation: {
    byChain: Array<{ chain: SupportedChain; value: number; weightPercent: number }>;
    byAsset: Array<{ asset: string; value: number; weightPercent: number }>;
    byProtocol: Array<{
      protocol: string;
      chainCount: number;
      positionCount: number;
      value: number;
      avgApy: number;
      weightPercent: number;
    }>;
  };
  activity: {
    totalVolume: number;
    totalValueUsd: number;
    averageValueUsd: number;
    onChainCompletedCount: number;
    onChainTotalValueUsd: number;
    onChainAverageValueUsd: number;
    successRatePercent: number;
    successfulCount: number;
    pendingCount: number;
    failedCount: number;
    latestTransactionAt: string | null;
    byType: Array<{ type: string; count: number; valueUsd: number }>;
    byStatus: Array<{ status: string; count: number }>;
    byChain: Array<{ chain: SupportedChain; count: number }>;
  };
  history: Array<{ date: string; value: number }>;
};

@Injectable()
export class PortfolioService {
  private readonly logger = new Logger(PortfolioService.name);
  private readonly optimizerSyntheticPositionEnabled = this.resolveBooleanEnv(
    process.env.OPTIMIZER_ENABLE_SYNTHETIC_POSITION,
  );
  private readonly portfolioSnapshotMinIntervalMs = this.resolvePortfolioSnapshotMinIntervalMs(
    process.env.PORTFOLIO_SNAPSHOT_MIN_INTERVAL_MS,
  );

  constructor(
    @InjectModel(Wallet.name) private walletModel: Model<WalletDocument>,
    @InjectModel(Transaction.name) private transactionModel: Model<TransactionDocument>,
    @InjectModel(PortfolioSnapshot.name) private snapshotModel: Model<PortfolioSnapshotDocument>,
    @InjectModel(DeFiPosition.name) private defiPositionModel: Model<DeFiPositionDocument>,
    private priceFeedService: PriceFeedService,
    private valuationService: ValuationService,
    private walletService: WalletService,
  ) {}

  async getPortfolio(userId: string) {
    const wallets = await this.walletService.getWallets(userId);

    const totalValue = await this.valuationService.calculatePortfolioValue(wallets);
    const breakdown = await this.valuationService.getAssetBreakdown(wallets);
    return {
      totalValue,
      walletCount: wallets.length,
      chains: [...new Set(wallets.map((wallet) => wallet.chain))],
      breakdown,
      wallets: wallets.map((wallet) => ({
        id: String((wallet as any)._id),
        chain: wallet.chain,
        address: wallet.address,
        label: wallet.label,
        balances: wallet.balances,
      })),
    };
  }

  async getSummary(userId: string) {
    const portfolio = await this.getPortfolio(userId);
    return {
      totalValue: portfolio.totalValue,
      totalReturn: 0,
      change24h: 0,
      walletCount: portfolio.walletCount,
      assetCount: portfolio.breakdown.length,
    };
  }

  async getAssets(userId: string): Promise<PortfolioAsset[]> {
    const wallets = await this.walletService.getWallets(userId);
    if (wallets.length === 0) {
      return [];
    }

    const breakdown = await this.valuationService.getAssetBreakdown(wallets);
    const priceByAsset = new Map<string, number>(
      breakdown.map((item: { asset: string; amount: number; value: number }) => {
        const unitPrice = item.amount > 0 ? item.value / item.amount : 0;
        return [item.asset, unitPrice];
      }),
    );

    const assetsByKey = new Map<string, PortfolioAsset>();
    for (const wallet of wallets) {
      const chain = this.normalizeChainLabel(wallet.chain);
      const balances = Array.isArray(wallet.balances) ? wallet.balances : [];

      for (const balance of balances) {
        const rawAsset = typeof balance.asset === 'string' ? balance.asset : '';
        if (!rawAsset) {
          continue;
        }

        const symbol = this.normalizeAssetSymbol(rawAsset);
        const amount = this.toNumber(balance.amount);
        const price = priceByAsset.get(rawAsset) ?? 0;
        const value = amount * price;
        const key = `${symbol}:${chain}`;

        const existing = assetsByKey.get(key);
        if (existing) {
          existing.balance += amount;
          existing.value += value;
          existing.price = existing.balance > 0 ? existing.value / existing.balance : existing.price;
          continue;
        }

        assetsByKey.set(key, {
          id: key,
          name: this.getAssetDisplayName(symbol),
          symbol,
          logo: this.getAssetLogo(symbol),
          balance: amount,
          value,
          price,
          change24h: 0,
          chain,
          apy: 0,
        });
      }
    }

    if (this.optimizerSyntheticPositionEnabled) {
      const walletIds = wallets.map((wallet) => wallet._id);
      const walletChainById = new Map<string, SupportedChain>(
        wallets.map((wallet) => [String(wallet._id), this.normalizeChainLabel(wallet.chain)]),
      );

      const positions = (await this.defiPositionModel
        .find({ walletId: { $in: walletIds }, status: { $ne: 'closed' } } as any)
        .lean()
        .exec()) as RawDeFiPosition[];

      const syntheticStakedByKey = new Map<string, number>();
      for (const position of positions) {
        const chain = walletChainById.get(String(position.walletId));
        if (!chain) {
          continue;
        }

        const symbol = this.extractPositionPrincipalAssetSymbol(position);
        const amount = this.extractNumericValue(position.principal);
        if (!symbol || amount <= 0) {
          continue;
        }

        const key = `${symbol}:${chain}`;
        const existingAmount = syntheticStakedByKey.get(key) || 0;
        syntheticStakedByKey.set(key, existingAmount + amount);
      }

      for (const [key, stakedAmount] of syntheticStakedByKey.entries()) {
        const asset = assetsByKey.get(key);
        if (!asset) {
          continue;
        }

        const availableBalance = Math.max(asset.balance - stakedAmount, 0);
        asset.balance = availableBalance;
        asset.value = availableBalance * asset.price;
      }
    }

    return Array.from(assetsByKey.values()).sort((left, right) => right.value - left.value);
  }

  async getPositions(userId: string): Promise<{ positions: PortfolioPosition[] }> {
    const wallets = await this.walletModel.find({ userId, isArchived: false } as any).select('_id chain').exec();
    if (wallets.length === 0) {
      return { positions: [] };
    }

    const walletIds = wallets.map((wallet) => wallet._id);
    const walletChainById = new Map<string, SupportedChain>(
      wallets.map((wallet) => [String(wallet._id), this.normalizeChainLabel(wallet.chain)]),
    );

    const positions = (await this.defiPositionModel
      .find({ walletId: { $in: walletIds }, status: { $ne: 'closed' } } as any)
      .lean()
      .exec()) as RawDeFiPosition[];

    return {
      positions: positions.map((position) => ({
        id: String(position._id),
        protocol: position.protocol,
        type: position.type,
        asset: this.normalizeAssetSymbol(position.assetId),
        balance: this.extractPositionBalance(position.currentValue, position.principal),
        apy: this.toNumber(position.apy),
        status: position.status ?? 'active',
        chain: walletChainById.get(String(position.walletId)) ?? 'Stellar',
      })),
    };
  }

  async getPortfolioHistory(
    userId: string,
    days = 30,
    options?: { ensureSnapshot?: boolean },
  ) {
    if (options?.ensureSnapshot !== false) {
      await this.ensureRecentSnapshot(userId);
    }

    const safeDays = Number.isFinite(days) && days > 0 ? Math.floor(days) : 30;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - safeDays);

    const snapshots = await this.snapshotModel
      .find({
        userId,
        timestamp: { $gte: startDate },
      } as any)
      .sort({ timestamp: 1 })
      .exec();

    if (snapshots.length === 0) {
      const current = await this.getPortfolio(userId);
      return [
        {
          date: new Date().toISOString(),
          value: current.totalValue,
        },
      ];
    }

    return snapshots.map((snapshot) => ({
      date: snapshot.timestamp.toISOString(),
      value: snapshot.totalValue,
      breakdown: snapshot.breakdown,
    }));
  }

  async captureSnapshot(userId: string) {
    const portfolio = await this.getPortfolio(userId);

    await this.snapshotModel.create({
      userId,
      totalValue: portfolio.totalValue,
      breakdown: portfolio.breakdown,
      timestamp: new Date(),
    } as any);

    return portfolio;
  }

  async getActivityFeed(userId: string, limit = 50) {
    const wallets = await this.walletModel.find({ userId, isArchived: false } as any).select('_id').exec();
    const walletIds = wallets.map((wallet) => wallet._id);

    const records = await this.transactionModel
      .find({ walletId: { $in: walletIds } } as any)
      .sort({ timestamp: -1 })
      .limit(limit)
      .lean()
      .exec();

    const normalizedRecords = records.map((tx) => {
      const metadata =
        tx.metadata && typeof tx.metadata === 'object'
          ? (tx.metadata as Record<string, unknown>)
          : {};
      const chain = this.normalizeChainLabel(tx.chain);
      const fallbackAsset = this.normalizeAssetSymbol(tx.asset || '');
      const fromAssetRaw =
        typeof metadata.fromAsset === 'string' && metadata.fromAsset.trim()
          ? metadata.fromAsset.trim().toUpperCase()
          : fallbackAsset;
      const fromAsset = this.normalizeAssetSymbol(fromAssetRaw);
      const toAsset =
        typeof metadata.toAsset === 'string' && metadata.toAsset.trim()
          ? this.normalizeAssetSymbol(metadata.toAsset.trim().toUpperCase())
          : undefined;
      const txCreatedAt =
        (tx as unknown as { createdAt?: Date | string }).createdAt || undefined;
      const txAmount = this.toNumber(tx.amount);
      const metadataAmount = this.toNumber(metadata.amount);
      const metadataSourceAmount = this.firstPositiveNumber(
        metadata.sourceAmount,
        metadata.source_amount,
        metadata.sendAmount,
        metadata.send_amount,
      );
      const metadataDestinationAmount = this.firstPositiveNumber(
        metadata.destinationAmount,
        metadata.destination_amount,
        metadata.destAmount,
        metadata.dest_amount,
        metadata.receiveAmount,
        metadata.receive_amount,
      );
      const amount = txAmount > 0 ? txAmount : metadataAmount > 0 ? metadataAmount : metadataSourceAmount;
      const sourceAmount = metadataSourceAmount > 0 ? metadataSourceAmount : amount;

      const fee = this.resolveActivityFee(tx.fee, metadata, chain);

      return {
        id: String(tx._id),
        type: this.normalizeActivityType(tx.type),
        fromAsset,
        ...(toAsset ? { toAsset } : {}),
        amount,
        ...(sourceAmount > 0 ? { sourceAmount } : {}),
        ...(metadataDestinationAmount > 0 ? { destinationAmount: metadataDestinationAmount } : {}),
        explicitValueUsd: this.toNumber(metadata.valueUsd),
        feeAmount: fee.amount,
        feeAsset: fee.asset,
        status: this.normalizeActivityStatus(tx.status),
        txHash: tx.hash || '',
        chain,
        timestamp: tx.timestamp || txCreatedAt || new Date(),
      };
    });

    const actionableRecords = normalizedRecords.filter((record) => {
      const destinationAmount = 'destinationAmount' in record ? (record.destinationAmount || 0) : 0;
      return record.amount > 0 || destinationAmount > 0 || record.explicitValueUsd > 0;
    });

    const assetsForPricing = [
      ...new Set(
        actionableRecords
          .flatMap((record) => {
            const assets: string[] = [];
            if (record.explicitValueUsd <= 0) {
              assets.push(record.fromAsset);
              if (record.type === 'swap' && 'toAsset' in record && record.toAsset) {
                assets.push(record.toAsset);
              }
            }
            if (record.feeAmount > 0) {
              assets.push(record.feeAsset);
            }
            return assets;
          })
          .filter((asset) => asset !== 'UNKNOWN' && !this.isUsdPeggedAsset(asset)),
      ),
    ];

    let pricesByAsset = new Map<string, number>();
    if (assetsForPricing.length > 0) {
      try {
        pricesByAsset = await this.priceFeedService.getPrices(assetsForPricing);
      } catch {
        pricesByAsset = new Map<string, number>();
      }
    }

    return actionableRecords.map((record) => {
      const sourceAmount = 'sourceAmount' in record ? (record.sourceAmount || 0) : 0;
      const destinationAmount = 'destinationAmount' in record ? (record.destinationAmount || 0) : 0;
      const toAsset = 'toAsset' in record ? record.toAsset : undefined;

      return {
        id: record.id,
        type: record.type,
        fromAsset: record.fromAsset,
        ...(toAsset ? { toAsset } : {}),
        amount: record.amount,
        ...(sourceAmount > 0 ? { sourceAmount } : {}),
        ...(destinationAmount > 0 ? { destinationAmount } : {}),
        value: this.resolveActivityValueUsd({
          amount: record.amount,
          explicitValueUsd: record.explicitValueUsd,
          asset: record.fromAsset,
          assetPriceUsd: this.toNumber(pricesByAsset.get(record.fromAsset)),
          preferredAmount: record.type === 'swap' ? destinationAmount : 0,
          preferredAsset: record.type === 'swap' ? (toAsset || '') : '',
          preferredAssetPriceUsd:
            record.type === 'swap' && toAsset ? this.toNumber(pricesByAsset.get(toAsset)) : 0,
        }),
        fee: this.resolveActivityFeeUsd({
          amount: record.feeAmount,
          asset: record.feeAsset,
          assetPriceUsd: this.toNumber(pricesByAsset.get(record.feeAsset)),
        }),
        feeAsset: record.feeAsset,
        status: record.status,
        txHash: record.txHash,
        chain: record.chain,
        timestamp: record.timestamp,
      };
    });
  }

  async getAnalytics(userId: string, days = 30): Promise<PortfolioAnalyticsResponse> {
    return this.buildAnalyticsPayload(userId, days, 'basic');
  }

  async getAdvancedAnalytics(userId: string, days = 30): Promise<PortfolioAnalyticsResponse> {
    return this.buildAnalyticsPayload(userId, days, 'advanced');
  }

  private async buildAnalyticsPayload(
    userId: string,
    days: number,
    tier: AnalyticsTier,
  ): Promise<PortfolioAnalyticsResponse> {
    const safeDays = Number.isFinite(days) && days > 0 ? Math.floor(days) : 30;
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - safeDays);
    const includeAdvancedMetrics = tier === 'advanced';
    await this.ensureRecentSnapshot(userId);

    const wallets = await this.walletService.getWallets(userId);
    const walletIds = wallets.map((wallet) => wallet._id);

    const [portfolio, historyRaw, positions, activityRecords, walletValues] = await Promise.all([
      this.getPortfolio(userId),
      this.getPortfolioHistory(userId, safeDays, { ensureSnapshot: false }),
      walletIds.length > 0
        ? this.defiPositionModel
            .find({ walletId: { $in: walletIds }, status: { $ne: 'closed' } } as any)
            .lean()
            .exec()
        : Promise.resolve([] as RawDeFiPosition[]),
      walletIds.length > 0
        ? this.transactionModel
            .find({
              walletId: { $in: walletIds },
              timestamp: { $gte: startDate },
            } as any)
            .lean()
            .exec()
        : Promise.resolve([] as Array<Record<string, unknown>>),
      Promise.all(
        wallets.map(async (wallet) => ({
          chain: this.normalizeChainLabel(String(wallet.chain || 'stellar')),
          value: await this.valuationService.calculatePortfolioValue([wallet]),
        })),
      ),
    ]);

    const walletChainById = new Map<string, SupportedChain>(
      wallets.map((wallet) => [
        String((wallet as unknown as { _id: unknown })._id),
        this.normalizeChainLabel(String(wallet.chain || 'stellar')),
      ]),
    );

    const chainAllocationMap = new Map<SupportedChain, number>();
    for (const walletValue of walletValues) {
      const existing = chainAllocationMap.get(walletValue.chain) || 0;
      chainAllocationMap.set(walletValue.chain, existing + walletValue.value);
    }

    const rawBreakdown = Array.isArray((portfolio as { breakdown?: unknown }).breakdown)
      ? ((portfolio as { breakdown: unknown[] }).breakdown as Array<Record<string, unknown>>)
      : [];
    const assetAllocationMap = new Map<string, number>();
    for (const breakdownItem of rawBreakdown) {
      const asset = this.normalizeAssetSymbol(String(breakdownItem.asset || ''));
      const value = this.toNumber(breakdownItem.value);
      if (!asset || asset === 'UNKNOWN' || value <= 0) {
        continue;
      }
      assetAllocationMap.set(asset, (assetAllocationMap.get(asset) || 0) + value);
    }
    const assetAllocation = Array.from(assetAllocationMap.entries())
      .map(([asset, value]) => ({
        asset,
        value: this.roundTo4(value),
        weightPercent: this.roundTo4(this.safePercent(value, portfolio.totalValue)),
      }))
      .sort((left, right) => right.value - left.value);
    const largestAsset = assetAllocation.length > 0 ? assetAllocation[0] : null;

    const protocolExposureMap = new Map<
      string,
      { protocol: string; chainSet: Set<SupportedChain>; value: number; apyTotal: number; count: number }
    >();
    for (const rawPosition of positions as RawDeFiPosition[]) {
      const protocol = (rawPosition.protocol || 'Unknown').trim() || 'Unknown';
      const chain = walletChainById.get(String(rawPosition.walletId)) || 'Stellar';
      const value = this.extractPositionBalance(rawPosition.currentValue, rawPosition.principal);
      const apy = this.toNumber(rawPosition.apy);
      const existing = protocolExposureMap.get(protocol) || {
        protocol,
        chainSet: new Set<SupportedChain>(),
        value: 0,
        apyTotal: 0,
        count: 0,
      };
      existing.chainSet.add(chain);
      existing.value += value;
      existing.apyTotal += apy;
      existing.count += 1;
      protocolExposureMap.set(protocol, existing);
    }

    const history = historyRaw.map((point) => ({
      date: String((point as { date: string }).date),
      value: this.toNumber((point as { value: number }).value),
    }));
    const startValue = history.length > 0 ? history[0].value : this.toNumber(portfolio.totalValue);
    const endValue =
      history.length > 0 ? history[history.length - 1].value : this.toNumber(portfolio.totalValue);
    const pnl = endValue - startValue;
    const roi = startValue > 0 ? (pnl / startValue) * 100 : 0;
    const dailyReturns = includeAdvancedMetrics
      ? this.computeStepReturns(history.map((point) => point.value))
      : [];
    const historyValues =
      history.length > 0 ? history.map((point) => point.value) : [this.toNumber(portfolio.totalValue)];
    const minValue = historyValues.length > 0 ? Math.min(...historyValues) : 0;
    const maxValue = historyValues.length > 0 ? Math.max(...historyValues) : 0;

    const normalizedActivityRecords = (activityRecords as Array<Record<string, unknown>>).map((record) => {
      const metadata =
        record.metadata && typeof record.metadata === 'object'
          ? (record.metadata as Record<string, unknown>)
          : {};
      const hash = typeof record.hash === 'string' ? record.hash.trim() : '';
      const fallbackAsset = this.normalizeAssetSymbol(String(record.asset || ''));
      const fromAssetRaw =
        typeof metadata.fromAsset === 'string' && metadata.fromAsset.trim().length > 0
          ? metadata.fromAsset.trim()
          : fallbackAsset;
      const fromAsset = this.normalizeAssetSymbol(fromAssetRaw);
      const timestamp = this.parseTimestamp(record.timestamp);

      return {
        type: this.normalizeActivityType(String(record.type || '')),
        status: this.normalizeActivityStatus(String(record.status || '')),
        chain: this.normalizeChainLabel(String(record.chain || 'stellar')),
        amount: this.toNumber(record.amount),
        explicitValueUsd: this.toNumber(metadata.valueUsd),
        fromAsset,
        timestamp,
        isUserInitiated: this.isUserInitiatedActivityRecord({
          hash,
          metadata,
        }),
      };
    });

    const measurableDetectedActivityRecords = normalizedActivityRecords.filter(
      (record) => (record.amount > 0 || record.explicitValueUsd > 0) && record.type !== 'fee',
    );
    const completedDetectedActivityRecords = measurableDetectedActivityRecords.filter(
      (record) => record.status === 'completed',
    );

    const measurableActivityRecords = normalizedActivityRecords.filter(
      (record) =>
        record.isUserInitiated &&
        (record.amount > 0 || record.explicitValueUsd > 0) &&
        record.type !== 'fee',
    );
    const completedActivityRecords = measurableActivityRecords.filter(
      (record) => record.status === 'completed',
    );

    const assetsForPricing = [
      ...new Set(
        completedDetectedActivityRecords
          .filter((record) => record.explicitValueUsd <= 0)
          .map((record) => record.fromAsset)
          .filter((asset) => asset !== 'UNKNOWN' && !this.isUsdPeggedAsset(asset)),
      ),
    ];
    let activityPricesByAsset = new Map<string, number>();
    if (assetsForPricing.length > 0) {
      try {
        activityPricesByAsset = await this.priceFeedService.getPrices(assetsForPricing);
      } catch {
        activityPricesByAsset = new Map<string, number>();
      }
    }

    const activityByType = new Map<string, { count: number; valueUsd: number }>();
    const activityByStatus = new Map<string, number>();
    const activityByChain = new Map<SupportedChain, number>();
    let totalActivityVolume = 0;
    let totalActivityValueUsd = 0;
    let onChainTotalActivityValueUsd = 0;
    let latestTransactionAtMs = 0;
    for (const record of completedActivityRecords) {
      totalActivityVolume += record.amount;
      const valueUsd = this.resolveActivityValueUsd({
        amount: record.amount,
        explicitValueUsd: record.explicitValueUsd,
        asset: record.fromAsset,
        assetPriceUsd: this.toNumber(activityPricesByAsset.get(record.fromAsset)),
      });
      totalActivityValueUsd += valueUsd;

      const activityType = activityByType.get(record.type) || { count: 0, valueUsd: 0 };
      activityType.count += 1;
      activityType.valueUsd += valueUsd;
      activityByType.set(record.type, activityType);

    }

    for (const record of completedDetectedActivityRecords) {
      const valueUsd = this.resolveActivityValueUsd({
        amount: record.amount,
        explicitValueUsd: record.explicitValueUsd,
        asset: record.fromAsset,
        assetPriceUsd: this.toNumber(activityPricesByAsset.get(record.fromAsset)),
      });
      onChainTotalActivityValueUsd += valueUsd;
    }

    for (const record of measurableActivityRecords) {
      activityByStatus.set(record.status, (activityByStatus.get(record.status) || 0) + 1);
      activityByChain.set(record.chain, (activityByChain.get(record.chain) || 0) + 1);

      if (record.timestamp && record.timestamp.getTime() > latestTransactionAtMs) {
        latestTransactionAtMs = record.timestamp.getTime();
      }
    }

    const attemptedTransactionCount = measurableActivityRecords.length;
    const successfulCount = activityByStatus.get('completed') || 0;
    const pendingCount = activityByStatus.get('pending') || 0;
    const failedCount = activityByStatus.get('failed') || 0;
    const transactionCount = completedActivityRecords.length;
    const onChainCompletedCount = completedDetectedActivityRecords.length;
    const averageWalletValue = wallets.length > 0 ? portfolio.totalValue / wallets.length : 0;

    return {
      periodDays: safeDays,
      generatedAt: new Date().toISOString(),
      tier,
      overview: {
        portfolioValue: this.roundTo4(portfolio.totalValue),
        walletCount: wallets.length,
        assetCount: assetAllocation.length,
        trackedChainCount: chainAllocationMap.size,
        activeProtocolCount: protocolExposureMap.size,
        activePositionCount: positions.length,
        transactionCount,
        averageWalletValue: this.roundTo4(averageWalletValue),
        largestAsset: largestAsset
          ? {
              symbol: largestAsset.asset,
              value: this.roundTo4(largestAsset.value),
              weightPercent: this.roundTo4(largestAsset.weightPercent),
            }
          : null,
      },
      performance: {
        startValue: this.roundTo4(startValue),
        endValue: this.roundTo4(endValue),
        absPnL: this.roundTo4(pnl),
        roiPercent: this.roundTo4(roi),
        volatilityPercent: includeAdvancedMetrics
          ? this.roundTo4(this.calculateStdDev(dailyReturns) * 100)
          : null,
        maxDrawdownPercent: includeAdvancedMetrics
          ? this.roundTo4(this.calculateMaxDrawdownPercent(history))
          : null,
        bestPeriodReturnPercent:
          includeAdvancedMetrics && dailyReturns.length > 0
            ? this.roundTo4(Math.max(...dailyReturns) * 100)
            : null,
        worstPeriodReturnPercent:
          includeAdvancedMetrics && dailyReturns.length > 0
            ? this.roundTo4(Math.min(...dailyReturns) * 100)
            : null,
        avgPeriodReturnPercent:
          includeAdvancedMetrics && dailyReturns.length > 0
            ? this.roundTo4(
                (dailyReturns.reduce((sum, value) => sum + value, 0) / dailyReturns.length) * 100,
              )
            : null,
        positivePeriodRatioPercent:
          includeAdvancedMetrics && dailyReturns.length > 0
            ? this.roundTo4(
                this.safePercent(
                  dailyReturns.filter((value) => value > 0).length,
                  dailyReturns.length,
                ),
              )
            : null,
        currentDrawdownPercent: includeAdvancedMetrics
          ? this.roundTo4(this.calculateCurrentDrawdownPercent(history))
          : null,
        valueRange: {
          min: this.roundTo4(minValue),
          max: this.roundTo4(maxValue),
        },
      },
      allocation: {
        byChain: Array.from(chainAllocationMap.entries())
          .map(([chain, value]) => ({
            chain,
            value: this.roundTo4(value),
            weightPercent: this.roundTo4(this.safePercent(value, portfolio.totalValue)),
          }))
          .sort((left, right) => right.value - left.value),
        byAsset: assetAllocation,
        byProtocol: includeAdvancedMetrics
          ? Array.from(protocolExposureMap.values())
              .map((entry) => ({
                protocol: entry.protocol,
                chainCount: entry.chainSet.size,
                positionCount: entry.count,
                value: this.roundTo4(entry.value),
                avgApy: this.roundTo4(entry.count > 0 ? entry.apyTotal / entry.count : 0),
                weightPercent: this.roundTo4(this.safePercent(entry.value, portfolio.totalValue)),
              }))
              .sort((left, right) => right.value - left.value)
          : [],
      },
      activity: {
        totalVolume: this.roundTo4(totalActivityVolume),
        totalValueUsd: this.roundTo4(totalActivityValueUsd),
        averageValueUsd: this.roundTo4(
          transactionCount > 0 ? totalActivityValueUsd / transactionCount : 0,
        ),
        onChainCompletedCount,
        onChainTotalValueUsd: this.roundTo4(onChainTotalActivityValueUsd),
        onChainAverageValueUsd: this.roundTo4(
          onChainCompletedCount > 0 ? onChainTotalActivityValueUsd / onChainCompletedCount : 0,
        ),
        successRatePercent: this.roundTo4(
          this.safePercent(successfulCount, attemptedTransactionCount),
        ),
        successfulCount,
        pendingCount,
        failedCount,
        latestTransactionAt: latestTransactionAtMs > 0 ? new Date(latestTransactionAtMs).toISOString() : null,
        byType: Array.from(activityByType.entries())
          .map(([type, item]) => ({
            type,
            count: item.count,
            valueUsd: this.roundTo4(item.valueUsd),
          }))
          .sort((left, right) => {
            if (right.count === left.count) {
              return right.valueUsd - left.valueUsd;
            }
            return right.count - left.count;
          }),
        byStatus: Array.from(activityByStatus.entries())
          .map(([status, count]) => ({ status, count }))
          .sort((left, right) => right.count - left.count),
        byChain: Array.from(activityByChain.entries())
          .map(([chain, count]) => ({ chain, count }))
          .sort((left, right) => right.count - left.count),
      },
      history,
    };
  }

  private toNumber(value: unknown): number {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  private firstPositiveNumber(...values: unknown[]): number {
    for (const candidate of values) {
      const parsed = this.toNumber(candidate);
      if (parsed > 0) {
        return parsed;
      }
    }

    return 0;
  }

  private roundTo4(value: number): number {
    if (!Number.isFinite(value)) {
      return 0;
    }
    return Number(value.toFixed(4));
  }

  private roundTo8(value: number): number {
    if (!Number.isFinite(value)) {
      return 0;
    }
    return Number(value.toFixed(8));
  }

  private resolveActivityValueUsd(params: {
    amount: number;
    explicitValueUsd: number;
    asset: string;
    assetPriceUsd: number;
    preferredAmount?: number;
    preferredAsset?: string;
    preferredAssetPriceUsd?: number;
  }): number {
    if (params.explicitValueUsd > 0) {
      return this.roundTo4(params.explicitValueUsd);
    }

    if ((params.preferredAmount || 0) > 0) {
      if ((params.preferredAssetPriceUsd || 0) > 0) {
        return this.roundTo4((params.preferredAmount || 0) * (params.preferredAssetPriceUsd || 0));
      }

      if (this.isUsdPeggedAsset(params.preferredAsset || '')) {
        return this.roundTo4(params.preferredAmount || 0);
      }
    }

    if (params.assetPriceUsd > 0) {
      return this.roundTo4(params.amount * params.assetPriceUsd);
    }

    if (this.isUsdPeggedAsset(params.asset)) {
      return this.roundTo4(params.amount);
    }

    return 0;
  }

  private resolveActivityFee(
    rawFee: unknown,
    metadata: Record<string, unknown>,
    chain: SupportedChain,
  ): { amount: number; asset: string } {
    const metadataProvider =
      typeof metadata.provider === 'string' ? metadata.provider.trim().toLowerCase() : '';
    const metadataRail = typeof metadata.rail === 'string' ? metadata.rail.trim().toLowerCase() : '';
    const allowStroopConversion =
      chain === 'Stellar' && metadataProvider !== 'anchor' && metadataRail !== 'fiat';
    const metadataFeeAsset = this.resolveMetadataFeeAsset(metadata);

    const directFee = this.normalizeActivityFeeCandidate(rawFee, allowStroopConversion);
    if (directFee.amount > 0) {
      return {
        amount: this.roundTo8(directFee.amount),
        asset: directFee.convertedFromStroops ? 'XLM' : metadataFeeAsset || 'USD',
      };
    }

    const metadataFee = this.normalizeActivityFeeCandidate(metadata.fee, allowStroopConversion);
    if (metadataFee.amount > 0) {
      return {
        amount: this.roundTo8(metadataFee.amount),
        asset: metadataFee.convertedFromStroops ? 'XLM' : metadataFeeAsset || 'USD',
      };
    }

    const metadataFeeCandidates: Array<{ value: unknown; asset: string }> = [
      { value: metadata.feeAmount, asset: metadataFeeAsset || 'USD' },
      { value: metadata.fee_amount, asset: metadataFeeAsset || 'USD' },
      { value: metadata.anchorFee, asset: metadataFeeAsset || 'USD' },
      { value: metadata.anchorFeeAmount, asset: metadataFeeAsset || 'USD' },
      { value: metadata.feeUsd, asset: 'USD' },
      { value: metadata.fee_usd, asset: 'USD' },
    ];
    for (const candidate of metadataFeeCandidates) {
      const amount = this.toNumber(candidate.value);
      if (amount > 0) {
        return { amount: this.roundTo8(amount), asset: candidate.asset };
      }
    }

    const quote =
      metadata.quote && typeof metadata.quote === 'object' && !Array.isArray(metadata.quote)
        ? (metadata.quote as Record<string, unknown>)
        : null;
    if (quote) {
      const quoteFee = this.extractQuoteFeeAmount(quote);
      if (quoteFee > 0) {
        return { amount: this.roundTo8(quoteFee), asset: metadataFeeAsset || 'USD' };
      }
    }

    return { amount: 0, asset: 'USD' };
  }

  private normalizeActivityFeeCandidate(
    rawCandidate: unknown,
    allowStroopConversion: boolean,
  ): { amount: number; convertedFromStroops: boolean } {
    const amount = this.toNumber(rawCandidate);
    if (amount <= 0) {
      return { amount: 0, convertedFromStroops: false };
    }

    if (allowStroopConversion && this.isLikelyStroopAmount(rawCandidate, amount)) {
      return { amount: amount / 10_000_000, convertedFromStroops: true };
    }

    return { amount, convertedFromStroops: false };
  }

  private resolveActivityFeeUsd(params: { amount: number; asset: string; assetPriceUsd: number }): number {
    if (params.amount <= 0) {
      return 0;
    }

    if (this.isUsdPeggedAsset(params.asset)) {
      return this.roundTo8(params.amount);
    }

    if (params.assetPriceUsd > 0) {
      return this.roundTo8(params.amount * params.assetPriceUsd);
    }

    return this.roundTo8(params.amount);
  }

  private resolveMetadataFeeAsset(metadata: Record<string, unknown>): string {
    const candidates = [
      metadata.feeAsset,
      metadata.fee_asset,
      metadata.feeTokenSymbol,
      metadata.fee_token_symbol,
      metadata.feeCurrency,
      metadata.fee_currency,
    ];
    for (const candidate of candidates) {
      const normalized = this.normalizeOptionalAssetSymbol(candidate);
      if (normalized) {
        return normalized;
      }
    }
    return '';
  }

  private normalizeOptionalAssetSymbol(value: unknown): string {
    if (typeof value !== 'string' || value.trim().length === 0) {
      return '';
    }

    const normalized = this.normalizeAssetSymbol(value.trim());
    return normalized === 'UNKNOWN' ? '' : normalized;
  }

  private isLikelyStroopAmount(rawValue: unknown, amount: number): boolean {
    if (!Number.isFinite(amount) || amount < 100) {
      return false;
    }

    if (typeof rawValue === 'number') {
      return Number.isInteger(rawValue);
    }

    if (typeof rawValue === 'string') {
      const trimmed = rawValue.trim();
      return /^\d+$/.test(trimmed);
    }

    return false;
  }

  private extractQuoteFeeAmount(quote: Record<string, unknown>): number {
    const directCandidates = [quote.fee_total, quote.total_fee, quote.feeAmount, quote.fee_amount];
    for (const candidate of directCandidates) {
      const amount = this.toNumber(candidate);
      if (amount > 0) {
        return amount;
      }
    }

    const rawFee = quote.fee;
    if (typeof rawFee === 'number' || typeof rawFee === 'string') {
      const amount = this.toNumber(rawFee);
      if (amount > 0) {
        return amount;
      }
    }

    if (rawFee && typeof rawFee === 'object' && !Array.isArray(rawFee)) {
      const feeRecord = rawFee as Record<string, unknown>;
      const nestedCandidates = [
        feeRecord.total,
        feeRecord.total_fee,
        feeRecord.amount,
        feeRecord.value,
        feeRecord.fee,
      ];
      for (const candidate of nestedCandidates) {
        const amount = this.toNumber(candidate);
        if (amount > 0) {
          return amount;
        }
      }
    }

    const price =
      quote.price && typeof quote.price === 'object' && !Array.isArray(quote.price)
        ? (quote.price as Record<string, unknown>)
        : null;
    if (price) {
      const priceCandidates = [price.total_fee, price.fee, price.fee_total];
      for (const candidate of priceCandidates) {
        const amount = this.toNumber(candidate);
        if (amount > 0) {
          return amount;
        }
      }
    }

    return 0;
  }

  private isUsdPeggedAsset(asset: string): boolean {
    const normalized = this.normalizeAssetSymbol(asset || '');
    const [symbol] = normalized.split(':');
    return (
      symbol === 'USDC' ||
      symbol === 'USDT' ||
      symbol === 'DAI' ||
      symbol === 'USD' ||
      symbol === 'USDS' ||
      symbol === 'FUSD'
    );
  }

  private safePercent(value: number, total: number): number {
    if (!Number.isFinite(value) || !Number.isFinite(total) || total <= 0) {
      return 0;
    }
    return (value / total) * 100;
  }

  private computeStepReturns(values: number[]): number[] {
    if (values.length < 2) {
      return [];
    }

    const returns: number[] = [];
    for (let index = 1; index < values.length; index += 1) {
      const previous = values[index - 1];
      const current = values[index];
      if (!Number.isFinite(previous) || !Number.isFinite(current) || previous <= 0) {
        continue;
      }
      returns.push((current - previous) / previous);
    }
    return returns;
  }

  private calculateStdDev(values: number[]): number {
    if (values.length < 2) {
      return 0;
    }

    const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
    const variance =
      values.reduce((sum, value) => sum + (value - mean) * (value - mean), 0) / values.length;
    return Math.sqrt(variance);
  }

  private calculateMaxDrawdownPercent(history: Array<{ value: number }>): number {
    if (history.length < 2) {
      return 0;
    }

    let peak = history[0].value;
    let maxDrawdown = 0;

    for (const point of history) {
      if (point.value > peak) {
        peak = point.value;
      }
      if (peak <= 0) {
        continue;
      }
      const drawdown = ((peak - point.value) / peak) * 100;
      if (drawdown > maxDrawdown) {
        maxDrawdown = drawdown;
      }
    }

    return maxDrawdown;
  }

  private calculateCurrentDrawdownPercent(history: Array<{ value: number }>): number {
    if (history.length < 2) {
      return 0;
    }

    let peak = history[0].value;
    for (const point of history) {
      if (point.value > peak) {
        peak = point.value;
      }
    }

    if (peak <= 0) {
      return 0;
    }

    const current = history[history.length - 1].value;
    return Math.max(((peak - current) / peak) * 100, 0);
  }

  private resolveBooleanEnv(value: string | undefined): boolean {
    if (!value) {
      return false;
    }

    const normalized = value.trim().toLowerCase();
    return normalized === 'true' || normalized === '1' || normalized === 'yes' || normalized === 'on';
  }

  private async ensureRecentSnapshot(userId: string): Promise<void> {
    try {
      const latestSnapshot = await this.snapshotModel
        .findOne({ userId } as any)
        .sort({ timestamp: -1 })
        .select('timestamp')
        .exec();

      if (latestSnapshot?.timestamp instanceof Date) {
        const ageMs = Date.now() - latestSnapshot.timestamp.getTime();
        if (ageMs < this.portfolioSnapshotMinIntervalMs) {
          return;
        }
      }

      await this.captureSnapshot(userId);
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.warn(`Failed to capture analytics snapshot for user ${userId}: ${message}`);
    }
  }

  private resolvePortfolioSnapshotMinIntervalMs(rawValue?: string): number {
    const parsed = Number.parseInt((rawValue || '').trim(), 10);
    if (!Number.isFinite(parsed)) {
      return 60_000;
    }

    return Math.min(Math.max(parsed, 5_000), 86_400_000);
  }

  private extractPositionBalance(currentValue: unknown, principal: unknown): number {
    const current = this.extractNumericValue(currentValue);
    if (current > 0) {
      return current;
    }
    return this.extractNumericValue(principal);
  }

  private extractNumericValue(value: unknown): number {
    if (typeof value === 'number') {
      return Number.isFinite(value) ? value : 0;
    }

    if (typeof value === 'string') {
      return this.toNumber(value);
    }

    if (Array.isArray(value)) {
      return value.reduce((total, item) => total + this.extractNumericValue(item), 0);
    }

    if (value && typeof value === 'object') {
      const record = value as Record<string, unknown>;
      if ('amount' in record) {
        return this.extractNumericValue(record.amount);
      }
      if ('value' in record) {
        return this.extractNumericValue(record.value);
      }
      if ('total' in record) {
        return this.extractNumericValue(record.total);
      }
    }

    return 0;
  }

  private extractPositionPrincipalAssetSymbol(position: RawDeFiPosition): string {
    const principal =
      position.principal && typeof position.principal === 'object' && !Array.isArray(position.principal)
        ? (position.principal as Record<string, unknown>)
        : null;

    if (principal && typeof principal.asset === 'string' && principal.asset.trim().length > 0) {
      return this.normalizeAssetSymbol(principal.asset);
    }

    return this.normalizeAssetSymbol(position.assetId || '');
  }

  private normalizeChainLabel(chain: string): SupportedChain {
    const normalized = (chain || 'stellar').trim().toLowerCase();
    switch (normalized) {
      case 'ethereum':
      case 'evm':
      case 'sepolia':
        return 'Ethereum';
      case 'polygon':
        return 'Polygon';
      case 'arbitrum':
        return 'Arbitrum';
      default:
        return 'Stellar';
    }
  }

  private normalizeActivityType(
    type: string,
  ):
    | 'deposit'
    | 'withdrawal'
    | 'swap'
    | 'bridge'
    | 'stake'
    | 'unstake'
    | 'transfer'
    | 'fee'
    | 'claim_rewards' {
    const normalized = (type || '').trim().toLowerCase();
    if (!normalized) {
      return 'deposit';
    }
    if (normalized.includes('claim') || normalized.includes('reward')) {
      return 'claim_rewards';
    }
    if (normalized.includes('bridge')) {
      return 'bridge';
    }
    if (normalized.includes('swap')) {
      return 'swap';
    }
    if (normalized.includes('unstake')) {
      return 'unstake';
    }
    if (normalized.includes('stake') || normalized.includes('defi')) {
      return 'stake';
    }
    if (normalized.includes('withdraw')) {
      return 'withdrawal';
    }
    if (normalized.includes('transfer') || normalized.includes('send')) {
      return 'transfer';
    }
    if (normalized.includes('fee')) {
      return 'fee';
    }
    return 'deposit';
  }

  private normalizeActivityStatus(status: string): 'completed' | 'pending' | 'failed' {
    const normalized = (status || '').trim().toLowerCase();
    if (normalized === 'failed' || normalized === 'error' || normalized === 'rejected') {
      return 'failed';
    }
    if (normalized === 'pending' || normalized === 'submitted' || normalized === 'processing') {
      return 'pending';
    }
    if (normalized === 'confirmed' || normalized === 'completed') {
      return 'completed';
    }
    return 'pending';
  }

  private isUserInitiatedActivityRecord(params: {
    hash: string;
    metadata: Record<string, unknown>;
  }): boolean {
    const normalizedHash = (params.hash || '').trim().toLowerCase();
    if (normalizedHash.startsWith('activity_') || normalizedHash.startsWith('anchor_')) {
      return true;
    }

    const readMetadataString = (key: string): string => {
      const value = params.metadata[key];
      return typeof value === 'string' ? value.trim().toLowerCase() : '';
    };

    const source = readMetadataString('source');
    if (source.length > 0) {
      return true;
    }

    const provider = readMetadataString('provider');
    if (provider === 'anchor') {
      return true;
    }

    const rail = readMetadataString('rail');
    if (rail === 'crypto' || rail === 'fiat') {
      return true;
    }

    const optimizerExecutionId = params.metadata.optimizerExecutionId;
    if (typeof optimizerExecutionId === 'string' && optimizerExecutionId.trim().length > 0) {
      return true;
    }

    const sessionId = params.metadata.sessionId;
    if (typeof sessionId === 'string' && sessionId.trim().length > 0) {
      return true;
    }

    return false;
  }

  private parseTimestamp(value: unknown): Date | null {
    if (value instanceof Date) {
      return Number.isFinite(value.getTime()) ? value : null;
    }

    if (typeof value === 'string' || typeof value === 'number') {
      const parsed = new Date(value);
      return Number.isFinite(parsed.getTime()) ? parsed : null;
    }

    return null;
  }

  private normalizeAssetSymbol(asset: string): string {
    const normalized = asset.trim();
    if (!normalized) {
      return 'UNKNOWN';
    }

    if (normalized.toLowerCase() === 'native') {
      return 'XLM';
    }

    if (normalized.includes(':')) {
      const [code, issuer] = normalized.split(':');
      if (code && issuer && this.looksLikeStellarPublicKey(issuer)) {
        return code.toUpperCase();
      }
      return normalized.toUpperCase();
    }

    return normalized.toUpperCase();
  }

  private looksLikeStellarPublicKey(value: string): boolean {
    return /^G[A-Z0-9]{55}$/.test(value);
  }

  private getAssetDisplayName(symbol: string): string {
    switch (symbol) {
      case 'XLM':
        return 'Stellar Lumens';
      case 'USDC':
        return 'USD Coin';
      case 'ETH':
        return 'Ethereum';
      case 'BTC':
      case 'WBTC':
        return 'Bitcoin';
      case 'MATIC':
        return 'Polygon';
      case 'ARB':
        return 'Arbitrum';
      case 'AQUA':
        return 'Aquarius';
      default:
        return symbol;
    }
  }

  private getAssetLogo(symbol: string): string {
    switch (symbol) {
      case 'XLM':
        return 'xlm';
      case 'USDC':
        return 'usdc';
      case 'ETH':
        return 'eth';
      case 'BTC':
      case 'WBTC':
        return 'btc';
      case 'MATIC':
        return 'matic';
      case 'ARB':
        return 'arb';
      case 'AQUA':
        return 'aqua';
      default:
        return 'asset';
    }
  }
}
