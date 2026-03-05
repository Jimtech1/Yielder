import { BadRequestException, Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model, Types } from 'mongoose';
import * as StellarSdk from 'stellar-sdk';
import { ethers } from 'ethers';
import { createHash } from 'crypto';
import {
  PlatformFeeAction,
  PlatformFeeRecord,
  PlatformFeeRecordDocument,
  PlatformFeeStatus,
} from './schemas/platform-fee-record.schema';

export type PlatformFeeQuote = {
  action: PlatformFeeAction;
  chainKey: string;
  payerAddress: string;
  collectorAddress: string;
  asset: string;
  assetSymbol: string;
  amount: string;
  amountBaseUnits: string;
  feeBps: number;
  feeAmount: string;
  feeBaseUnits: string;
  decimals: number;
};

type BuildPlatformFeeQuoteParams = {
  action: PlatformFeeAction;
  chainKey: string;
  payerAddress: string;
  asset: string;
  assetSymbol: string;
  amount: string;
  decimals: number;
};

type AccruePlatformFeeParams = {
  userId: string;
  quote: PlatformFeeQuote;
  reference?: string;
  metadata?: Record<string, unknown>;
};

type ListPlatformFeeRecordsOptions = {
  limit: number;
  status?: PlatformFeeStatus;
  action?: PlatformFeeAction;
};

type CollectPlatformFeesOptions = {
  feeIds?: string[];
  limit?: number;
  collectionTxHash?: string;
};

const PLATFORM_FEE_ACTIONS: PlatformFeeAction[] = [
  'swap',
  'bridge',
  'claim_rewards',
  'deposit',
  'withdrawal',
];

@Injectable()
export class PlatformFeeService {
  private readonly platformFeeCollectorAddress =
    this.normalizeAddressString(process.env.PLATFORM_FEE_COLLECTOR_ADDRESS) ||
    this.normalizeAddressString(process.env.OPTIMIZER_FEE_COLLECTOR_ADDRESS);
  private readonly platformFeeCollectorAddresses = this.resolveAddressMap(
    process.env.PLATFORM_FEE_COLLECTOR_ADDRESSES ||
      process.env.OPTIMIZER_FEE_COLLECTOR_ADDRESSES,
  );
  private readonly platformFeeDefaultBps = this.resolveFeeBps(
    process.env.PLATFORM_FEE_BPS_DEFAULT,
    10,
  );
  private readonly platformFeeBpsByAction: Record<PlatformFeeAction, number> = {
    swap: this.resolveFeeBps(process.env.PLATFORM_FEE_BPS_SWAP, this.platformFeeDefaultBps),
    bridge: this.resolveFeeBps(process.env.PLATFORM_FEE_BPS_BRIDGE, this.platformFeeDefaultBps),
    claim_rewards: this.resolveFeeBps(
      process.env.PLATFORM_FEE_BPS_CLAIM_REWARDS,
      this.platformFeeDefaultBps,
    ),
    deposit: this.resolveFeeBps(process.env.PLATFORM_FEE_BPS_DEPOSIT, this.platformFeeDefaultBps),
    withdrawal: this.resolveFeeBps(
      process.env.PLATFORM_FEE_BPS_WITHDRAWAL,
      this.platformFeeDefaultBps,
    ),
  };

  constructor(
    @InjectModel(PlatformFeeRecord.name)
    private readonly platformFeeRecordModel: Model<PlatformFeeRecordDocument>,
  ) {}

  buildQuote(params: BuildPlatformFeeQuoteParams): PlatformFeeQuote | null {
    const feeBps = this.platformFeeBpsByAction[params.action] ?? 0;
    if (!Number.isInteger(feeBps) || feeBps <= 0) {
      return null;
    }

    const normalizedChainKey = this.normalizeChainKey(params.chainKey);
    const collectorAddress = this.resolveCollectorAddress(normalizedChainKey);
    if (!collectorAddress) {
      return null;
    }

    const payerAddress = this.normalizePayerAddress(normalizedChainKey, params.payerAddress);
    if (!payerAddress) {
      return null;
    }

    const decimals = Number.isInteger(params.decimals) && params.decimals >= 0 ? params.decimals : 0;
    const amount = this.normalizeDecimal(params.amount);
    if (this.toPositiveNumber(amount) <= 0) {
      return null;
    }

    const amountBaseUnits = this.toBaseUnits(amount, decimals, 'amount');
    if (amountBaseUnits <= 0n) {
      return null;
    }

    const feeBaseUnits = (amountBaseUnits * BigInt(feeBps)) / 10000n;
    if (feeBaseUnits <= 0n) {
      return null;
    }

    return {
      action: params.action,
      chainKey: normalizedChainKey,
      payerAddress,
      collectorAddress,
      asset: params.asset.trim(),
      assetSymbol: params.assetSymbol.trim().toUpperCase(),
      amount,
      amountBaseUnits: amountBaseUnits.toString(),
      feeBps,
      feeAmount: this.fromBaseUnits(feeBaseUnits, decimals),
      feeBaseUnits: feeBaseUnits.toString(),
      decimals,
    };
  }

  async accrueFee(params: AccruePlatformFeeParams) {
    const payload: Omit<
      PlatformFeeRecord,
      'status' | 'reference' | 'metadata' | 'collectedAt' | 'collectedBy' | 'collectionTxHash'
    > &
      Partial<Pick<PlatformFeeRecord, 'reference' | 'metadata'>> = {
      userId: params.userId,
      action: params.quote.action,
      chainKey: params.quote.chainKey,
      payerAddress: params.quote.payerAddress,
      collectorAddress: params.quote.collectorAddress,
      asset: params.quote.asset,
      assetSymbol: params.quote.assetSymbol,
      amount: params.quote.amount,
      feeBps: params.quote.feeBps,
      feeAmount: params.quote.feeAmount,
      ...(params.reference ? { reference: params.reference.trim() } : {}),
      ...(params.metadata ? { metadata: params.metadata } : {}),
    };

    const reference = params.reference?.trim();
    if (reference) {
      const record = await this.platformFeeRecordModel
        .findOneAndUpdate(
          { reference },
          {
            $setOnInsert: {
              ...payload,
              status: 'accrued',
            },
          },
          {
            upsert: true,
            new: true,
            setDefaultsOnInsert: true,
          },
        )
        .lean()
        .exec();

      return this.mapPlatformFeeRecord(record as unknown as Record<string, unknown>);
    }

    const createdRecord = await this.platformFeeRecordModel.create({
      ...payload,
      status: 'accrued',
    });
    return this.mapPlatformFeeRecord(
      createdRecord.toObject() as unknown as Record<string, unknown>,
    );
  }

  async getSummary() {
    const summary = await this.platformFeeRecordModel
      .aggregate([
        {
          $addFields: {
            feeAmountNumber: { $toDouble: '$feeAmount' },
          },
        },
        {
          $group: {
            _id: {
              status: '$status',
              action: '$action',
            },
            count: { $sum: 1 },
            totalFeeAmount: { $sum: '$feeAmountNumber' },
          },
        },
      ])
      .exec();

    const byAction: Record<
      PlatformFeeAction,
      {
        accruedCount: number;
        accruedAmount: number;
        collectedCount: number;
        collectedAmount: number;
      }
    > = {
      swap: { accruedCount: 0, accruedAmount: 0, collectedCount: 0, collectedAmount: 0 },
      bridge: { accruedCount: 0, accruedAmount: 0, collectedCount: 0, collectedAmount: 0 },
      claim_rewards: { accruedCount: 0, accruedAmount: 0, collectedCount: 0, collectedAmount: 0 },
      deposit: { accruedCount: 0, accruedAmount: 0, collectedCount: 0, collectedAmount: 0 },
      withdrawal: { accruedCount: 0, accruedAmount: 0, collectedCount: 0, collectedAmount: 0 },
    };

    let totalAccruedCount = 0;
    let totalCollectedCount = 0;
    let totalAccruedAmount = 0;
    let totalCollectedAmount = 0;

    for (const row of summary as Array<Record<string, unknown>>) {
      const group = row._id as { status?: string; action?: string } | undefined;
      if (!group) {
        continue;
      }

      const action = this.normalizeAction(group.action);
      const status = this.normalizeStatus(group.status);
      if (!action || !status) {
        continue;
      }

      const count = this.toPositiveNumber(row.count);
      const amount = this.toPositiveNumber(row.totalFeeAmount);

      if (status === 'accrued') {
        byAction[action].accruedCount += count;
        byAction[action].accruedAmount = this.roundTo6(byAction[action].accruedAmount + amount);
        totalAccruedCount += count;
        totalAccruedAmount = this.roundTo6(totalAccruedAmount + amount);
      } else {
        byAction[action].collectedCount += count;
        byAction[action].collectedAmount = this.roundTo6(byAction[action].collectedAmount + amount);
        totalCollectedCount += count;
        totalCollectedAmount = this.roundTo6(totalCollectedAmount + amount);
      }
    }

    return {
      totalAccruedCount,
      totalCollectedCount,
      totalAccruedAmount,
      totalCollectedAmount,
      byAction,
      config: {
        defaultBps: this.platformFeeDefaultBps,
        byAction: this.platformFeeBpsByAction,
      },
    };
  }

  async listRecords(options: ListPlatformFeeRecordsOptions) {
    const query: Partial<PlatformFeeRecord> = {};
    if (options.status) {
      query.status = options.status;
    }
    if (options.action) {
      query.action = options.action;
    }

    const limit = Math.max(Math.min(options.limit, 200), 1);
    const records = await this.platformFeeRecordModel
      .find(query as any)
      .sort({ createdAt: -1 })
      .limit(limit)
      .lean()
      .exec();

    return records.map((record) =>
      this.mapPlatformFeeRecord(record as unknown as Record<string, unknown>),
    );
  }

  async collectAccruedFees(ownerUserId: string, options: CollectPlatformFeesOptions) {
    const ids = this.normalizeFeeIds(options.feeIds);
    const query: {
      status: PlatformFeeStatus;
      _id?: { $in: Types.ObjectId[] };
    } = { status: 'accrued' };
    if (ids.length > 0) {
      query._id = { $in: ids };
    }

    const limit = Math.max(Math.min(options.limit ?? 100, 500), 1);
    const records = await this.platformFeeRecordModel
      .find(query as any)
      .sort({ createdAt: 1 })
      .limit(limit)
      .lean()
      .exec();

    if (records.length === 0) {
      return {
        collectedCount: 0,
        collectedFeeAmount: 0,
        records: [],
      };
    }

    const recordIds = records.map((record) => record._id) as Types.ObjectId[];
    const now = new Date();
    const collectionTxHash = this.normalizeAddressString(options.collectionTxHash);

    await this.platformFeeRecordModel
      .updateMany(
        { _id: { $in: recordIds } } as any,
        {
          $set: {
            status: 'collected',
            collectedAt: now,
            collectedBy: ownerUserId,
            ...(collectionTxHash ? { collectionTxHash } : {}),
          },
        },
      )
      .exec();

    const collectedFeeAmount = records.reduce(
      (sum, record) => this.roundTo6(sum + this.toPositiveNumber(record.feeAmount)),
      0,
    );

    return {
      collectedCount: records.length,
      collectedFeeAmount,
      records: records.map((record) =>
        this.mapPlatformFeeRecord({
          ...(record as unknown as Record<string, unknown>),
          status: 'collected',
          collectedAt: now,
          collectedBy: ownerUserId,
          ...(collectionTxHash ? { collectionTxHash } : {}),
        }),
      ),
    };
  }

  buildReference(parts: Array<string | number | null | undefined>): string {
    const content = parts
      .map((part) => (part === null || part === undefined ? '' : String(part).trim()))
      .join('|');
    return createHash('sha256').update(content).digest('hex');
  }

  resolveFeeBpsForAction(action: PlatformFeeAction): number {
    return this.platformFeeBpsByAction[action] ?? 0;
  }

  private mapPlatformFeeRecord(record: Record<string, unknown>) {
    const createdAt =
      record.createdAt instanceof Date
        ? record.createdAt.toISOString()
        : typeof record.createdAt === 'string'
        ? record.createdAt
        : null;
    const updatedAt =
      record.updatedAt instanceof Date
        ? record.updatedAt.toISOString()
        : typeof record.updatedAt === 'string'
        ? record.updatedAt
        : null;
    const collectedAt =
      record.collectedAt instanceof Date
        ? record.collectedAt.toISOString()
        : typeof record.collectedAt === 'string'
        ? record.collectedAt
        : null;

    return {
      id: String(record._id || ''),
      userId: String(record.userId || ''),
      action: String(record.action || ''),
      chainKey: String(record.chainKey || ''),
      payerAddress: String(record.payerAddress || ''),
      collectorAddress: String(record.collectorAddress || ''),
      asset: String(record.asset || ''),
      assetSymbol: String(record.assetSymbol || ''),
      amount: String(record.amount || '0'),
      feeBps: this.toPositiveNumber(record.feeBps),
      feeAmount: String(record.feeAmount || '0'),
      status: String(record.status || 'accrued'),
      reference: record.reference ? String(record.reference) : null,
      collectedAt,
      collectedBy: record.collectedBy ? String(record.collectedBy) : null,
      collectionTxHash: record.collectionTxHash ? String(record.collectionTxHash) : null,
      metadata: record.metadata && typeof record.metadata === 'object' ? record.metadata : {},
      createdAt,
      updatedAt,
    };
  }

  private normalizeChainKey(value: string): string {
    return value.trim().toLowerCase();
  }

  private resolveCollectorAddress(chainKey: string): string | null {
    const configured =
      this.platformFeeCollectorAddresses[chainKey] ||
      this.platformFeeCollectorAddress ||
      null;
    if (!configured) {
      return null;
    }

    if (chainKey === 'stellar') {
      const normalized = configured.trim();
      return StellarSdk.StrKey.isValidEd25519PublicKey(normalized) ? normalized : null;
    }

    return this.normalizeEvmAddress(configured);
  }

  private normalizePayerAddress(chainKey: string, payerAddress: string): string | null {
    if (!payerAddress) {
      return null;
    }

    const normalizedRaw = payerAddress.trim();
    if (!normalizedRaw) {
      return null;
    }

    if (chainKey === 'stellar') {
      return StellarSdk.StrKey.isValidEd25519PublicKey(normalizedRaw) ? normalizedRaw : null;
    }

    return this.normalizeEvmAddress(normalizedRaw);
  }

  private normalizeEvmAddress(address: string): string | null {
    const trimmed = address.trim();
    if (!/^0x[a-fA-F0-9]{40}$/.test(trimmed)) {
      return null;
    }

    try {
      return ethers.getAddress(trimmed);
    } catch {
      return null;
    }
  }

  private toBaseUnits(amount: string, decimals: number, fieldName: string): bigint {
    try {
      return ethers.parseUnits(this.normalizeDecimal(amount), decimals);
    } catch {
      throw new BadRequestException(
        `Invalid ${fieldName}. Could not parse amount for token decimals=${decimals}`,
      );
    }
  }

  private fromBaseUnits(amount: bigint, decimals: number): string {
    return this.normalizeDecimal(ethers.formatUnits(amount, decimals));
  }

  private normalizeDecimal(value: string): string {
    const raw = value.trim();
    if (!raw.includes('.')) {
      const whole = raw.replace(/^0+(?=\d)/, '');
      return whole.length > 0 ? whole : '0';
    }

    const [wholeRaw, fractionRaw = ''] = raw.split('.');
    const whole = wholeRaw.replace(/^0+(?=\d)/, '') || '0';
    const fraction = fractionRaw.replace(/0+$/, '');

    return fraction.length > 0 ? `${whole}.${fraction}` : whole;
  }

  private resolveAddressMap(value: string | undefined): Record<string, string> {
    if (!value) {
      return {};
    }

    const map: Record<string, string> = {};
    const upsert = (chainKey: string, address: string) => {
      const normalizedChainKey = this.normalizeChainKey(chainKey);
      const normalizedAddress = this.normalizeAddressString(address);
      if (!normalizedChainKey || !normalizedAddress) {
        return;
      }
      map[normalizedChainKey] = normalizedAddress;
    };

    try {
      const parsed = JSON.parse(value);
      if (parsed && typeof parsed === 'object') {
        for (const [chainKey, address] of Object.entries(parsed as Record<string, unknown>)) {
          if (typeof address === 'string') {
            upsert(chainKey, address);
          }
        }
      }
    } catch {
      const pairs = value.split(',');
      for (const pair of pairs) {
        const [rawChainKey, ...rest] = pair.split('=');
        const rawAddress = rest.join('=');
        if (rawChainKey && rawAddress) {
          upsert(rawChainKey, rawAddress);
        }
      }
    }

    return map;
  }

  private resolveFeeBps(value: string | undefined, fallback: number): number {
    if (value === undefined || value === null || value === '') {
      return fallback;
    }

    const parsed = Number.parseInt(value, 10);
    if (!Number.isInteger(parsed) || parsed < 0 || parsed > 1000) {
      return fallback;
    }

    return parsed;
  }

  private normalizeAction(value: unknown): PlatformFeeAction | null {
    if (typeof value !== 'string') {
      return null;
    }

    const normalized = value.trim().toLowerCase();
    return PLATFORM_FEE_ACTIONS.find((action) => action === normalized) || null;
  }

  private normalizeStatus(value: unknown): PlatformFeeStatus | null {
    if (value === 'accrued' || value === 'collected') {
      return value;
    }
    return null;
  }

  private normalizeFeeIds(feeIds?: string[]): Types.ObjectId[] {
    if (!Array.isArray(feeIds) || feeIds.length === 0) {
      return [];
    }

    return feeIds
      .map((id) => id.trim())
      .filter((id) => Types.ObjectId.isValid(id))
      .map((id) => new Types.ObjectId(id));
  }

  private normalizeAddressString(value: string | undefined | null): string | null {
    if (!value) {
      return null;
    }
    const normalized = value.trim();
    return normalized.length > 0 ? normalized : null;
  }

  private toPositiveNumber(value: unknown): number {
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  private roundTo6(value: number): number {
    if (!Number.isFinite(value)) {
      return 0;
    }
    return Number(value.toFixed(6));
  }
}
