import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { User, UserDocument } from '../auth/schemas/user.schema';
import { Transaction, TransactionDocument } from '../wallet/schemas/transaction.schema';
import { Wallet, WalletDocument } from '../wallet/schemas/wallet.schema';
import {
  Announcement,
  AnnouncementDocument,
  AnnouncementStatus,
} from './schemas/announcement.schema';
import {
  AdminAuditEvent,
  AdminAuditEventDocument,
  AdminAuditStatus,
} from './schemas/admin-audit-event.schema';
import {
  AdminFeatureFlags,
  AdminFeatureFlagsDocument,
} from './schemas/admin-feature-flags.schema';
import {
  PlatformFeeRecord,
  PlatformFeeRecordDocument,
} from '../platform-fee/schemas/platform-fee-record.schema';
import { BridgeHistory, BridgeHistoryDocument } from '../defi/schemas/bridge-history.schema';

type TransactionNotificationSource = {
  _id: unknown;
  walletId?: unknown;
  type?: string;
  hash?: string;
  chain?: string;
  amount?: string | number;
  asset?: string;
  status?: string;
  timestamp?: Date | string;
  createdAt?: Date | string;
};

type AnnouncementNotificationSource = {
  _id: unknown;
  title?: string;
  message?: string;
  status?: string;
  createdAt?: Date | string;
  updatedAt?: Date | string;
  createdByUserId?: string;
  createdByEmail?: string;
};

export type NotificationItem = {
  id: string;
  title: string;
  message: string;
  type: string;
  severity: 'success' | 'warning' | 'error' | 'info';
  status: string;
  txHash?: string;
  chain: string;
  createdAt: string;
  isRead: boolean;
};

export type NotificationsPayload = {
  items: NotificationItem[];
  unreadCount: number;
  lastReadAt: string | null;
};

export type AdminAnnouncementItem = {
  id: string;
  title: string;
  message: string;
  status: AnnouncementStatus;
  createdAt: string;
  updatedAt: string;
  createdByUserId: string;
  createdByEmail: string | null;
};

export type AdminAnnouncementsPayload = {
  items: AdminAnnouncementItem[];
};

type AdminAuditEventSource = {
  _id: unknown;
  action?: string;
  status?: string;
  target?: string;
  details?: string;
  actorUserId?: string;
  actorEmail?: string;
  createdAt?: Date | string;
  updatedAt?: Date | string;
};

export type AdminAuditEventItem = {
  id: string;
  action: string;
  status: AdminAuditStatus;
  target: string;
  details: string;
  actorUserId: string;
  actorEmail: string | null;
  createdAt: string;
  updatedAt: string;
};

export type AdminAuditEventsPayload = {
  items: AdminAuditEventItem[];
};

type AdminFeatureFlagsSource = {
  _id: unknown;
  userId?: string;
  autoRefreshUsers?: boolean;
  requireBulkConfirmation?: boolean;
  compactUserRows?: boolean;
  createdAt?: Date | string;
  updatedAt?: Date | string;
};

export type AdminFeatureFlagsItem = {
  autoRefreshUsers: boolean;
  requireBulkConfirmation: boolean;
  compactUserRows: boolean;
  updatedAt: string | null;
};

type PlatformFeeRecordSource = {
  _id: unknown;
  userId?: string;
  action?: string;
  chainKey?: string;
  feeAmount?: string;
  assetSymbol?: string;
  status?: string;
  createdAt?: Date | string;
  updatedAt?: Date | string;
};

type BridgeHistorySource = {
  _id: unknown;
  userId?: string;
  provider?: string;
  srcChainKey?: string;
  dstChainKey?: string;
  bridgeTxHash?: string;
  status?: string;
  createdAt?: Date | string;
  updatedAt?: Date | string;
};

export type AdminTimelineModule = 'auth' | 'defi' | 'wallet' | 'rpc' | 'notifications';

export type AdminTimelineItem = {
  id: string;
  module: AdminTimelineModule;
  action: string;
  status: 'success' | 'error' | 'partial';
  target: string;
  details: string;
  createdAt: string;
};

export type AdminTimelinePayload = {
  items: AdminTimelineItem[];
};

@Injectable()
export class NotificationsService {
  constructor(
    @InjectModel(Wallet.name) private readonly walletModel: Model<WalletDocument>,
    @InjectModel(Transaction.name) private readonly transactionModel: Model<TransactionDocument>,
    @InjectModel(User.name) private readonly userModel: Model<UserDocument>,
    @InjectModel(Announcement.name)
    private readonly announcementModel: Model<AnnouncementDocument>,
    @InjectModel(AdminAuditEvent.name)
    private readonly adminAuditEventModel: Model<AdminAuditEventDocument>,
    @InjectModel(AdminFeatureFlags.name)
    private readonly adminFeatureFlagsModel: Model<AdminFeatureFlagsDocument>,
    @InjectModel(PlatformFeeRecord.name)
    private readonly platformFeeRecordModel: Model<PlatformFeeRecordDocument>,
    @InjectModel(BridgeHistory.name)
    private readonly bridgeHistoryModel: Model<BridgeHistoryDocument>,
  ) {}

  async getNotifications(userId: string, limit = 15): Promise<NotificationsPayload> {
    const safeLimit = Number.isFinite(limit) ? Math.min(Math.max(Math.floor(limit), 1), 50) : 15;

    const [wallets, user, announcements] = await Promise.all([
      this.walletModel.find({ userId, isArchived: false } as any).select('_id').lean().exec(),
      this.userModel.findById(userId).select('notificationLastReadAt').lean().exec(),
      this.announcementModel
        .find({ status: 'active' } as any)
        .sort({ createdAt: -1 })
        .limit(safeLimit)
        .lean()
        .exec() as Promise<AnnouncementNotificationSource[]>,
    ]);

    const lastReadAt = this.toSafeDate(
      (user as { notificationLastReadAt?: unknown } | null)?.notificationLastReadAt,
    );

    const walletIds = wallets.map((wallet) => wallet._id);
    const transactions: TransactionNotificationSource[] =
      walletIds.length === 0
        ? []
        : ((await this.transactionModel
            .find({ walletId: { $in: walletIds } } as any)
            .sort({ timestamp: -1, createdAt: -1 })
            .limit(safeLimit)
            .lean()
            .exec()) as TransactionNotificationSource[]);

    const [transactionUnreadCount, announcementUnreadCount] = await Promise.all([
      walletIds.length === 0 ? Promise.resolve(0) : this.countUnreadNotifications(walletIds, lastReadAt),
      this.countUnreadAnnouncements(lastReadAt),
    ]);

    const items = [
      ...transactions.map((tx) => this.mapTransactionToNotification(tx, lastReadAt)),
      ...announcements.map((announcement) =>
        this.mapAnnouncementToNotification(announcement, lastReadAt),
      ),
    ]
      .sort((left, right) => {
        const leftMs = this.toSafeDate(left.createdAt)?.getTime() || 0;
        const rightMs = this.toSafeDate(right.createdAt)?.getTime() || 0;
        return rightMs - leftMs;
      })
      .slice(0, safeLimit);

    return {
      items,
      unreadCount: transactionUnreadCount + announcementUnreadCount,
      lastReadAt: lastReadAt ? lastReadAt.toISOString() : null,
    };
  }

  async markAllAsRead(userId: string): Promise<{ lastReadAt: string }> {
    const now = new Date();
    await this.userModel.updateOne({ _id: userId } as any, { $set: { notificationLastReadAt: now } }).exec();
    return { lastReadAt: now.toISOString() };
  }

  async listAnnouncements(
    limit = 25,
    statusFilter: 'all' | AnnouncementStatus = 'all',
  ): Promise<AdminAnnouncementsPayload> {
    const safeLimit = Number.isFinite(limit) ? Math.min(Math.max(Math.floor(limit), 1), 100) : 25;
    const filter =
      statusFilter === 'all'
        ? {}
        : ({
            status: statusFilter,
          } as Record<string, unknown>);

    const announcements = (await this.announcementModel
      .find(filter as any)
      .sort({ createdAt: -1 })
      .limit(safeLimit)
      .lean()
      .exec()) as AnnouncementNotificationSource[];

    return {
      items: announcements.map((item) => this.mapAnnouncementToAdminItem(item)),
    };
  }

  async createAnnouncement(params: {
    title: string;
    message: string;
    actorUserId: string;
    actorEmail?: string;
  }): Promise<AdminAnnouncementItem> {
    const title = params.title.trim();
    const message = params.message.trim();

    const created = (await this.announcementModel.create({
      title,
      message,
      status: 'active',
      createdByUserId: params.actorUserId,
      createdByEmail: params.actorEmail || null,
    })) as AnnouncementDocument;

    const record = created.toObject() as unknown as AnnouncementNotificationSource;
    return this.mapAnnouncementToAdminItem(record);
  }

  async updateAnnouncementStatus(params: {
    announcementId: string;
    status: AnnouncementStatus;
  }): Promise<AdminAnnouncementItem> {
    const updated = (await this.announcementModel
      .findByIdAndUpdate(
        params.announcementId,
        { $set: { status: params.status } },
        { new: true },
      )
      .lean()
      .exec()) as AnnouncementNotificationSource | null;

    if (!updated) {
      throw new NotFoundException('Announcement not found');
    }

    return this.mapAnnouncementToAdminItem(updated);
  }

  async recordAdminAuditEvent(params: {
    action: string;
    status: AdminAuditStatus;
    target: string;
    details: string;
    actorUserId: string;
    actorEmail?: string;
  }): Promise<AdminAuditEventItem> {
    const created = (await this.adminAuditEventModel.create({
      action: params.action.trim().toLowerCase(),
      status: params.status,
      target: params.target.trim() || 'unknown',
      details: params.details.trim() || 'No details',
      actorUserId: params.actorUserId,
      actorEmail: params.actorEmail || null,
    })) as AdminAuditEventDocument;

    const record = created.toObject() as unknown as AdminAuditEventSource;
    return this.mapAuditEventToItem(record);
  }

  async listAdminAuditEvents(
    limit = 200,
    filters?: {
      action?: string;
      status?: 'all' | AdminAuditStatus;
    },
  ): Promise<AdminAuditEventsPayload> {
    const safeLimit = Number.isFinite(limit) ? Math.min(Math.max(Math.floor(limit), 1), 500) : 200;
    const query: Record<string, unknown> = {};

    const action = typeof filters?.action === 'string' ? filters.action.trim().toLowerCase() : '';
    if (action && action !== 'all') {
      query.action = action;
    }

    const status = typeof filters?.status === 'string' ? filters.status.trim().toLowerCase() : 'all';
    if (status === 'success' || status === 'error' || status === 'partial') {
      query.status = status;
    }

    const events = (await this.adminAuditEventModel
      .find(query as any)
      .sort({ createdAt: -1 })
      .limit(safeLimit)
      .lean()
      .exec()) as AdminAuditEventSource[];

    return {
      items: events.map((event) => this.mapAuditEventToItem(event)),
    };
  }

  async listBillingActivity(limit = 100): Promise<AdminAuditEventsPayload> {
    const safeLimit = Number.isFinite(limit) ? Math.min(Math.max(Math.floor(limit), 1), 300) : 100;
    const events = (await this.adminAuditEventModel
      .find({ action: 'tier_update' } as any)
      .sort({ createdAt: -1 })
      .limit(safeLimit)
      .lean()
      .exec()) as AdminAuditEventSource[];

    return {
      items: events.map((event) => this.mapAuditEventToItem(event)),
    };
  }

  async clearAdminAuditEvents(): Promise<{ deletedCount: number }> {
    const result = await this.adminAuditEventModel.deleteMany({} as any).exec();
    return {
      deletedCount: Number(result.deletedCount || 0),
    };
  }

  async getAdminFeatureFlags(userId: string): Promise<AdminFeatureFlagsItem> {
    const record = (await this.adminFeatureFlagsModel
      .findOne({ userId } as any)
      .lean()
      .exec()) as AdminFeatureFlagsSource | null;

    return this.mapFeatureFlags(record);
  }

  async updateAdminFeatureFlags(
    userId: string,
    flags: {
      autoRefreshUsers: boolean;
      requireBulkConfirmation: boolean;
      compactUserRows: boolean;
    },
  ): Promise<AdminFeatureFlagsItem> {
    const updated = (await this.adminFeatureFlagsModel
      .findOneAndUpdate(
        { userId } as any,
        {
          $set: {
            autoRefreshUsers: Boolean(flags.autoRefreshUsers),
            requireBulkConfirmation: Boolean(flags.requireBulkConfirmation),
            compactUserRows: Boolean(flags.compactUserRows),
          },
        },
        { new: true, upsert: true },
      )
      .lean()
      .exec()) as AdminFeatureFlagsSource | null;

    return this.mapFeatureFlags(updated);
  }

  async listAdminTimeline(
    limit = 250,
    moduleFilter: 'all' | AdminTimelineModule = 'all',
  ): Promise<AdminTimelinePayload> {
    const safeLimit = Number.isFinite(limit) ? Math.min(Math.max(Math.floor(limit), 1), 500) : 250;

    const [auditEvents, platformFeeRecords, bridgeHistoryRecords, transactions] = await Promise.all([
      this.adminAuditEventModel
        .find({} as any)
        .sort({ createdAt: -1 })
        .limit(safeLimit)
        .lean()
        .exec() as Promise<AdminAuditEventSource[]>,
      this.platformFeeRecordModel
        .find({} as any)
        .sort({ createdAt: -1 })
        .limit(safeLimit)
        .lean()
        .exec() as Promise<PlatformFeeRecordSource[]>,
      this.bridgeHistoryModel
        .find({} as any)
        .sort({ createdAt: -1 })
        .limit(safeLimit)
        .lean()
        .exec() as Promise<BridgeHistorySource[]>,
      this.transactionModel
        .find({} as any)
        .sort({ timestamp: -1, createdAt: -1 })
        .limit(safeLimit)
        .lean()
        .exec() as Promise<TransactionNotificationSource[]>,
    ]);

    const walletIds = transactions
      .map((tx) => (tx as { walletId?: unknown }).walletId)
      .filter((walletId): walletId is unknown => walletId !== undefined && walletId !== null);
    const wallets = walletIds.length
      ? await this.walletModel
          .find({ _id: { $in: walletIds } } as any)
          .select('_id userId chain address')
          .lean()
          .exec()
      : [];
    const walletById = new Map<string, { userId?: unknown; chain?: unknown; address?: unknown }>();
    wallets.forEach((wallet) => {
      walletById.set(String(wallet._id), wallet as { userId?: unknown; chain?: unknown; address?: unknown });
    });

    const merged: AdminTimelineItem[] = [
      ...auditEvents.map((event) => this.mapAuditEventToTimelineItem(event)),
      ...platformFeeRecords.map((record) => this.mapPlatformFeeToTimelineItem(record)),
      ...bridgeHistoryRecords.map((record) => this.mapBridgeHistoryToTimelineItem(record)),
      ...transactions.map((tx) =>
        this.mapWalletTransactionToTimelineItem(
          tx,
          walletById.get(String((tx as { walletId?: unknown }).walletId || '')),
        ),
      ),
    ];

    const filtered = moduleFilter === 'all'
      ? merged
      : merged.filter((item) => item.module === moduleFilter);

    filtered.sort((left, right) => {
      const leftMs = this.toSafeDate(left.createdAt)?.getTime() || 0;
      const rightMs = this.toSafeDate(right.createdAt)?.getTime() || 0;
      return rightMs - leftMs;
    });

    return {
      items: filtered.slice(0, safeLimit),
    };
  }

  private async countUnreadNotifications(
    walletIds: unknown[],
    lastReadAt: Date | null,
  ): Promise<number> {
    if (!lastReadAt) {
      return this.transactionModel.countDocuments({ walletId: { $in: walletIds } } as any).exec();
    }

    return this.transactionModel
      .countDocuments({
        walletId: { $in: walletIds },
        $or: [
          { timestamp: { $gt: lastReadAt } },
          {
            timestamp: { $exists: false },
            createdAt: { $gt: lastReadAt },
          },
        ],
      } as any)
      .exec();
  }

  private async countUnreadAnnouncements(lastReadAt: Date | null): Promise<number> {
    if (!lastReadAt) {
      return this.announcementModel.countDocuments({ status: 'active' } as any).exec();
    }

    return this.announcementModel
      .countDocuments({
        status: 'active',
        createdAt: { $gt: lastReadAt },
      } as any)
      .exec();
  }

  private mapTransactionToNotification(
    tx: TransactionNotificationSource,
    lastReadAt: Date | null,
  ): NotificationItem {
    const status = this.normalizeStatus(tx.status);
    const type = this.normalizeType(tx.type);
    const timestamp = this.resolveTimestamp(tx);

    return {
      id: String(tx._id),
      title: this.buildTitle(type, status),
      message: this.buildMessage(tx),
      type,
      severity: this.resolveSeverity(status),
      status,
      txHash: typeof tx.hash === 'string' ? tx.hash : undefined,
      chain: this.normalizeChain(tx.chain),
      createdAt: timestamp.toISOString(),
      isRead: lastReadAt ? timestamp.getTime() <= lastReadAt.getTime() : false,
    };
  }

  private mapAnnouncementToNotification(
    announcement: AnnouncementNotificationSource,
    lastReadAt: Date | null,
  ): NotificationItem {
    const createdAt = this.toSafeDate(announcement.createdAt) ?? new Date(0);
    const title =
      typeof announcement.title === 'string' && announcement.title.trim()
        ? announcement.title.trim()
        : 'Announcement';
    const message =
      typeof announcement.message === 'string' && announcement.message.trim()
        ? announcement.message.trim()
        : 'New announcement available.';
    const status =
      typeof announcement.status === 'string' && announcement.status.trim()
        ? announcement.status.trim().toLowerCase()
        : 'active';

    return {
      id: String(announcement._id),
      title,
      message,
      type: 'announcement',
      severity: 'info',
      status,
      chain: 'System',
      createdAt: createdAt.toISOString(),
      isRead: lastReadAt ? createdAt.getTime() <= lastReadAt.getTime() : false,
    };
  }

  private mapAnnouncementToAdminItem(
    announcement: AnnouncementNotificationSource,
  ): AdminAnnouncementItem {
    const createdAt = this.toSafeDate(announcement.createdAt) ?? new Date(0);
    const updatedAt = this.toSafeDate(announcement.updatedAt) ?? createdAt;
    const statusRaw = typeof announcement.status === 'string' ? announcement.status.trim().toLowerCase() : 'active';
    const status: AnnouncementStatus = statusRaw === 'archived' ? 'archived' : 'active';

    return {
      id: String(announcement._id),
      title:
        typeof announcement.title === 'string' && announcement.title.trim().length > 0
          ? announcement.title.trim()
          : 'Announcement',
      message:
        typeof announcement.message === 'string' && announcement.message.trim().length > 0
          ? announcement.message.trim()
          : '',
      status,
      createdAt: createdAt.toISOString(),
      updatedAt: updatedAt.toISOString(),
      createdByUserId:
        typeof announcement.createdByUserId === 'string' ? announcement.createdByUserId : '',
      createdByEmail:
        typeof announcement.createdByEmail === 'string' && announcement.createdByEmail.trim().length > 0
          ? announcement.createdByEmail.trim()
          : null,
    };
  }

  private mapAuditEventToItem(event: AdminAuditEventSource): AdminAuditEventItem {
    const createdAt = this.toSafeDate(event.createdAt) ?? new Date(0);
    const updatedAt = this.toSafeDate(event.updatedAt) ?? createdAt;
    const statusRaw = typeof event.status === 'string' ? event.status.trim().toLowerCase() : 'success';
    const status: AdminAuditStatus =
      statusRaw === 'error' || statusRaw === 'partial' ? statusRaw : 'success';
    const action =
      typeof event.action === 'string' && event.action.trim().length > 0
        ? event.action.trim().toLowerCase()
        : 'admin_action';

    return {
      id: String(event._id),
      action,
      status,
      target: typeof event.target === 'string' && event.target.trim().length > 0 ? event.target.trim() : 'unknown',
      details:
        typeof event.details === 'string' && event.details.trim().length > 0
          ? event.details.trim()
          : 'No details',
      actorUserId:
        typeof event.actorUserId === 'string' && event.actorUserId.trim().length > 0
          ? event.actorUserId.trim()
          : '',
      actorEmail:
        typeof event.actorEmail === 'string' && event.actorEmail.trim().length > 0
          ? event.actorEmail.trim()
          : null,
      createdAt: createdAt.toISOString(),
      updatedAt: updatedAt.toISOString(),
    };
  }

  private mapFeatureFlags(record: AdminFeatureFlagsSource | null): AdminFeatureFlagsItem {
    const updatedAt = this.toSafeDate(record?.updatedAt);
    return {
      autoRefreshUsers:
        typeof record?.autoRefreshUsers === 'boolean' ? record.autoRefreshUsers : true,
      requireBulkConfirmation:
        typeof record?.requireBulkConfirmation === 'boolean'
          ? record.requireBulkConfirmation
          : true,
      compactUserRows:
        typeof record?.compactUserRows === 'boolean' ? record.compactUserRows : false,
      updatedAt: updatedAt ? updatedAt.toISOString() : null,
    };
  }

  private mapAuditEventToTimelineItem(event: AdminAuditEventSource): AdminTimelineItem {
    const auditItem = this.mapAuditEventToItem(event);
    const module = this.resolveTimelineModuleFromAuditAction(auditItem.action);
    return {
      id: `audit-${auditItem.id}`,
      module,
      action: auditItem.action,
      status: auditItem.status,
      target: auditItem.target,
      details: auditItem.details,
      createdAt: auditItem.createdAt,
    };
  }

  private mapPlatformFeeToTimelineItem(record: PlatformFeeRecordSource): AdminTimelineItem {
    const createdAt = this.toSafeDate(record.createdAt) ?? new Date(0);
    const statusRaw = typeof record.status === 'string' ? record.status.trim().toLowerCase() : 'accrued';
    const status: 'success' | 'error' | 'partial' =
      statusRaw === 'collected' ? 'success' : statusRaw === 'failed' ? 'error' : 'partial';
    const action =
      typeof record.action === 'string' && record.action.trim().length > 0
        ? `platform_fee_${record.action.trim().toLowerCase()}`
        : 'platform_fee';
    const chainKey =
      typeof record.chainKey === 'string' && record.chainKey.trim().length > 0
        ? record.chainKey.trim().toLowerCase()
        : 'unknown';
    const userId =
      typeof record.userId === 'string' && record.userId.trim().length > 0 ? record.userId.trim() : 'unknown';
    const feeAmount =
      typeof record.feeAmount === 'string' && record.feeAmount.trim().length > 0
        ? record.feeAmount.trim()
        : '0';
    const symbol =
      typeof record.assetSymbol === 'string' && record.assetSymbol.trim().length > 0
        ? record.assetSymbol.trim().toUpperCase()
        : 'asset';

    return {
      id: `fee-${String(record._id)}`,
      module: 'defi',
      action,
      status,
      target: userId,
      details: `${statusRaw} ${feeAmount} ${symbol} fee on ${chainKey}`,
      createdAt: createdAt.toISOString(),
    };
  }

  private mapBridgeHistoryToTimelineItem(record: BridgeHistorySource): AdminTimelineItem {
    const createdAt = this.toSafeDate(record.createdAt) ?? new Date(0);
    const statusRaw = typeof record.status === 'string' ? record.status.trim().toLowerCase() : 'submitted';
    const status = this.resolveTimelineStatus(statusRaw);
    const src = typeof record.srcChainKey === 'string' ? record.srcChainKey.trim().toLowerCase() : 'unknown';
    const dst = typeof record.dstChainKey === 'string' ? record.dstChainKey.trim().toLowerCase() : 'unknown';
    const txHash = typeof record.bridgeTxHash === 'string' ? record.bridgeTxHash.trim() : 'unknown';
    const userId = typeof record.userId === 'string' ? record.userId.trim() : 'unknown';
    const provider = typeof record.provider === 'string' ? record.provider.trim() : 'bridge';

    return {
      id: `bridge-${String(record._id)}`,
      module: 'defi',
      action: `bridge_${statusRaw || 'update'}`,
      status,
      target: userId,
      details: `${provider} ${src} -> ${dst} (${txHash})`,
      createdAt: createdAt.toISOString(),
    };
  }

  private mapWalletTransactionToTimelineItem(
    tx: TransactionNotificationSource,
    wallet?: { userId?: unknown; chain?: unknown; address?: unknown },
  ): AdminTimelineItem {
    const timestamp = this.resolveTimestamp(tx);
    const statusRaw = this.normalizeStatus(tx.status);
    const status = this.resolveTimelineStatus(statusRaw);
    const type = this.normalizeType(tx.type);
    const chain =
      typeof tx.chain === 'string'
        ? tx.chain.trim().toLowerCase()
        : typeof wallet?.chain === 'string'
          ? wallet.chain.trim().toLowerCase()
          : 'stellar';
    const userId =
      typeof wallet?.userId === 'string' && wallet.userId.trim().length > 0
        ? wallet.userId.trim()
        : 'unknown';
    const hash = typeof tx.hash === 'string' ? tx.hash : 'unknown';
    const asset = this.normalizeAsset(tx.asset) || 'asset';
    const amount = this.toAmountLabel(tx.amount) || '0';

    return {
      id: `wallet-${String(tx._id)}`,
      module: 'wallet',
      action: `tx_${type}`,
      status,
      target: userId,
      details: `${chain} ${hash} ${amount} ${asset} (${statusRaw})`,
      createdAt: timestamp.toISOString(),
    };
  }

  private resolveTimelineModuleFromAuditAction(action: string): AdminTimelineModule {
    const normalized = action.trim().toLowerCase();
    if (
      normalized === 'role_update' ||
      normalized === 'tier_update' ||
      normalized === 'account_suspend' ||
      normalized === 'account_unsuspend' ||
      normalized === 'session_revoke' ||
      normalized === 'password_reset_trigger' ||
      normalized === 'two_factor_reset'
    ) {
      return 'auth';
    }

    if (normalized.startsWith('rpc_')) {
      return 'rpc';
    }

    if (normalized.startsWith('defi_') || normalized.startsWith('platform_fee') || normalized.startsWith('bridge_')) {
      return 'defi';
    }

    if (
      normalized === 'announcement' ||
      normalized === 'notification' ||
      normalized === 'audit_log_clear' ||
      normalized === 'feature_flags'
    ) {
      return 'notifications';
    }

    return 'notifications';
  }

  private resolveTimelineStatus(status: string): 'success' | 'error' | 'partial' {
    const normalized = status.trim().toLowerCase();
    if (
      normalized === 'success' ||
      normalized === 'completed' ||
      normalized === 'confirmed' ||
      normalized === 'collected' ||
      normalized === 'active'
    ) {
      return 'success';
    }
    if (
      normalized === 'error' ||
      normalized === 'failed' ||
      normalized === 'rejected' ||
      normalized === 'disconnected'
    ) {
      return 'error';
    }
    return 'partial';
  }

  private resolveSeverity(status: string): NotificationItem['severity'] {
    if (status === 'failed') return 'error';
    if (status === 'pending') return 'warning';
    if (status === 'completed') return 'success';
    return 'info';
  }

  private normalizeStatus(value?: string): string {
    const normalized = (value || 'unknown').trim().toLowerCase();
    if (!normalized) return 'unknown';
    if (['confirmed', 'completed', 'success', 'succeeded'].includes(normalized)) return 'completed';
    if (['pending', 'processing'].includes(normalized)) return 'pending';
    if (['failed', 'error', 'rejected'].includes(normalized)) return 'failed';
    return normalized;
  }

  private normalizeType(value?: string): string {
    const normalized = (value || 'transaction').trim().toLowerCase();
    if (!normalized) return 'transaction';
    return normalized.replace(/\s+/g, '_');
  }

  private buildTitle(type: string, status: string): string {
    const label = this.toTitleCase(type.replace(/_/g, ' '));
    if (status === 'completed') return `${label} completed`;
    if (status === 'pending') return `${label} pending`;
    if (status === 'failed') return `${label} failed`;
    return `${label} update`;
  }

  private buildMessage(tx: TransactionNotificationSource): string {
    const amount = this.toAmountLabel(tx.amount);
    const asset = this.normalizeAsset(tx.asset);
    const chain = this.normalizeChain(tx.chain);

    if (amount && asset) {
      return `${amount} ${asset} on ${chain}`;
    }

    if (asset) {
      return `${asset} transaction on ${chain}`;
    }

    return `Transaction activity detected on ${chain}`;
  }

  private resolveTimestamp(tx: TransactionNotificationSource): Date {
    return this.toSafeDate(tx.timestamp) ?? this.toSafeDate(tx.createdAt) ?? new Date(0);
  }

  private toSafeDate(value: unknown): Date | null {
    if (value instanceof Date) {
      return Number.isNaN(value.getTime()) ? null : value;
    }

    if (typeof value === 'string' || typeof value === 'number') {
      const date = new Date(value);
      return Number.isNaN(date.getTime()) ? null : date;
    }

    return null;
  }

  private normalizeAsset(value?: string): string | null {
    if (typeof value !== 'string') return null;
    const normalized = value.trim();
    if (!normalized) return null;
    if (normalized.toLowerCase() === 'native') return 'XLM';
    if (normalized.includes(':')) {
      const [code] = normalized.split(':');
      return code ? code.toUpperCase() : normalized.toUpperCase();
    }
    return normalized.toUpperCase();
  }

  private normalizeChain(value?: string): string {
    const normalized = (value || 'stellar').trim().toLowerCase();
    if (!normalized) return 'Stellar';
    if (normalized === 'evm' || normalized === 'sepolia') return 'Ethereum';
    return normalized.charAt(0).toUpperCase() + normalized.slice(1);
  }

  private toAmountLabel(value: unknown): string | null {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return null;
    }

    return parsed.toLocaleString('en-US', {
      maximumFractionDigits: 6,
    });
  }

  private toTitleCase(value: string): string {
    return value
      .split(' ')
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(' ');
  }
}
