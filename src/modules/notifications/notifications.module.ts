import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { User, UserSchema } from '../auth/schemas/user.schema';
import { Transaction, TransactionSchema } from '../wallet/schemas/transaction.schema';
import { Wallet, WalletSchema } from '../wallet/schemas/wallet.schema';
import { Announcement, AnnouncementSchema } from './schemas/announcement.schema';
import { AdminAuditEvent, AdminAuditEventSchema } from './schemas/admin-audit-event.schema';
import {
  AdminFeatureFlags,
  AdminFeatureFlagsSchema,
} from './schemas/admin-feature-flags.schema';
import { NotificationsController } from './notifications.controller';
import { NotificationsService } from './notifications.service';
import {
  PlatformFeeRecord,
  PlatformFeeRecordSchema,
} from '../platform-fee/schemas/platform-fee-record.schema';
import { BridgeHistory, BridgeHistorySchema } from '../defi/schemas/bridge-history.schema';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: User.name, schema: UserSchema },
      { name: Wallet.name, schema: WalletSchema },
      { name: Transaction.name, schema: TransactionSchema },
      { name: Announcement.name, schema: AnnouncementSchema },
      { name: AdminAuditEvent.name, schema: AdminAuditEventSchema },
      { name: AdminFeatureFlags.name, schema: AdminFeatureFlagsSchema },
      { name: PlatformFeeRecord.name, schema: PlatformFeeRecordSchema },
      { name: BridgeHistory.name, schema: BridgeHistorySchema },
    ]),
  ],
  controllers: [NotificationsController],
  providers: [NotificationsService],
  exports: [NotificationsService],
})
export class NotificationsModule {}
