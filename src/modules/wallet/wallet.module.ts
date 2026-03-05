import { forwardRef, Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { WalletController } from './wallet.controller';
import { WalletService } from './wallet.service';
import { StellarDriver } from './drivers/stellar.driver';
import { EvmDriver } from './drivers/evm.driver';
import { Wallet, WalletSchema } from './schemas/wallet.schema';
import { Transaction, TransactionSchema } from './schemas/transaction.schema';

import { WalletDiscoveryService } from './services/wallet-discovery.service';
import { TransactionBuilderService } from './services/transaction-builder.service';
import { FeeEstimationService } from './services/fee-estimation.service';
import { DeFiModule } from '../defi/defi.module';
import { PlatformFeeModule } from '../platform-fee/platform-fee.module';
import { AccessModule } from '../access/access.module';
import { NotificationsModule } from '../notifications/notifications.module';

@Module({
  imports: [
    forwardRef(() => DeFiModule),
    AccessModule,
    PlatformFeeModule,
    NotificationsModule,
    MongooseModule.forFeature([
      { name: Wallet.name, schema: WalletSchema },
      { name: Transaction.name, schema: TransactionSchema },
    ]),
  ],
  controllers: [WalletController],
  providers: [
    WalletService,
    StellarDriver,
    EvmDriver,
    WalletDiscoveryService,
    TransactionBuilderService,
    FeeEstimationService,
  ],
  exports: [
    WalletService,
    StellarDriver,
    EvmDriver,
    WalletDiscoveryService,
    TransactionBuilderService,
    FeeEstimationService,
  ],
})
export class WalletModule {}
