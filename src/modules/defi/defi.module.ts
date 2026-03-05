import { forwardRef, Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { DeFiPosition, DeFiPositionSchema } from './schemas/defi-position.schema';
import { BridgeHistory, BridgeHistorySchema } from './schemas/bridge-history.schema';
import {
  OptimizerExecution,
  OptimizerExecutionSchema,
} from './schemas/optimizer-execution.schema';
import {
  OptimizerFeeSettlement,
  OptimizerFeeSettlementSchema,
} from './schemas/optimizer-fee-settlement.schema';
import { Wallet, WalletSchema } from '../wallet/schemas/wallet.schema';
import { Transaction, TransactionSchema } from '../wallet/schemas/transaction.schema';
import { ConnectedWallet, ConnectedWalletSchema } from '../auth/schemas/connected-wallet.schema';
import { DeFiService } from './defi.service';
import { DeFiController } from './defi.controller';
import { WalletModule } from '../wallet/wallet.module';
import { PortfolioModule } from '../portfolio/portfolio.module';
import { PlatformFeeModule } from '../platform-fee/platform-fee.module';
import { AccessModule } from '../access/access.module';
import { NotificationsModule } from '../notifications/notifications.module';
import { MarketModule } from '../market/market.module';
import { CircleCctpV2CustomTxBuilderService } from './services/circle-cctp-v2-custom-tx-builder.service';

@Module({
  imports: [
    forwardRef(() => WalletModule),
    PortfolioModule,
    MarketModule,
    AccessModule,
    PlatformFeeModule,
    NotificationsModule,
    MongooseModule.forFeature([
      { name: DeFiPosition.name, schema: DeFiPositionSchema },
      { name: BridgeHistory.name, schema: BridgeHistorySchema },
      { name: OptimizerExecution.name, schema: OptimizerExecutionSchema },
      { name: OptimizerFeeSettlement.name, schema: OptimizerFeeSettlementSchema },
      { name: Wallet.name, schema: WalletSchema },
      { name: Transaction.name, schema: TransactionSchema },
      { name: ConnectedWallet.name, schema: ConnectedWalletSchema },
    ]),
  ],
  controllers: [DeFiController],
  providers: [DeFiService, CircleCctpV2CustomTxBuilderService],
  exports: [DeFiService],
})
export class DeFiModule {}
