import { forwardRef, Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MongooseModule } from '@nestjs/mongoose';
import { PortfolioController } from './portfolio.controller';
import { PortfolioService } from './portfolio.service';
import { ValuationService } from './valuation.service';
import { PriceFeedService } from './price-feed.service';
import { PerformanceService } from './performance.service';
import { WalletModule } from '../wallet/wallet.module';
import { Wallet, WalletSchema } from '../wallet/schemas/wallet.schema';
import { Transaction, TransactionSchema } from '../wallet/schemas/transaction.schema';
import { PortfolioSnapshot, PortfolioSnapshotSchema } from './schemas/portfolio-snapshot.schema';
import { DeFiPosition, DeFiPositionSchema } from '../defi/schemas/defi-position.schema';
import { AccessModule } from '../access/access.module';

@Module({
  imports: [
    ConfigModule,
    forwardRef(() => WalletModule),
    AccessModule,
    MongooseModule.forFeature([
      { name: Wallet.name, schema: WalletSchema },
      { name: Transaction.name, schema: TransactionSchema },
      { name: PortfolioSnapshot.name, schema: PortfolioSnapshotSchema },
      { name: DeFiPosition.name, schema: DeFiPositionSchema },
    ]),
  ],
  controllers: [PortfolioController],
  providers: [PortfolioService, ValuationService, PriceFeedService, PerformanceService],
  exports: [PortfolioService, PerformanceService, PriceFeedService],
})
export class PortfolioModule {}
