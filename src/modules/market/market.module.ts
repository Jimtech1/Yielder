import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { MarketController } from './market.controller';
import { MarketService } from './market.service';
import { Wallet, WalletSchema } from '../wallet/schemas/wallet.schema';
import { DeFiPosition, DeFiPositionSchema } from '../defi/schemas/defi-position.schema';
import { PortfolioModule } from '../portfolio/portfolio.module';

@Module({
  imports: [
    PortfolioModule,
    MongooseModule.forFeature([
      { name: Wallet.name, schema: WalletSchema },
      { name: DeFiPosition.name, schema: DeFiPositionSchema },
    ]),
  ],
  controllers: [MarketController],
  providers: [MarketService],
  exports: [MarketService],
})
export class MarketModule {}
