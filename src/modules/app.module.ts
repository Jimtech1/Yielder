import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MongooseModule } from '@nestjs/mongoose';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { SharedModule } from '../shared/shared.module';
import { AuthModule } from './auth/auth.module';
import { WalletModule } from './wallet/wallet.module';
import { PortfolioModule } from './portfolio/portfolio.module';
import { IndexerModule } from './indexer/indexer.module';
import { DeFiModule } from './defi/defi.module';
import { RpcModule } from './rpc/rpc.module';
import { AnchorModule } from './anchor/anchor.module';
import { MarketModule } from './market/market.module';
import { OracleModule } from './oracle/oracle.module';
import { RealtimeModule } from './realtime/realtime.module';
import { NotificationsModule } from './notifications/notifications.module';
import { BillingModule } from './billing/billing.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MongooseModule.forRoot(process.env.MONGO_URI as string, {
      dbName: 'yielder',
      retryAttempts: 3,
    }),
    SharedModule,
    AuthModule,
    WalletModule,
    PortfolioModule,
    IndexerModule,
    DeFiModule,
    RpcModule,
    AnchorModule,
    MarketModule,
    OracleModule,
    RealtimeModule,
    NotificationsModule,
    BillingModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
