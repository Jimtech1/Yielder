import { Module } from '@nestjs/common';
import { MarketModule } from '../market/market.module';
import { OracleModule } from '../oracle/oracle.module';
import { RealtimeController } from './realtime.controller';
import { RealtimeService } from './realtime.service';

@Module({
  imports: [MarketModule, OracleModule],
  controllers: [RealtimeController],
  providers: [RealtimeService],
})
export class RealtimeModule {}
