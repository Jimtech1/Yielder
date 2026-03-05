import { Module } from '@nestjs/common';
import { OracleController } from './oracle.controller';
import { OracleService } from './oracle.service';
import { PortfolioModule } from '../portfolio/portfolio.module';

@Module({
  imports: [PortfolioModule],
  controllers: [OracleController],
  providers: [OracleService],
  exports: [OracleService],
})
export class OracleModule {}
