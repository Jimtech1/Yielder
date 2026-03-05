import { Controller, Get, Param } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { MarketService } from './market.service';

@ApiTags('market')
@Controller('api/stellar')
export class MarketController {
  constructor(private readonly marketService: MarketService) {}

  @Get('network-stats')
  getNetworkStats() {
    return this.marketService.getNetworkStats();
  }

  @Get('trending-assets')
  getTrendingAssets() {
    return this.marketService.getTrendingAssets();
  }

  @Get('liquidity-pools')
  getLiquidityPools() {
    return this.marketService.getLiquidityPools();
  }

  @Get('soroban-protocols')
  getSorobanProtocols() {
    return this.marketService.getSorobanProtocols();
  }

  @Get('account/:address')
  getAccountSummary(@Param('address') address: string) {
    return this.marketService.getAccountSummary(address);
  }

  @Get('defi/positions/:address')
  getDeFiPositions(@Param('address') address: string) {
    return this.marketService.getDeFiPositions(address);
  }
}
