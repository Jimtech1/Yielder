import { Controller, Get, Query, Req, UseGuards } from '@nestjs/common';
import { ApiBearerAuth, ApiTags } from '@nestjs/swagger';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { AuthenticatedUser } from '../auth/interfaces/authenticated-user.interface';
import { AccessControlService } from '../access/access-control.service';
import { PerformanceService } from './performance.service';
import { PortfolioService } from './portfolio.service';

@ApiTags('portfolio')
@Controller('portfolio')
@UseGuards(JwtAuthGuard)
@ApiBearerAuth()
export class PortfolioController {
  constructor(
    private portfolioService: PortfolioService,
    private performanceService: PerformanceService,
    private accessControlService: AccessControlService,
  ) {}

  @Get()
  getPortfolio(@Req() req: { user: AuthenticatedUser }) {
    return this.portfolioService.getPortfolio(req.user.userId);
  }

  @Get('summary')
  getSummary(@Req() req: { user: AuthenticatedUser }) {
    return this.portfolioService.getSummary(req.user.userId);
  }

  @Get('assets')
  getAssets(@Req() req: { user: AuthenticatedUser }) {
    return this.portfolioService.getAssets(req.user.userId);
  }

  @Get('positions')
  getPositions(@Req() req: { user: AuthenticatedUser }) {
    return this.portfolioService.getPositions(req.user.userId);
  }

  @Get('history')
  getHistory(
    @Req() req: { user: AuthenticatedUser },
    @Query('days') days?: string,
    @Query('period') period?: string,
  ) {
    return this.portfolioService.getPortfolioHistory(
      req.user.userId,
      this.resolveDays(days, period),
    );
  }

  @Get('activity')
  getActivity(@Req() req: { user: AuthenticatedUser }, @Query('limit') limit?: string) {
    return this.portfolioService.getActivityFeed(
      req.user.userId,
      this.resolveLimit(limit),
    );
  }

  @Get('performance')
  getPerformance(@Req() req: { user: AuthenticatedUser }, @Query('days') days?: string) {
    this.ensureAdvancedAnalyticsAccess(req.user);
    return this.performanceService.getPortfolioPerformance(
      req.user.userId,
      this.resolveDays(days),
    );
  }

  @Get('pnl')
  getPnL(@Req() req: { user: AuthenticatedUser }, @Query('days') days?: string) {
    this.ensureAdvancedAnalyticsAccess(req.user);
    return this.performanceService.getPortfolioPerformance(
      req.user.userId,
      this.resolveDays(days),
    );
  }

  @Get('analytics/advanced')
  getAdvancedAnalytics(
    @Req() req: { user: AuthenticatedUser },
    @Query('days') days?: string,
  ) {
    this.ensureAdvancedAnalyticsAccess(req.user);
    return this.portfolioService.getAdvancedAnalytics(
      req.user.userId,
      this.resolveDays(days),
    );
  }

  @Get('analytics')
  getAnalytics(
    @Req() req: { user: AuthenticatedUser },
    @Query('days') days?: string,
    @Query('period') period?: string,
  ) {
    return this.portfolioService.getAnalytics(
      req.user.userId,
      this.resolveDays(days, period),
    );
  }

  private resolveDays(days?: string, period?: string): number {
    const parsedDays = Number.parseInt(days || '', 10);
    if (Number.isFinite(parsedDays) && parsedDays > 0) {
      return parsedDays;
    }

    const normalizedPeriod = (period || '').trim().toLowerCase();
    const periodToDays = new Map<string, number>([
      ['1d', 1],
      ['1w', 7],
      ['1m', 30],
      ['3m', 90],
      ['6m', 180],
      ['1y', 365],
    ]);

    return periodToDays.get(normalizedPeriod) ?? 30;
  }

  private resolveLimit(limit?: string): number {
    const parsedLimit = Number.parseInt(limit || '', 10);
    if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) {
      return 50;
    }
    return Math.min(parsedLimit, 200);
  }

  private ensureAdvancedAnalyticsAccess(user: AuthenticatedUser): void {
    const policy = this.accessControlService.resolvePolicyForTier(user.subscriptionTier);
    this.accessControlService.assertFeature(
      {
        tier: policy.tier,
        features: policy.features,
      },
      'advancedAnalytics',
    );
  }
}
