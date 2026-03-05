import {
  Body,
  Controller,
  ForbiddenException,
  Get,
  Headers,
  HttpException,
  Param,
  Post,
  Query,
  Req,
  UseGuards,
} from '@nestjs/common';
import { ApiBearerAuth, ApiTags } from '@nestjs/swagger';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { AuthenticatedUser } from '../auth/interfaces/authenticated-user.interface';
import { DeFiService } from './defi.service';
import { BuildPathPaymentDto } from './dto/build-path-payment.dto';
import { BuildBridgeTransactionDto } from './dto/build-bridge-transaction.dto';
import { BridgeQuoteDto } from './dto/bridge-quote.dto';
import { CreateOptimizerPlanDto } from './dto/create-optimizer-plan.dto';
import { ExecuteOptimizerDto } from './dto/execute-optimizer.dto';
import { ExecuteBorrowDto } from './dto/execute-borrow.dto';
import { CompleteOptimizerDepositDto } from './dto/complete-optimizer-deposit.dto';
import { SettleOptimizerFeeDto } from './dto/settle-optimizer-fee.dto';
import { ConfirmOptimizerFeeSettlementDto } from './dto/confirm-optimizer-fee-settlement.dto';
import { RecordBridgeHistoryDto } from './dto/record-bridge-history.dto';
import { PathPaymentQuoteDto } from './dto/path-payment-quote.dto';
import { BuildOptimizerStellarDepositTxDto } from './dto/build-optimizer-stellar-deposit-tx.dto';
import { CollectPlatformFeesDto } from './dto/collect-platform-fees.dto';
import { PlatformFeeRecordsQueryDto } from './dto/platform-fee-records-query.dto';
import { AccessControlService } from '../access/access-control.service';
import { NotificationsService } from '../notifications/notifications.service';
import { hasAdminPermission } from '../auth/admin-permissions';

@ApiTags('defi')
@Controller('defi')
export class DeFiController {
  constructor(
    private readonly defiService: DeFiService,
    private readonly accessControlService: AccessControlService,
    private readonly notificationsService: NotificationsService,
  ) {}

  @Get('yields')
  getYieldOpportunities(
    @Query('chain') chain?: string,
    @Query('category') category?: string,
    @Query('limit') limit?: string,
  ) {
    return this.defiService.getYieldOpportunities(chain, category, {
      limit: this.resolveYieldLimit(limit),
    });
  }

  @Post('optimizer/plan')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  createOptimizerPlan(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: CreateOptimizerPlanDto,
  ) {
    this.ensureOptimizerAccess(req.user);
    return this.defiService.createOptimizerPlan(req.user.userId, dto);
  }

  @Post('optimizer/execute')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  executeOptimizer(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: ExecuteOptimizerDto,
  ) {
    this.ensureOptimizerAccess(req.user);
    return this.defiService.executeOptimizer(req.user.userId, dto);
  }

  @Post('borrow/execute')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  executeBorrow(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: ExecuteBorrowDto,
  ) {
    this.ensureOptimizerAccess(req.user);
    return this.defiService.executeBorrow(req.user.userId, dto);
  }

  @Post('optimizer/:executionId/complete-deposit')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  completeOptimizerDeposit(
    @Req() req: { user: AuthenticatedUser },
    @Param('executionId') executionId: string,
    @Body() dto: CompleteOptimizerDepositDto,
  ) {
    this.ensureOptimizerAccess(req.user);
    return this.defiService.completeOptimizerDeposit(req.user.userId, executionId, dto);
  }

  @Post('optimizer/:executionId/build-stellar-deposit-tx')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  buildOptimizerStellarDepositTx(
    @Req() req: { user: AuthenticatedUser },
    @Param('executionId') executionId: string,
    @Body() dto: BuildOptimizerStellarDepositTxDto,
  ) {
    this.ensureOptimizerAccess(req.user);
    return this.defiService.buildOptimizerStellarDepositTransaction(
      req.user.userId,
      executionId,
      dto,
    );
  }

  @Get('optimizer/history')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  getOptimizerHistory(
    @Req() req: { user: AuthenticatedUser },
    @Query('limit') limit?: string,
  ) {
    this.ensureOptimizerAccess(req.user);
    return this.defiService.getOptimizerHistory(req.user.userId, this.resolveLimit(limit));
  }

  @Get('optimizer/fees/summary')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  getOptimizerFeeSummary(@Req() req: { user: AuthenticatedUser }) {
    this.ensureOptimizerAccess(req.user);
    return this.defiService.getOptimizerFeeSummary(req.user.userId);
  }

  @Post('optimizer/fees/settle')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  settleOptimizerFee(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: SettleOptimizerFeeDto,
  ) {
    this.ensureOptimizerAccess(req.user);
    return this.defiService.settleOptimizerFee(req.user.userId, dto);
  }

  @Get('optimizer/fees/settlements')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  getOptimizerFeeSettlements(
    @Req() req: { user: AuthenticatedUser },
    @Query('limit') limit?: string,
  ) {
    this.ensureOptimizerAccess(req.user);
    return this.defiService.getOptimizerFeeSettlements(req.user.userId, this.resolveLimit(limit));
  }

  @Post('optimizer/fees/settlements/:settlementId/confirm')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  confirmOptimizerFeeSettlement(
    @Req() req: { user: AuthenticatedUser },
    @Param('settlementId') settlementId: string,
    @Body() dto: ConfirmOptimizerFeeSettlementDto,
  ) {
    this.ensureOptimizerAccess(req.user);
    return this.defiService.confirmOptimizerFeeSettlement(req.user.userId, settlementId, dto.txHash);
  }

  @Get('bridge/chains')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  getBridgeChains() {
    return this.defiService.getBridgeChains();
  }

  @Get('bridge/tokens')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  getBridgeTokens(
    @Query('chainKey') chainKey?: string,
    @Query('bridgeableOnly') bridgeableOnly?: string,
  ) {
    const onlyBridgeable = bridgeableOnly === undefined || bridgeableOnly === 'true';
    return this.defiService.getBridgeTokens(chainKey, onlyBridgeable);
  }

  @Get('bridge/quote')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  getBridgeQuote(@Query() dto: BridgeQuoteDto) {
    return this.defiService.getBridgeQuote(dto);
  }

  @Post('bridge/build-tx')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  buildBridgeTransaction(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: BuildBridgeTransactionDto,
  ) {
    return this.defiService.buildBridgeTransaction(dto, req.user.userId);
  }

  @Post('bridge/internal/circle-cctp-v2/build-tx')
  buildInternalCircleCctpV2BridgeTransaction(
    @Body() payload: Record<string, unknown>,
    @Headers('x-api-key') apiKey?: string,
  ) {
    return this.defiService.buildInternalCircleCctpV2BridgeTransactionPayload(
      payload,
      apiKey,
    );
  }

  @Post('bridge/internal/circle-cctp-v2/custom/build-tx')
  buildInternalCustomCircleCctpV2BridgeTransaction(
    @Body() payload: Record<string, unknown>,
    @Headers('x-api-key') apiKey?: string,
  ) {
    return this.defiService.buildInternalCircleCctpV2CustomBridgeTransactionPayload(
      payload,
      apiKey,
    );
  }

  @Get('bridge/history')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  getBridgeHistory(
    @Req() req: { user: AuthenticatedUser },
    @Query('limit') limit?: string,
  ) {
    return this.defiService.getBridgeHistory(req.user.userId, this.resolveLimit(limit));
  }

  @Post('bridge/history')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  recordBridgeHistory(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: RecordBridgeHistoryDto,
  ) {
    return this.defiService.recordBridgeHistory(req.user.userId, dto);
  }

  @Post('bridge/history/:bridgeTxHash/refresh')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  refreshBridgeHistoryStatus(
    @Req() req: { user: AuthenticatedUser },
    @Param('bridgeTxHash') bridgeTxHash: string,
  ) {
    return this.defiService.refreshBridgeHistoryStatus(req.user.userId, bridgeTxHash);
  }

  @Get('swap/quote')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  getSwapQuote(@Query() dto: PathPaymentQuoteDto) {
    return this.defiService.getStrictSendQuote(dto);
  }

  @Post('swap/build-tx')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  buildSwapTransaction(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: BuildPathPaymentDto,
  ) {
    return this.defiService.buildStrictSendPathPayment(dto, req.user.userId);
  }

  @Get('platform-fees/summary')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async getPlatformFeeSummary(@Req() req: { user: AuthenticatedUser }) {
    this.ensurePermission(req.user, 'platform_fees.read');
    try {
      const result = await this.defiService.getPlatformFeeSummary();
      await this.safeRecordOptionalAdminEvent(req.user, {
        action: 'defi_platform_fee_summary',
        status: 'success',
        target: 'platform-fees/summary',
        details: 'Viewed platform fee summary',
      });
      return result;
    } catch (error) {
      await this.safeRecordOptionalAdminEvent(req.user, {
        action: 'defi_platform_fee_summary',
        status: 'error',
        target: 'platform-fees/summary',
        details: `Failed to fetch platform fee summary: ${this.resolveErrorMessage(error)}`,
      });
      throw error;
    }
  }

  @Get('platform-fees')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  getPlatformFeeRecords(
    @Req() req: { user: AuthenticatedUser },
    @Query() query: PlatformFeeRecordsQueryDto,
  ) {
    this.ensurePermission(req.user, 'platform_fees.read');
    return this.defiService
      .getPlatformFeeRecords({
        limit: query.limit ?? 100,
        status: query.status,
        action: query.action,
      })
      .then(async (result) => {
        await this.safeRecordOptionalAdminEvent(req.user, {
          action: 'defi_platform_fee_records',
          status: 'success',
          target: 'platform-fees',
          details: `Viewed platform fee records (count=${result.length || 0})`,
        });
        return result;
      })
      .catch(async (error) => {
        await this.safeRecordOptionalAdminEvent(req.user, {
          action: 'defi_platform_fee_records',
          status: 'error',
          target: 'platform-fees',
          details: `Failed to fetch platform fee records: ${this.resolveErrorMessage(error)}`,
        });
        throw error;
      });
  }

  @Post('platform-fees/collect')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async collectPlatformFees(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: CollectPlatformFeesDto,
  ) {
    this.ensurePermission(req.user, 'platform_fees.collect');
    try {
      const result = await this.defiService.collectPlatformFees(req.user.userId, dto);
      await this.safeRecordOptionalAdminEvent(req.user, {
        action: 'defi_platform_fee_collect',
        status: 'success',
        target: 'platform-fees/collect',
        details: `Collected platform fees. Total records: ${result.collectedCount || 0}`,
      });
      return result;
    } catch (error) {
      await this.safeRecordOptionalAdminEvent(req.user, {
        action: 'defi_platform_fee_collect',
        status: 'error',
        target: 'platform-fees/collect',
        details: `Failed to collect platform fees: ${this.resolveErrorMessage(error)}`,
      });
      throw error;
    }
  }

  private resolveLimit(limit?: string): number {
    const parsedLimit = Number.parseInt(limit || '', 10);
    if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) {
      return 20;
    }
    return Math.min(parsedLimit, 100);
  }

  private resolveYieldLimit(limit?: string): number | undefined {
    if (limit === undefined) {
      return undefined;
    }
    const parsedLimit = Number.parseInt(limit, 10);
    if (!Number.isFinite(parsedLimit) || parsedLimit < 0) {
      return undefined;
    }
    return Math.min(parsedLimit, 2000);
  }

  private ensurePermission(
    user: AuthenticatedUser,
    permission: Parameters<typeof hasAdminPermission>[1],
  ): void {
    if (hasAdminPermission(user.role, permission)) {
      return;
    }
    throw new ForbiddenException(`Admin permission required: ${permission}`);
  }

  private async safeRecordOptionalAdminEvent(
    user: AuthenticatedUser,
    payload: {
      action: string;
      status: 'success' | 'error' | 'partial';
      target: string;
      details: string;
    },
  ): Promise<void> {
    const role = (user.role || '').trim().toLowerCase();
    if (role !== 'owner' && role !== 'admin') {
      return;
    }
    try {
      await this.notificationsService.recordAdminAuditEvent({
        ...payload,
        actorUserId: user.userId,
        actorEmail: user.email,
      });
    } catch {
      // Do not block primary request flow on audit persistence failures.
    }
  }

  private resolveErrorMessage(error: unknown): string {
    if (error instanceof HttpException) {
      const response = error.getResponse() as unknown;
      if (typeof response === 'string' && response.trim().length > 0) {
        return response;
      }
      if (response && typeof response === 'object') {
        const message = (response as { message?: unknown }).message;
        if (typeof message === 'string' && message.trim().length > 0) {
          return message;
        }
        if (Array.isArray(message) && message.length > 0) {
          const first = message[0];
          if (typeof first === 'string' && first.trim().length > 0) {
            return first;
          }
        }
      }
      return error.message;
    }
    if (error instanceof Error && error.message.trim().length > 0) {
      return error.message;
    }
    return 'Unknown error';
  }

  private ensureOptimizerAccess(user: AuthenticatedUser): void {
    const policy = this.accessControlService.resolvePolicyForTier(user.subscriptionTier);
    this.accessControlService.assertFeature(
      {
        tier: policy.tier,
        features: policy.features,
      },
      'optimizer',
    );
  }
}
