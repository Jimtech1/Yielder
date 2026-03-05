import { Controller, Get, Post, Delete, Body, Param, UseGuards, Req, Query } from '@nestjs/common';
import { ApiTags, ApiBearerAuth } from '@nestjs/swagger';
import { WalletService } from './wallet.service';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { ImportWalletDto } from './dto';
import { AddWatchWalletDto } from './dto/add-watch-wallet.dto';
import { GetClaimableBalancesDto } from './dto/get-claimable-balances.dto';
import { BuildClaimBalanceDto } from './dto/build-claim-balance.dto';
import { BuildWithdrawalDto } from './dto/build-withdrawal.dto';
import { RecordActivityTransactionDto } from './dto/record-activity-transaction.dto';
import { AuthenticatedUser } from '../auth/interfaces/authenticated-user.interface';
import { NotificationsService } from '../notifications/notifications.service';

@ApiTags('wallet')
@Controller('wallet')
export class WalletController {
  constructor(
    private walletService: WalletService,
    private notificationsService: NotificationsService,
  ) {}

  @Post('connect')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async connectWallet(@Req() req: { user: AuthenticatedUser }, @Body() dto: ImportWalletDto) {
    const result = await this.walletService.connectWallet(req.user.userId, dto.address, dto.chain, dto.label);
    await this.safeRecordOptionalAdminEvent(req.user, {
      action: 'wallet_connect',
      status: 'success',
      target: dto.address,
      details: `Connected ${dto.chain || 'stellar'} wallet`,
    });
    return result;
  }

  @Post('watch')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async addWatchWallet(@Req() req: { user: AuthenticatedUser }, @Body() dto: AddWatchWalletDto) {
    const result = await this.walletService.addWatchWallet(req.user.userId, dto);
    await this.safeRecordOptionalAdminEvent(req.user, {
      action: 'wallet_watch_add',
      status: 'success',
      target: dto.address,
      details: `Added watch wallet (${dto.chain})`,
    });
    return result;
  }

  @Get()
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  getWallets(@Req() req: { user: AuthenticatedUser }) {
    return this.walletService.getWallets(req.user.userId);
  }

  @Get('claimable/:address')
  getClaimableBalances(
    @Param('address') address: string,
    @Query() query: GetClaimableBalancesDto,
  ) {
    return this.walletService.getClaimableBalances(address, query.limit, query.asset);
  }

  @Post('claimable/build-claim-tx')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  buildClaimableBalanceTx(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: BuildClaimBalanceDto,
  ) {
    return this.walletService.buildClaimBalanceTransaction(
      req.user.userId,
      dto.sourcePublicKey,
      dto.balanceId,
      dto.memo,
      dto,
    );
  }

  @Post('withdraw')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  buildWithdrawalTx(@Req() req: { user: AuthenticatedUser }, @Body() dto: BuildWithdrawalDto) {
    return this.walletService.buildWithdrawalTransaction(req.user.userId, dto);
  }

  @Post('activity')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  recordActivity(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: RecordActivityTransactionDto,
  ) {
    return this.walletService.recordActivityTransaction(req.user.userId, dto);
  }

  @Get(':id')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  getWallet(@Req() req: { user: AuthenticatedUser }, @Param('id') id: string) {
    return this.walletService.getWallet(req.user.userId, id);
  }

  @Delete(':id')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async deleteWallet(@Req() req: { user: AuthenticatedUser }, @Param('id') id: string) {
    const result = await this.walletService.deleteWallet(req.user.userId, id);
    await this.safeRecordOptionalAdminEvent(req.user, {
      action: 'wallet_archive',
      status: 'success',
      target: id,
      details: 'Archived wallet',
    });
    return result;
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
      // Ignore audit failures in wallet flows.
    }
  }
}
