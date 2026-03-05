import {
  Body,
  Controller,
  ForbiddenException,
  Get,
  HttpException,
  Param,
  Post,
  Query,
  Req,
  UseGuards,
} from '@nestjs/common';
import { Request } from 'express';
import { ApiTags, ApiBearerAuth } from '@nestjs/swagger';
import { AuthService } from './auth.service';
import { JwtAuthGuard } from './guards/jwt-auth.guard';
import {
  RegisterDto,
  LoginDto,
  RefreshTokenDto,
  ForgotPasswordDto,
  ResetPasswordDto,
  GoogleLoginDto,
  UpdateSubscriptionTierByEmailDto,
  UpdateSubscriptionTierDto,
  UpdateUserRoleByEmailDto,
  UpdateUserRoleDto,
} from './dto';
import { WalletChallengeDto, WalletLoginDto } from './dto/wallet-auth.dto';
import {
  AdminSuspendUserDto,
  AdminUserDetailsQueryDto,
} from './dto/admin-user-security.dto';
import { AuthenticatedUser } from './interfaces/authenticated-user.interface';
import { AccessControlService } from '../access/access-control.service';
import { NotificationsService } from '../notifications/notifications.service';
import {
  AdminPermissionKey,
  getAdminPermissionMatrix,
  hasAdminPermission,
  normalizeAdminRole,
} from './admin-permissions';

@ApiTags('auth')
@Controller('auth')
export class AuthController {
  constructor(
    private authService: AuthService,
    private accessControlService: AccessControlService,
    private notificationsService: NotificationsService,
  ) {}

  @Post('register')
  register(@Body() dto: RegisterDto, @Req() req: Request) {
    return this.authService.register(dto, req.ip);
  }

  @Post('login')
  login(@Body() dto: LoginDto, @Req() req: Request) {
    return this.authService.login(dto, req.ip);
  }

  @Post('google/login')
  loginWithGoogle(@Body() dto: GoogleLoginDto, @Req() req: Request) {
    return this.authService.loginWithGoogle(dto, req.ip);
  }

  @Post('refresh')
  refresh(@Body() dto: RefreshTokenDto) {
    return this.authService.refresh(dto);
  }

  @Post('logout')
  logout(@Body() dto: RefreshTokenDto) {
    return this.authService.logout(dto);
  }

  @Post('forgot-password')
  forgotPassword(@Body() dto: ForgotPasswordDto) {
    return this.authService.forgotPassword(dto);
  }

  @Post('reset-password')
  resetPassword(@Body() dto: ResetPasswordDto) {
    return this.authService.resetPassword(dto);
  }

  @Get('me')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async getProfile(@Req() req: { user: AuthenticatedUser }) {
    const [subscription, wallets] = await Promise.all([
      this.accessControlService.getSubscriptionInfo(req.user.userId),
      this.authService.getConnectedWalletSummaries(req.user.userId),
    ]);
    return {
      id: req.user.id || req.user.userId,
      email: req.user.email,
      role: req.user.role,
      subscription,
      wallets,
    };
  }

  @Post('subscription/tier')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async updateSubscriptionTier(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: UpdateSubscriptionTierDto,
  ) {
    this.ensurePermission(req.user, 'users.update_tier');
    await this.assertCanManageTargetById(req.user, dto.userId);
    try {
      const result = await this.accessControlService.updateUserSubscriptionTier({
        targetUserId: dto.userId,
        tier: dto.tier,
        updatedByUserId: req.user.userId,
      });
      await this.safeRecordAdminEvent(req.user, {
        action: 'tier_update',
        status: 'success',
        target: result.userId,
        details: `Tier set to ${dto.tier}`,
      });
      return result;
    } catch (error) {
      await this.safeRecordAdminEvent(req.user, {
        action: 'tier_update',
        status: 'error',
        target: dto.userId,
        details: `Failed setting tier to ${dto.tier}: ${this.resolveErrorMessage(error)}`,
      });
      throw error;
    }
  }

  @Post('subscription/tier/email')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async updateSubscriptionTierByEmail(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: UpdateSubscriptionTierByEmailDto,
  ) {
    this.ensurePermission(req.user, 'users.update_tier');
    const targetEmail = dto.email.trim().toLowerCase();
    try {
      const targetUser = await this.authService.getAdminUserSnapshotByEmail(targetEmail);
      this.assertCanManageTargetRole(req.user, targetUser.role);
      const result = await this.accessControlService.updateUserSubscriptionTierByEmail({
        email: dto.email,
        tier: dto.tier,
        updatedByUserId: req.user.userId,
      });
      await this.safeRecordAdminEvent(req.user, {
        action: 'tier_update',
        status: 'success',
        target: result.email || targetEmail,
        details: `Tier set to ${dto.tier}`,
      });
      return result;
    } catch (error) {
      await this.safeRecordAdminEvent(req.user, {
        action: 'tier_update',
        status: 'error',
        target: targetEmail || dto.email,
        details: `Failed setting tier to ${dto.tier}: ${this.resolveErrorMessage(error)}`,
      });
      throw error;
    }
  }

  @Get('admin/users')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  listUsersForAdmin(
    @Req() req: { user: AuthenticatedUser },
    @Query('search') search?: string,
    @Query('limit') limit?: string,
  ) {
    this.ensurePermission(req.user, 'users.read');
    const parsedLimit = Number.parseInt(limit || '', 10);
    return this.authService.listUsersForAdmin({
      search,
      limit: Number.isFinite(parsedLimit) && parsedLimit > 0 ? parsedLimit : 20,
    });
  }

  @Post('admin/role')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async updateUserRole(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: UpdateUserRoleDto,
  ) {
    this.ensurePermission(req.user, 'users.update_role');
    try {
      const result = await this.authService.updateUserRole({
        targetUserId: dto.userId,
        role: dto.role,
        actorRole: normalizeAdminRole(req.user.role),
      });
      await this.safeRecordAdminEvent(req.user, {
        action: 'role_update',
        status: 'success',
        target: result.userId,
        details: `Role set to ${dto.role}`,
      });
      return result;
    } catch (error) {
      await this.safeRecordAdminEvent(req.user, {
        action: 'role_update',
        status: 'error',
        target: dto.userId,
        details: `Failed setting role to ${dto.role}: ${this.resolveErrorMessage(error)}`,
      });
      throw error;
    }
  }

  @Post('admin/role/email')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async updateUserRoleByEmail(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: UpdateUserRoleByEmailDto,
  ) {
    this.ensurePermission(req.user, 'users.update_role');
    const targetEmail = dto.email.trim().toLowerCase();
    try {
      const result = await this.authService.updateUserRoleByEmail({
        email: dto.email,
        role: dto.role,
        actorRole: normalizeAdminRole(req.user.role),
      });
      await this.safeRecordAdminEvent(req.user, {
        action: 'role_update',
        status: 'success',
        target: result.email || targetEmail,
        details: `Role set to ${dto.role}`,
      });
      return result;
    } catch (error) {
      await this.safeRecordAdminEvent(req.user, {
        action: 'role_update',
        status: 'error',
        target: targetEmail || dto.email,
        details: `Failed setting role to ${dto.role}: ${this.resolveErrorMessage(error)}`,
      });
      throw error;
    }
  }

  @Get('admin/permissions')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  getAdminPermissions(@Req() req: { user: AuthenticatedUser }) {
    this.ensurePermission(req.user, 'permissions.read');
    return {
      role: normalizeAdminRole(req.user.role),
      permissions: getAdminPermissionMatrix(req.user.role),
    };
  }

  @Get('admin/users/:userId/details')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async getAdminUserDetails(
    @Req() req: { user: AuthenticatedUser },
    @Param('userId') userId: string,
    @Query() query: AdminUserDetailsQueryDto,
  ) {
    this.ensurePermission(req.user, 'users.read');
    await this.assertCanManageTargetById(req.user, userId);
    return this.authService.getAdminUserDetails({
      targetUserId: userId,
      txLimit: query.txLimit,
    });
  }

  @Post('admin/users/:userId/suspend')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async suspendUser(
    @Req() req: { user: AuthenticatedUser },
    @Param('userId') userId: string,
    @Body() dto: AdminSuspendUserDto,
  ) {
    this.ensurePermission(req.user, 'users.suspend');
    await this.assertCanManageTargetById(req.user, userId);
    try {
      const result = await this.authService.setUserSuspension({
        targetUserId: userId,
        suspended: true,
        reason: dto.reason,
        updatedByUserId: req.user.userId,
        forceLogout: true,
      });
      await this.safeRecordAdminEvent(req.user, {
        action: 'account_suspend',
        status: 'success',
        target: result.email || result.userId,
        details: `Suspended account. Revoked sessions: ${result.revokedRefreshTokens}`,
      });
      return result;
    } catch (error) {
      await this.safeRecordAdminEvent(req.user, {
        action: 'account_suspend',
        status: 'error',
        target: userId,
        details: `Failed to suspend account: ${this.resolveErrorMessage(error)}`,
      });
      throw error;
    }
  }

  @Post('admin/users/:userId/unsuspend')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async unsuspendUser(
    @Req() req: { user: AuthenticatedUser },
    @Param('userId') userId: string,
  ) {
    this.ensurePermission(req.user, 'users.suspend');
    await this.assertCanManageTargetById(req.user, userId);
    try {
      const result = await this.authService.setUserSuspension({
        targetUserId: userId,
        suspended: false,
        updatedByUserId: req.user.userId,
        forceLogout: false,
      });
      await this.safeRecordAdminEvent(req.user, {
        action: 'account_unsuspend',
        status: 'success',
        target: result.email || result.userId,
        details: 'Unsuspended account',
      });
      return result;
    } catch (error) {
      await this.safeRecordAdminEvent(req.user, {
        action: 'account_unsuspend',
        status: 'error',
        target: userId,
        details: `Failed to unsuspend account: ${this.resolveErrorMessage(error)}`,
      });
      throw error;
    }
  }

  @Post('admin/users/:userId/revoke-sessions')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async revokeUserSessions(
    @Req() req: { user: AuthenticatedUser },
    @Param('userId') userId: string,
  ) {
    this.ensurePermission(req.user, 'users.session_revoke');
    await this.assertCanManageTargetById(req.user, userId);
    try {
      const result = await this.authService.revokeUserSessions({ targetUserId: userId });
      await this.safeRecordAdminEvent(req.user, {
        action: 'session_revoke',
        status: 'success',
        target: userId,
        details: `Revoked sessions: ${result.revokedRefreshTokens}`,
      });
      return result;
    } catch (error) {
      await this.safeRecordAdminEvent(req.user, {
        action: 'session_revoke',
        status: 'error',
        target: userId,
        details: `Failed to revoke sessions: ${this.resolveErrorMessage(error)}`,
      });
      throw error;
    }
  }

  @Post('admin/users/:userId/password-reset')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async triggerPasswordResetByAdmin(
    @Req() req: { user: AuthenticatedUser },
    @Param('userId') userId: string,
  ) {
    this.ensurePermission(req.user, 'users.password_reset');
    await this.assertCanManageTargetById(req.user, userId);
    try {
      const result = await this.authService.triggerAdminPasswordReset({
        targetUserId: userId,
        requestedByUserId: req.user.userId,
        invalidateSessions: true,
      });
      await this.safeRecordAdminEvent(req.user, {
        action: 'password_reset_trigger',
        status: 'success',
        target: result.email || result.userId,
        details:
          `Triggered password reset. Invalidated sessions: ${result.invalidatedSessions}. ` +
          `Revoked refresh tokens: ${result.revokedRefreshTokens}`,
      });
      return result;
    } catch (error) {
      await this.safeRecordAdminEvent(req.user, {
        action: 'password_reset_trigger',
        status: 'error',
        target: userId,
        details: `Failed to trigger password reset: ${this.resolveErrorMessage(error)}`,
      });
      throw error;
    }
  }

  @Post('admin/users/:userId/2fa/reset')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  async resetUserTwoFactor(
    @Req() req: { user: AuthenticatedUser },
    @Param('userId') userId: string,
  ) {
    this.ensurePermission(req.user, 'users.two_factor_reset');
    await this.assertCanManageTargetById(req.user, userId);
    try {
      const result = await this.authService.resetUserTwoFactor({
        targetUserId: userId,
        resetByUserId: req.user.userId,
        revokeSessions: true,
      });
      await this.safeRecordAdminEvent(req.user, {
        action: 'two_factor_reset',
        status: 'success',
        target: userId,
        details: `2FA reset completed. Revoked sessions: ${result.revokedRefreshTokens}`,
      });
      return result;
    } catch (error) {
      await this.safeRecordAdminEvent(req.user, {
        action: 'two_factor_reset',
        status: 'error',
        target: userId,
        details: `Failed to reset 2FA: ${this.resolveErrorMessage(error)}`,
      });
      throw error;
    }
  }

  @Get('wallet/challenge')
  getWalletChallenge(@Query() dto: WalletChallengeDto) {
    return this.authService.generateWalletChallenge(dto);
  }

  @Post('wallet/login')
  loginWithWallet(@Body() dto: WalletLoginDto, @Req() req: Request) {
    return this.authService.authenticateWithWallet(dto, req.ip);
  }

  @Post('wallet/link')
  @UseGuards(JwtAuthGuard)
  @ApiBearerAuth()
  linkWalletToCurrentUser(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: WalletLoginDto,
  ) {
    return this.authService.linkWalletForUser(req.user.userId, dto);
  }

  private ensurePermission(
    user: AuthenticatedUser,
    permission: AdminPermissionKey,
  ): void {
    if (hasAdminPermission(user.role, permission)) {
      return;
    }
    throw new ForbiddenException(`Admin permission required: ${permission}`);
  }

  private async assertCanManageTargetById(
    actor: AuthenticatedUser,
    targetUserId: string,
  ): Promise<void> {
    const target = await this.authService.getAdminUserSnapshot(targetUserId);
    this.assertCanManageTargetRole(actor, target.role);
  }

  private assertCanManageTargetRole(
    actor: AuthenticatedUser,
    targetRole: 'user' | 'admin' | 'owner',
  ): void {
    const actorRole = normalizeAdminRole(actor.role);
    if (actorRole === 'owner') {
      return;
    }
    if (targetRole === 'owner') {
      throw new ForbiddenException('Only owner can manage owner accounts');
    }
  }

  private async safeRecordAdminEvent(
    user: AuthenticatedUser,
    payload: {
      action: string;
      status: 'success' | 'error' | 'partial';
      target: string;
      details: string;
    },
  ): Promise<void> {
    try {
      await this.notificationsService.recordAdminAuditEvent({
        ...payload,
        actorUserId: user.userId,
        actorEmail: user.email,
      });
    } catch {
      // Keep admin action response successful even if audit persistence fails.
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
}
