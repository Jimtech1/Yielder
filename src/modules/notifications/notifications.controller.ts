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
import { ApiBearerAuth, ApiTags } from '@nestjs/swagger';
import { AuthenticatedUser } from '../auth/interfaces/authenticated-user.interface';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { hasAdminPermission } from '../auth/admin-permissions';
import { NotificationsService } from './notifications.service';
import {
  CreateAdminAnnouncementDto,
  UpdateAdminAnnouncementStatusDto,
} from './dto/admin-announcement.dto';
import { UpdateAdminFeatureFlagsDto } from './dto/admin-feature-flags.dto';
import { CreateAdminAuditEventDto } from './dto/admin-audit-event.dto';
import type { AnnouncementStatus } from './schemas/announcement.schema';

@ApiTags('notifications')
@Controller('notifications')
@UseGuards(JwtAuthGuard)
@ApiBearerAuth()
export class NotificationsController {
  constructor(private readonly notificationsService: NotificationsService) {}

  @Get()
  getNotifications(
    @Req() req: { user: AuthenticatedUser },
    @Query('limit') limit?: string,
  ) {
    return this.notificationsService.getNotifications(req.user.userId, this.resolveLimit(limit));
  }

  @Post('read-all')
  async markAllAsRead(@Req() req: { user: AuthenticatedUser }) {
    const result = await this.notificationsService.markAllAsRead(req.user.userId);
    await this.safeRecordOptionalAdminEvent(req.user, {
      action: 'notification',
      status: 'success',
      target: 'notification-center',
      details: 'Marked all notifications as read',
    });
    return result;
  }

  @Get('admin/announcements')
  listAdminAnnouncements(
    @Req() req: { user: AuthenticatedUser },
    @Query('limit') limit?: string,
    @Query('status') status?: string,
  ) {
    this.ensureAdmin(req.user);
    this.ensurePermission(req.user, 'announcements.manage');
    return this.notificationsService.listAnnouncements(
      this.resolveAdminLimit(limit),
      this.resolveAnnouncementStatusFilter(status),
    );
  }

  @Post('admin/announcements')
  async createAdminAnnouncement(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: CreateAdminAnnouncementDto,
  ) {
    this.ensureAdmin(req.user);
    this.ensurePermission(req.user, 'announcements.manage');
    try {
      const result = await this.notificationsService.createAnnouncement({
        title: dto.title,
        message: dto.message,
        actorUserId: req.user.userId,
        actorEmail: req.user.email,
      });
      await this.safeRecordAdminEvent(req.user, {
        action: 'announcement',
        status: 'success',
        target: result.title,
        details: `Published announcement (${result.status})`,
      });
      return result;
    } catch (error) {
      await this.safeRecordAdminEvent(req.user, {
        action: 'announcement',
        status: 'error',
        target: dto.title || 'announcement',
        details: `Failed to publish announcement: ${this.resolveErrorMessage(error)}`,
      });
      throw error;
    }
  }

  @Post('admin/announcements/:announcementId/status')
  async updateAdminAnnouncementStatus(
    @Req() req: { user: AuthenticatedUser },
    @Param('announcementId') announcementId: string,
    @Body() dto: UpdateAdminAnnouncementStatusDto,
  ) {
    this.ensureAdmin(req.user);
    this.ensurePermission(req.user, 'announcements.manage');
    try {
      const result = await this.notificationsService.updateAnnouncementStatus({
        announcementId,
        status: dto.status,
      });
      await this.safeRecordAdminEvent(req.user, {
        action: 'announcement',
        status: 'success',
        target: result.title,
        details: `Announcement status set to ${result.status}`,
      });
      return result;
    } catch (error) {
      await this.safeRecordAdminEvent(req.user, {
        action: 'announcement',
        status: 'error',
        target: announcementId,
        details: `Failed to update announcement status: ${this.resolveErrorMessage(error)}`,
      });
      throw error;
    }
  }

  @Get('admin/audit-logs')
  listAdminAuditLogs(
    @Req() req: { user: AuthenticatedUser },
    @Query('limit') limit?: string,
    @Query('status') status?: string,
    @Query('action') action?: string,
  ) {
    this.ensureAdmin(req.user);
    this.ensurePermission(req.user, 'audit.read');
    return this.notificationsService.listAdminAuditEvents(this.resolveAuditLimit(limit), {
      status: this.resolveAuditStatus(status),
      action: typeof action === 'string' ? action : 'all',
    });
  }

  @Post('admin/audit-logs')
  async createAdminAuditLog(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: CreateAdminAuditEventDto,
  ) {
    this.ensureAdmin(req.user);
    this.ensurePermission(req.user, 'audit.read');
    return this.notificationsService.recordAdminAuditEvent({
      action: dto.action,
      status: dto.status,
      target: dto.target,
      details: dto.details,
      actorUserId: req.user.userId,
      actorEmail: req.user.email,
    });
  }

  @Get('admin/billing-activity')
  listAdminBillingActivity(
    @Req() req: { user: AuthenticatedUser },
    @Query('limit') limit?: string,
  ) {
    this.ensureAdmin(req.user);
    this.ensurePermission(req.user, 'billing.read');
    return this.notificationsService.listBillingActivity(this.resolveAuditLimit(limit));
  }

  @Get('admin/timeline')
  listAdminTimeline(
    @Req() req: { user: AuthenticatedUser },
    @Query('limit') limit?: string,
    @Query('module') module?: string,
  ) {
    this.ensureAdmin(req.user);
    this.ensurePermission(req.user, 'timeline.read');
    const normalizedModule = (module || 'all').trim().toLowerCase();
    const moduleFilter: 'all' | 'auth' | 'defi' | 'wallet' | 'rpc' | 'notifications' =
      normalizedModule === 'auth' ||
      normalizedModule === 'defi' ||
      normalizedModule === 'wallet' ||
      normalizedModule === 'rpc' ||
      normalizedModule === 'notifications'
        ? normalizedModule
        : 'all';
    return this.notificationsService.listAdminTimeline(this.resolveAuditLimit(limit), moduleFilter);
  }

  @Post('admin/audit-logs/clear')
  async clearAdminAuditLogs(@Req() req: { user: AuthenticatedUser }) {
    this.ensureAdmin(req.user);
    this.ensurePermission(req.user, 'audit.clear');
    return this.notificationsService.clearAdminAuditEvents();
  }

  @Get('admin/feature-flags')
  getAdminFeatureFlags(@Req() req: { user: AuthenticatedUser }) {
    this.ensureAdmin(req.user);
    this.ensurePermission(req.user, 'feature_flags.manage');
    return this.notificationsService.getAdminFeatureFlags(req.user.userId);
  }

  @Post('admin/feature-flags')
  async updateAdminFeatureFlags(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: UpdateAdminFeatureFlagsDto,
  ) {
    this.ensureAdmin(req.user);
    this.ensurePermission(req.user, 'feature_flags.manage');
    const result = await this.notificationsService.updateAdminFeatureFlags(req.user.userId, {
      autoRefreshUsers: dto.autoRefreshUsers,
      requireBulkConfirmation: dto.requireBulkConfirmation,
      compactUserRows: dto.compactUserRows,
    });
    await this.safeRecordAdminEvent(req.user, {
      action: 'feature_flags',
      status: 'success',
      target: 'admin-feature-flags',
      details:
        `Updated flags: autoRefreshUsers=${result.autoRefreshUsers}, ` +
        `requireBulkConfirmation=${result.requireBulkConfirmation}, ` +
        `compactUserRows=${result.compactUserRows}`,
    });
    return result;
  }

  private resolveLimit(limit?: string): number {
    const parsedLimit = Number.parseInt(limit || '', 10);
    if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) {
      return 15;
    }
    return Math.min(parsedLimit, 50);
  }

  private resolveAdminLimit(limit?: string): number {
    const parsedLimit = Number.parseInt(limit || '', 10);
    if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) {
      return 25;
    }
    return Math.min(parsedLimit, 100);
  }

  private resolveAnnouncementStatusFilter(value?: string): 'all' | AnnouncementStatus {
    const normalized = (value || 'all').trim().toLowerCase();
    if (normalized === 'active' || normalized === 'archived') {
      return normalized;
    }
    return 'all';
  }

  private resolveAuditLimit(limit?: string): number {
    const parsedLimit = Number.parseInt(limit || '', 10);
    if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) {
      return 200;
    }
    return Math.min(parsedLimit, 500);
  }

  private resolveAuditStatus(value?: string): 'all' | 'success' | 'error' | 'partial' {
    const normalized = (value || 'all').trim().toLowerCase();
    if (normalized === 'success' || normalized === 'error' || normalized === 'partial') {
      return normalized;
    }
    return 'all';
  }

  private ensureAdmin(user: AuthenticatedUser): void {
    const role = (user.role || '').trim().toLowerCase();
    if (role === 'owner' || role === 'admin') {
      return;
    }
    throw new ForbiddenException('Owner/Admin access is required');
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
      // Do not break primary request flow if audit recording fails.
    }
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
    await this.safeRecordAdminEvent(user, payload);
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
