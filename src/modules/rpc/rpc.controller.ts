import { Body, Controller, Get, Param, Post, Req, Sse, UseGuards } from '@nestjs/common';
import { ApiBearerAuth, ApiTags } from '@nestjs/swagger';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { AuthenticatedUser } from '../auth/interfaces/authenticated-user.interface';
import { RpcService } from './rpc.service';
import { BroadcastTransactionDto } from './dto/broadcast-transaction.dto';
import { ChainCallDto } from './dto/chain-call.dto';
import { BatchChainCallDto } from './dto/batch-chain-call.dto';
import { MessageEvent } from '@nestjs/common/interfaces/http/message-event.interface';
import { Observable } from 'rxjs';
import { AccessControlService } from '../access/access-control.service';
import { NotificationsService } from '../notifications/notifications.service';

@ApiTags('rpc')
@Controller('rpc')
@UseGuards(JwtAuthGuard)
@ApiBearerAuth()
export class RpcController {
  constructor(
    private readonly rpcService: RpcService,
    private readonly accessControlService: AccessControlService,
    private readonly notificationsService: NotificationsService,
  ) {}

  @Post('broadcast')
  async broadcast(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: BroadcastTransactionDto,
  ) {
    await this.accessControlService.consumeApiQuota(req.user.userId, 1);
    const result = await this.rpcService.broadcastTransaction(dto.transaction, dto.network);
    await this.safeRecordOptionalAdminEvent(req.user, {
      action: 'rpc_broadcast',
      status: 'success',
      target: dto.network || 'stellar',
      details: `Broadcast hash ${result.hash}`,
    });
    return result;
  }

  @Post('call/:chain')
  async call(
    @Req() req: { user: AuthenticatedUser },
    @Param('chain') chain: string,
    @Body() dto: ChainCallDto,
  ) {
    await this.accessControlService.consumeApiQuota(req.user.userId, 1);
    const result = await this.rpcService.executeChainCall(chain, dto.method, dto.params || []);
    await this.safeRecordOptionalAdminEvent(req.user, {
      action: 'rpc_call',
      status: 'success',
      target: chain,
      details: `Executed ${dto.method}`,
    });
    return result;
  }

  @Post('batch/:chain')
  async batch(
    @Req() req: { user: AuthenticatedUser },
    @Param('chain') chain: string,
    @Body() dto: BatchChainCallDto,
  ) {
    await this.accessControlService.consumeApiQuota(
      req.user.userId,
      Math.max(Array.isArray(dto.calls) ? dto.calls.length : 0, 1),
    );
    const result = await this.rpcService.executeBatchChainCalls(chain, dto.calls);
    await this.safeRecordOptionalAdminEvent(req.user, {
      action: 'rpc_batch_call',
      status: 'success',
      target: chain,
      details: `Executed ${Array.isArray(dto.calls) ? dto.calls.length : 0} calls`,
    });
    return result;
  }

  @Get('status/:chain')
  async getStatus(
    @Req() req: { user: AuthenticatedUser },
    @Param('chain') chain: string,
  ) {
    await this.accessControlService.consumeApiQuota(req.user.userId, 1);
    const result = await this.rpcService.getChainStatus(chain);
    await this.safeRecordOptionalAdminEvent(req.user, {
      action: 'rpc_status',
      status: 'success',
      target: chain,
      details: `Chain status ${result.status}`,
    });
    return result;
  }

  @Get('metrics')
  async getMetrics(@Req() req: { user: AuthenticatedUser }) {
    await this.accessControlService.consumeApiQuota(req.user.userId, 1);
    const result = this.rpcService.getMetrics();
    await this.safeRecordOptionalAdminEvent(req.user, {
      action: 'rpc_metrics',
      status: 'success',
      target: 'rpc-metrics',
      details: `RPC calls ${result.totalCalls}, failures ${result.failedCalls}`,
    });
    return result;
  }

  @Sse('events/:chain')
  async stream(
    @Req() req: { user: AuthenticatedUser },
    @Param('chain') chain: string,
  ): Promise<Observable<MessageEvent>> {
    await this.accessControlService.consumeApiQuota(req.user.userId, 1);
    await this.safeRecordOptionalAdminEvent(req.user, {
      action: 'rpc_stream',
      status: 'success',
      target: chain,
      details: 'Subscribed to chain head stream',
    });
    return this.rpcService.streamChainHeads(chain);
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
      // Do not block RPC flow if audit logging fails.
    }
  }
}
