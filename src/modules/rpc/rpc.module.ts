import { Module } from '@nestjs/common';
import { RpcService } from './rpc.service';
import { AxelarService } from './axelar.service';
import { RpcController } from './rpc.controller';
import { AccessModule } from '../access/access.module';
import { NotificationsModule } from '../notifications/notifications.module';

@Module({
  imports: [AccessModule, NotificationsModule],
  controllers: [RpcController],
  providers: [RpcService, AxelarService],
  exports: [RpcService, AxelarService],
})
export class RpcModule {}
