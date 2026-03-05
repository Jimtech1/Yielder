import { Controller, Get, Sse } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { MessageEvent } from '@nestjs/common/interfaces/http/message-event.interface';
import { Observable } from 'rxjs';
import { RealtimeService } from './realtime.service';

@ApiTags('realtime')
@Controller('realtime')
export class RealtimeController {
  constructor(private readonly realtimeService: RealtimeService) {}

  @Sse('events')
  events(): Observable<MessageEvent> {
    return this.realtimeService.stream();
  }

  @Get('health')
  health() {
    return this.realtimeService.getMetadata();
  }
}
