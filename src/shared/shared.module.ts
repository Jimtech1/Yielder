
import { Module, Global } from '@nestjs/common';
import { PinoLoggerService } from './logger';

@Global()
@Module({
  providers: [PinoLoggerService],
  exports: [PinoLoggerService],
})
export class SharedModule {}
