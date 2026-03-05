import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import {
  PlatformFeeRecord,
  PlatformFeeRecordSchema,
} from './schemas/platform-fee-record.schema';
import { PlatformFeeService } from './platform-fee.service';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: PlatformFeeRecord.name, schema: PlatformFeeRecordSchema },
    ]),
  ],
  providers: [PlatformFeeService],
  exports: [PlatformFeeService],
})
export class PlatformFeeModule {}
