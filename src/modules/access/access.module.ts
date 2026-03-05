import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MongooseModule } from '@nestjs/mongoose';
import { AccessControlService } from './access-control.service';
import { User, UserSchema } from '../auth/schemas/user.schema';

@Module({
  imports: [
    ConfigModule,
    MongooseModule.forFeature([{ name: User.name, schema: UserSchema }]),
  ],
  providers: [AccessControlService],
  exports: [AccessControlService],
})
export class AccessModule {}
