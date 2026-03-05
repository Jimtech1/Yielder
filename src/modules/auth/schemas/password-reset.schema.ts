
import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Types } from 'mongoose';
import { User } from './user.schema';

export type PasswordResetDocument = PasswordReset & Document;

@Schema({ timestamps: true })
export class PasswordReset {
  @Prop({ type: Types.ObjectId, ref: 'User', required: true })
  user: User | Types.ObjectId;

  @Prop({ required: true, unique: true })
  token: string;

  @Prop({ required: true })
  expiresAt: Date;

  @Prop({ default: false })
  used: boolean;
}

export const PasswordResetSchema = SchemaFactory.createForClass(PasswordReset);
