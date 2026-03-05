import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Schema as MongooseSchema } from 'mongoose';
import { Wallet } from '../../wallet/schemas/wallet.schema';
import type { SubscriptionTier } from '../../access/subscription.types';

export type UserDocument = User & Document;
export type UserApiUsage = {
  period: string;
  used: number;
  lastRequestAt?: Date;
};

@Schema({ _id: false })
export class UserApiUsageEntry {
  @Prop({ required: true })
  period: string;

  @Prop({ required: true, default: 0 })
  used: number;

  @Prop()
  lastRequestAt?: Date;
}

export const UserApiUsageEntrySchema = SchemaFactory.createForClass(UserApiUsageEntry);

@Schema({ timestamps: true })
export class User {
  @Prop({ unique: true, sparse: true })
  email: string;

  @Prop()
  passwordHash: string;

  @Prop({ default: 'user' })
  role: string;

  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'Wallet' })
  defaultWallet: Wallet;

  @Prop({ default: 'wallet', enum: ['wallet', 'email', 'google'] })
  registrationType: string;

  @Prop({ default: false })
  emailVerified: boolean;

  @Prop({ default: false, index: true })
  isSuspended: boolean;

  @Prop()
  suspendedAt?: Date;

  @Prop()
  suspendedReason?: string;

  @Prop()
  suspendedBy?: string;

  @Prop()
  lastLoginAt: Date;

  @Prop()
  notificationLastReadAt: Date;

  @Prop({ default: 'free', enum: ['free', 'premium', 'enterprise'], index: true })
  subscriptionTier: SubscriptionTier;

  @Prop({ type: UserApiUsageEntrySchema })
  apiUsage?: UserApiUsageEntry;

  @Prop()
  subscriptionUpdatedAt?: Date;

  @Prop()
  subscriptionUpdatedBy?: string;

  @Prop({ default: 0 })
  tokenVersion: number;

  @Prop()
  sessionsRevokedAt?: Date;

  @Prop({ default: false })
  twoFactorEnabled: boolean;

  @Prop()
  twoFactorSecret?: string;

  @Prop()
  twoFactorResetAt?: Date;

  @Prop()
  twoFactorResetBy?: string;
}

export const UserSchema = SchemaFactory.createForClass(User);
