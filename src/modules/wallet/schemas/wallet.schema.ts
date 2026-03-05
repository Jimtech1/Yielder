import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Schema as MongooseSchema } from 'mongoose';
import { User } from '../../auth/schemas/user.schema';

export type WalletDocument = Wallet & Document;

@Schema({ _id: false })
export class Balance {
  @Prop({ required: true })
  asset: string;

  @Prop({ required: true, default: 'native' })
  assetType: string; // 'native', 'credit', 'liquidity_pool'

  @Prop({ required: true })
  amount: string; // Stored as string for precision

  @Prop()
  lockedAmount: string;
}

export const BalanceSchema = SchemaFactory.createForClass(Balance);

@Schema({ timestamps: true })
export class Wallet {
  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'User', required: true, index: true })
  userId: User;

  @Prop({ required: true, default: 'stellar' })
  chain: string;

  @Prop({ required: true, index: true })
  address: string;

  @Prop()
  publicKey: string; // Optional, might differ from address in some chains

  @Prop()
  label: string;

  @Prop({ default: false })
  isWatchOnly: boolean;

  @Prop({ default: false })
  isArchived: boolean;

  @Prop({ type: MongooseSchema.Types.Mixed })
  metadata: any;

  @Prop({ type: [BalanceSchema], default: [] })
  balances: Balance[];
}

export const WalletSchema = SchemaFactory.createForClass(Wallet);
WalletSchema.index({ userId: 1, chain: 1, address: 1 }, { unique: true });
