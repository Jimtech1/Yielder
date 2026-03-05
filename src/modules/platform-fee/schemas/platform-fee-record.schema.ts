import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type PlatformFeeAction =
  | 'swap'
  | 'bridge'
  | 'claim_rewards'
  | 'deposit'
  | 'withdrawal';

export type PlatformFeeStatus = 'accrued' | 'collected';

export type PlatformFeeRecordDocument = PlatformFeeRecord & Document;

@Schema({ timestamps: true })
export class PlatformFeeRecord {
  @Prop({ required: true, index: true })
  userId: string;

  @Prop({
    required: true,
    index: true,
    enum: ['swap', 'bridge', 'claim_rewards', 'deposit', 'withdrawal'],
  })
  action: PlatformFeeAction;

  @Prop({ required: true, index: true })
  chainKey: string;

  @Prop({ required: true })
  payerAddress: string;

  @Prop({ required: true })
  collectorAddress: string;

  @Prop({ required: true })
  asset: string;

  @Prop({ required: true })
  assetSymbol: string;

  @Prop({ required: true })
  amount: string;

  @Prop({ required: true })
  feeBps: number;

  @Prop({ required: true })
  feeAmount: string;

  @Prop({ required: true, default: 'accrued', index: true, enum: ['accrued', 'collected'] })
  status: PlatformFeeStatus;

  @Prop()
  reference?: string;

  @Prop()
  collectedAt?: Date;

  @Prop()
  collectedBy?: string;

  @Prop()
  collectionTxHash?: string;

  @Prop({ type: Object })
  metadata?: Record<string, unknown>;
}

export const PlatformFeeRecordSchema = SchemaFactory.createForClass(PlatformFeeRecord);

PlatformFeeRecordSchema.index({ createdAt: -1 });
PlatformFeeRecordSchema.index({ action: 1, status: 1, createdAt: -1 });
PlatformFeeRecordSchema.index({ userId: 1, action: 1, createdAt: -1 });
PlatformFeeRecordSchema.index({ reference: 1 }, { unique: true, sparse: true });
