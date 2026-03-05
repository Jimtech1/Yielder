import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type BridgeHistoryDocument = BridgeHistory & Document;

@Schema({ timestamps: true })
export class BridgeHistory {
  @Prop({ required: true, index: true })
  userId: string;

  @Prop({ required: true, default: 'stargate' })
  provider: string;

  @Prop({ required: true })
  srcChainKey: string;

  @Prop({ required: true })
  dstChainKey: string;

  @Prop({ required: true })
  srcAddress: string;

  @Prop({ required: true })
  dstAddress: string;

  @Prop({ required: true })
  srcTokenSymbol: string;

  @Prop({ required: true })
  dstTokenSymbol: string;

  @Prop({ required: true })
  srcAmount: string;

  @Prop()
  dstAmount?: string;

  @Prop()
  dstAmountMin?: string;

  @Prop()
  route?: string;

  @Prop()
  approvalTxHash?: string;

  @Prop({ required: true, index: true })
  bridgeTxHash: string;

  @Prop({ default: 'submitted' })
  status: string;

  @Prop()
  estimatedDurationSeconds?: number;

  @Prop()
  feeAmount?: string;

  @Prop()
  feeTokenSymbol?: string;

  @Prop({ type: Object })
  metadata?: Record<string, unknown>;
}

export const BridgeHistorySchema = SchemaFactory.createForClass(BridgeHistory);

BridgeHistorySchema.index({ userId: 1, createdAt: -1 });
BridgeHistorySchema.index({ userId: 1, bridgeTxHash: 1 }, { unique: true });
