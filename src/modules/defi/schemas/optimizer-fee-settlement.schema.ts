import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type OptimizerFeeSettlementDocument = OptimizerFeeSettlement & Document;

@Schema({ timestamps: true })
export class OptimizerFeeSettlement {
  @Prop({ required: true, index: true })
  userId: string;

  @Prop({ required: true, index: true })
  optimizerExecutionId: string;

  @Prop({ required: true })
  chainKey: string;

  @Prop({ required: true })
  assetSymbol: string;

  @Prop({ required: true })
  payerAddress: string;

  @Prop({ required: true })
  collectorAddress: string;

  @Prop({ required: true })
  realizedProfitAmount: string;

  @Prop({ required: true })
  performanceFeeBps: number;

  @Prop({ required: true })
  feeAmount: string;

  @Prop({ required: true, default: 'wallet', enum: ['wallet', 'backend'] })
  settlementMode: 'wallet' | 'backend';

  @Prop({ required: true, default: 'wallet-action-required', index: true })
  status: 'wallet-action-required' | 'submitted' | 'confirmed' | 'failed';

  @Prop()
  txHash?: string;

  @Prop({ type: Object })
  txPayload?: Record<string, unknown>;

  @Prop()
  error?: string;

  @Prop({ type: Object })
  metadata?: Record<string, unknown>;
}

export const OptimizerFeeSettlementSchema = SchemaFactory.createForClass(OptimizerFeeSettlement);

OptimizerFeeSettlementSchema.index({ userId: 1, createdAt: -1 });
OptimizerFeeSettlementSchema.index({ userId: 1, optimizerExecutionId: 1, createdAt: -1 });
