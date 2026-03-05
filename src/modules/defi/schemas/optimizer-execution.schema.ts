import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type OptimizerExecutionDocument = OptimizerExecution & Document;

@Schema({ timestamps: true })
export class OptimizerExecution {
  @Prop({ required: true, index: true })
  userId: string;

  @Prop({ required: true })
  strategyName: string;

  @Prop({ required: true })
  assetSymbol: string;

  @Prop({ required: true })
  amount: string;

  @Prop({ required: true })
  sourceChainKey: string;

  @Prop({ required: true })
  destinationChainKey: string;

  @Prop()
  sourceAddress?: string;

  @Prop()
  destinationAddress?: string;

  @Prop({ required: true })
  protocol: string;

  @Prop({ required: true })
  category: string;

  @Prop({ required: true })
  baselineApy: number;

  @Prop({ required: true })
  optimizedApy: number;

  @Prop({ required: true })
  netApy: number;

  @Prop({ required: true })
  performanceFeeBps: number;

  @Prop({ required: true })
  estimatedAnnualFee: number;

  @Prop({ required: true })
  estimatedAnnualNetYield: number;

  @Prop()
  route?: string;

  @Prop()
  bridgeExecutionType?: string;

  @Prop()
  bridgeTxHash?: string;

  @Prop()
  approvalTxHash?: string;

  @Prop()
  externalUrl?: string;

  @Prop()
  positionId?: string;

  @Prop({ required: true, default: 'planned', index: true })
  status:
    | 'planned'
    | 'bridge-submitted'
    | 'bridge-external'
    | 'awaiting-bridge-finality'
    | 'deposit-pending'
    | 'wallet-action-required'
    | 'position-opened'
    | 'failed';

  @Prop()
  lastError?: string;

  @Prop({ type: [Object], default: [] })
  flow: Array<{ index: number; title: string; detail: string }>;

  @Prop({ type: Object })
  metadata?: Record<string, unknown>;
}

export const OptimizerExecutionSchema = SchemaFactory.createForClass(OptimizerExecution);

OptimizerExecutionSchema.index({ userId: 1, createdAt: -1 });
OptimizerExecutionSchema.index({ userId: 1, strategyName: 1, createdAt: -1 });
