import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Schema as MongooseSchema } from 'mongoose';
import { Wallet } from '../../wallet/schemas/wallet.schema';

export type DeFiPositionDocument = DeFiPosition & Document;

@Schema({ timestamps: true })
export class DeFiPosition {
  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'Wallet', required: true, index: true })
  walletId: Wallet;

  @Prop({ required: true })
  protocol: string; // 'aquarius', 'stellarx', 'blend'

  @Prop({ required: true })
  type: string; // 'liquidity_pool', 'lending', 'staking'

  @Prop({ required: true })
  assetId: string; // ID of the pool or token pair (e.g. 'AQUA:XLM')

  @Prop({ type: Object })
  principal: any; // Initial investment { amount: string, asset: string }[]

  @Prop({ type: Object })
  currentValue: any; // { amount: string, asset: string }[] - breakdown of claimable tokens

  @Prop()
  apy: number; // Current APY snapshot

  @Prop()
  unclaimedRewards: string;

  @Prop({ default: 'active' })
  status: string; // 'active', 'closed'
}

export const DeFiPositionSchema = SchemaFactory.createForClass(DeFiPosition);

DeFiPositionSchema.index({ walletId: 1, protocol: 1 });
