import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Schema as MongooseSchema } from 'mongoose';
import { Wallet } from './wallet.schema';

export type TransactionDocument = Transaction & Document;

@Schema({ timestamps: true })
export class Transaction {
  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'Wallet', required: true, index: true })
  walletId: Wallet;

  @Prop({ required: true, index: true })
  hash: string;

  @Prop({ required: true })
  chain: string; // 'stellar', 'axelar', 'soroban'

  @Prop({ required: true })
  type: string; // 'transfer', 'swap', 'defi_deposit', etc.

  @Prop()
  from: string;

  @Prop()
  to: string;

  @Prop()
  amount: string; // Store as string for precision

  @Prop()
  asset: string; // 'XLM', 'USDC:G...', etc.

  @Prop()
  fee: string;

  @Prop()
  status: string; // 'confirmed', 'failed', 'pending'

  @Prop()
  blockNumber: number;

  @Prop()
  timestamp: Date;

  @Prop({ type: Object })
  metadata: any; // Flexible storage for chain-specific data (e.g. memo, contract function)
}

export const TransactionSchema = SchemaFactory.createForClass(Transaction);

// compound index for efficient lookups of specific tx
TransactionSchema.index({ chain: 1, hash: 1 }, { unique: true });
