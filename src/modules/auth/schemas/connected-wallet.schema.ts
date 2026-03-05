import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Schema as MongooseSchema } from 'mongoose';
import { User } from './user.schema';

export type ConnectedWalletDocument = ConnectedWallet & Document;

@Schema({ timestamps: true })
export class ConnectedWallet {
  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'User', required: true, index: true })
  userId: User;

  @Prop({ required: true, unique: true })
  publicKey: string;

  @Prop({ required: true })
  walletType: string; // 'freighter', 'albedo', etc.

  @Prop({ default: false })
  isPrimary: boolean;

  @Prop()
  label: string;

  @Prop({ default: Date.now })
  lastUsedAt: Date;
}

export const ConnectedWalletSchema = SchemaFactory.createForClass(ConnectedWallet);
