
import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type IndexerStateDocument = IndexerState & Document;

@Schema({ timestamps: true })
export class IndexerState {
  @Prop({ required: true, unique: true })
  chain: string;

  @Prop({ required: true })
  lastBlock: number;

  @Prop()
  lastLedger: number;

  @Prop({ required: true })
  lastSync: Date;

  @Prop({ default: 'active' })
  status: string;
}

export const IndexerStateSchema = SchemaFactory.createForClass(IndexerState);
