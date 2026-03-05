import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Schema as MongooseSchema } from 'mongoose';
import { User } from '../../auth/schemas/user.schema';

export type PortfolioSnapshotDocument = PortfolioSnapshot & Document;

@Schema({ timestamps: true })
export class PortfolioSnapshot {
  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'User', required: true, index: true })
  userId: User;

  @Prop({ required: true })
  totalValue: number;

  @Prop({ type: Object })
  breakdown: any; // Store the asset breakdown at this point in time

  @Prop({ required: true, index: true })
  timestamp: Date;
}

export const PortfolioSnapshotSchema = SchemaFactory.createForClass(PortfolioSnapshot);

// Ensure one snapshot per day per user (optional, but good practice). 
// Or just index to sort by date.
PortfolioSnapshotSchema.index({ userId: 1, timestamp: -1 });
