import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type AdminFeatureFlagsDocument = AdminFeatureFlags & Document;

@Schema({ timestamps: true })
export class AdminFeatureFlags {
  @Prop({ required: true, unique: true, index: true })
  userId: string;

  @Prop({ required: true, default: true })
  autoRefreshUsers: boolean;

  @Prop({ required: true, default: true })
  requireBulkConfirmation: boolean;

  @Prop({ required: true, default: false })
  compactUserRows: boolean;
}

export const AdminFeatureFlagsSchema = SchemaFactory.createForClass(AdminFeatureFlags);
