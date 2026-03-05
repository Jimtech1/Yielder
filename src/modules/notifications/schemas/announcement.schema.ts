import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type AnnouncementDocument = Announcement & Document;
export type AnnouncementStatus = 'active' | 'archived';

@Schema({ timestamps: true })
export class Announcement {
  @Prop({ required: true, trim: true })
  title: string;

  @Prop({ required: true, trim: true })
  message: string;

  @Prop({ required: true, enum: ['active', 'archived'], default: 'active', index: true })
  status: AnnouncementStatus;

  @Prop({ required: true, index: true })
  createdByUserId: string;

  @Prop()
  createdByEmail?: string;
}

export const AnnouncementSchema = SchemaFactory.createForClass(Announcement);

AnnouncementSchema.index({ status: 1, createdAt: -1 });
