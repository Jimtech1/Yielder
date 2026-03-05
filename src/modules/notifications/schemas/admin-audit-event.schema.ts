import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type AdminAuditEventDocument = AdminAuditEvent & Document;
export type AdminAuditStatus = 'success' | 'error' | 'partial';

@Schema({ timestamps: true })
export class AdminAuditEvent {
  @Prop({ required: true, trim: true, index: true })
  action: string;

  @Prop({ required: true, enum: ['success', 'error', 'partial'], index: true })
  status: AdminAuditStatus;

  @Prop({ required: true, trim: true })
  target: string;

  @Prop({ required: true, trim: true })
  details: string;

  @Prop({ required: true, index: true })
  actorUserId: string;

  @Prop()
  actorEmail?: string;
}

export const AdminAuditEventSchema = SchemaFactory.createForClass(AdminAuditEvent);

AdminAuditEventSchema.index({ createdAt: -1 });
AdminAuditEventSchema.index({ action: 1, createdAt: -1 });
AdminAuditEventSchema.index({ status: 1, createdAt: -1 });
