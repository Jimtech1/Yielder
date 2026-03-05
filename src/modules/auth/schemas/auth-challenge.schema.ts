import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type AuthChallengeDocument = AuthChallenge & Document;

@Schema({ timestamps: true })
export class AuthChallenge {
  @Prop({ required: true, index: true })
  publicKey: string;

  @Prop({ required: true, unique: true })
  challenge: string;

  @Prop({ required: true })
  expiresAt: Date;

  @Prop({ default: false })
  used: boolean;
}

export const AuthChallengeSchema = SchemaFactory.createForClass(AuthChallenge);
AuthChallengeSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 }); // TTL Index
