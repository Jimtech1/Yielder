import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import * as crypto from 'crypto';
import { AuthChallenge, AuthChallengeDocument } from '../schemas/auth-challenge.schema';

@Injectable()
export class ChallengeService {
  constructor(
    @InjectModel(AuthChallenge.name)
    private challengeModel: Model<AuthChallengeDocument>,
  ) {}

  async generateChallenge(publicKey: string): Promise<{ challenge: string; expiresAt: Date }> {
    const nonce = crypto.randomBytes(32).toString('hex');
    const expiresAt = new Date(Date.now() + 5 * 60 * 1000); // 5 minutes

    const challengeMessage = `Sign this message to authenticate with Yielder Portfolio
  
Public Key: ${publicKey}
Timestamp: ${new Date().toISOString()}
Nonce: ${nonce}
Domain: yielder.io
  
This request will not trigger a blockchain transaction or cost any fees.`;

    await this.challengeModel.create({
      publicKey,
      challenge: challengeMessage,
      expiresAt,
    });

    return { challenge: challengeMessage, expiresAt };
  }

  async validateChallenge(publicKey: string, challenge: string): Promise<boolean> {
    const record = await this.challengeModel.findOne({ publicKey, challenge });
    
    if (!record) {
      return false;
    }

    if (record.used) {
        return false; // Replay protection
    }
    
    if (record.expiresAt < new Date()) {
        return false; // Expired
    }

    // Mark as used
    record.used = true;
    await record.save();

    return true;
  }

  async getLastChallengeMessage(publicKey: string): Promise<string | null> {
    const record = await this.challengeModel.findOne({ 
      publicKey, 
      expiresAt: { $gt: new Date() },
      used: false 
    }).sort({ createdAt: -1 });
    
    return record ? record.challenge : null;
  }

  async consumeChallenge(publicKey: string, challenge: string): Promise<boolean> {
    const result = await this.challengeModel.updateOne(
      {
        publicKey,
        challenge,
        used: false,
        expiresAt: { $gt: new Date() },
      },
      { used: true },
    );

    return result.modifiedCount > 0;
  }
}
