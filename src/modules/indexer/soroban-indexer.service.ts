import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import * as StellarSdk from 'stellar-sdk';
import { PinoLoggerService } from '../../shared/logger';
import { Transaction, TransactionDocument } from '../wallet/schemas/transaction.schema';

@Injectable()
export class SorobanIndexerService {
  constructor(
    @InjectModel(Transaction.name) private transactionModel: Model<TransactionDocument>,
    private logger: PinoLoggerService,
  ) {}

  async decodeContractEvent(xdr: string): Promise<any> {
    if (!xdr || !xdr.trim()) {
      return { decoded: false, reason: 'empty_xdr' };
    }

    try {
      StellarSdk.xdr.TransactionMeta.fromXDR(xdr, 'base64');
      return { decoded: false, reason: 'event_extraction_unavailable' };
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : 'invalid_xdr';
      this.logger.warn(`Soroban meta decode failed: ${message}`);
      return { decoded: false, reason: 'invalid_xdr' };
    }
  }
}
