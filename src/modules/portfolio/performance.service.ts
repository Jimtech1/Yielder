import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { PortfolioSnapshot, PortfolioSnapshotDocument } from './schemas/portfolio-snapshot.schema';
import { Transaction, TransactionDocument } from '../wallet/schemas/transaction.schema';

@Injectable()
export class PerformanceService {
  constructor(
    @InjectModel(PortfolioSnapshot.name) private snapshotModel: Model<PortfolioSnapshotDocument>,
    @InjectModel(Transaction.name) private transactionModel: Model<TransactionDocument>,
  ) {}

  async getPortfolioPerformance(userId: string, days = 30) {
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(endDate.getDate() - days);

    // 1. Get Starting Snapshot
    // Find closest snapshot equal to or before start date
    const startSnapshot = await this.snapshotModel.findOne({
      userId,
      timestamp: { $lte: startDate }
    } as any).sort({ timestamp: -1 }).exec();

    // 2. Get Current Snapshot (or closest to end date)
    const endSnapshot = await this.snapshotModel.findOne({
      userId,
      timestamp: { $lte: endDate }
    } as any).sort({ timestamp: -1 }).exec();

    if (!startSnapshot || !endSnapshot) {
        return {
            period: `${days}d`,
            absPnL: 0,
            roi: 0,
            note: 'Insufficient history'
        };
    }

    const startValue = startSnapshot.totalValue;
    const endValue = endSnapshot.totalValue;

    // 3. Calculate Net Flows (Deposits - Withdrawals) during the period
    // We assume 'receive' is positive flow (Deposit) and 'send' is negative flow (Withdrawal)
    // This is a simplification.
    /*
    const transactions = await this.transactionModel.find({
        // userId needs to be mapped to walletIds, assume we have walletIds or do aggregation
        // timestamp: { $gte: startSnapshot.timestamp, $lte: endSnapshot.timestamp }
    });
    */
    // For V1, we will skip Net Flow adjustment to keep it robust against missing transaction types
    // and focus on pure Value Change (Equity Curve).
    
    const pnl = endValue - startValue;
    const roi = startValue > 0 ? (pnl / startValue) * 100 : 0;

    return {
      period: `${days}d`,
      startValue,
      endValue,
      absPnL: pnl,
      roi: parseFloat(roi.toFixed(2))
    };
  }
}
