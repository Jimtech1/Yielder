import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { IndexerService } from './indexer.service';
import { StellarIndexerService } from './stellar-indexer.service';
import { SorobanIndexerService } from './soroban-indexer.service';
import { WalletModule } from '../wallet/wallet.module';
import { IndexerState, IndexerStateSchema } from './schemas/indexer-state.schema';
import { Wallet, WalletSchema } from '../wallet/schemas/wallet.schema';
import { Transaction, TransactionSchema } from '../wallet/schemas/transaction.schema';
// Removed incorrect SharedModule import if it doesn't exist, will check.

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: IndexerState.name, schema: IndexerStateSchema },
      { name: Wallet.name, schema: WalletSchema },
      { name: Transaction.name, schema: TransactionSchema }, // Added Transaction schema
    ]),
    WalletModule, // Reordered WalletModule
  ],
  providers: [IndexerService, StellarIndexerService, SorobanIndexerService],
  exports: [IndexerService, StellarIndexerService, SorobanIndexerService],
})
export class IndexerModule {}