import { Injectable, OnModuleInit } from '@nestjs/common';
import { StellarIndexerService } from './stellar-indexer.service';
import { PinoLoggerService } from '../../shared/logger';

@Injectable()
export class IndexerService implements OnModuleInit {
  private indexingInterval: NodeJS.Timeout;
  private isIndexing = false;
  private readonly indexingTickMs = this.resolveIndexingTickMs(process.env.INDEXER_TICK_MS);
  private readonly successLogCooldownMs = this.resolveSuccessLogCooldownMs(
    process.env.INDEXER_SUCCESS_LOG_COOLDOWN_MS,
  );
  private completedCycles = 0;
  private lastSuccessLogAt = 0;

  constructor(
    private stellarIndexer: StellarIndexerService,
    private logger: PinoLoggerService,
  ) {}

  async onModuleInit() {
    if (process.env.ENABLE_INDEXER !== 'false') {
      this.startIndexing();
    }
  }

  private startIndexing() {
    this.logger.log(
      `Starting background indexer (interval: ${this.indexingTickMs}ms)`,
      'IndexerService',
    );
    
    this.indexingInterval = setInterval(async () => {
      await this.indexAllChains();
    }, this.indexingTickMs);

    // Initial index
    this.indexAllChains();
  }

  async indexAllChains() {
    if (this.isIndexing) {
      this.logger.debug('Skipping index tick because previous cycle is still running', 'IndexerService');
      return;
    }

    this.isIndexing = true;
    try {
      await this.stellarIndexer.indexLatestLedgers();
      this.completedCycles += 1;

      const now = Date.now();
      const shouldLogSuccess =
        this.completedCycles === 1 || now - this.lastSuccessLogAt >= this.successLogCooldownMs;
      if (shouldLogSuccess) {
        this.lastSuccessLogAt = now;
        this.logger.log(`Indexing cycle completed (total cycles: ${this.completedCycles})`, 'IndexerService');
      }
    } catch (error: any) {
      if (error instanceof Error) {
        this.logger.error('Indexing failed', error.stack, 'IndexerService');
      } else {
         this.logger.error('Indexing failed', String(error), 'IndexerService');
      }
    } finally {
      this.isIndexing = false;
    }
  }

  async stopIndexing() {
    if (this.indexingInterval) {
      clearInterval(this.indexingInterval);
      this.logger.log('Indexer stopped', 'IndexerService');
    }
  }

  private resolveIndexingTickMs(rawValue?: string): number {
    const parsed = Number.parseInt((rawValue || '').trim(), 10);
    if (!Number.isFinite(parsed)) {
      return 5000;
    }
    return Math.min(Math.max(parsed, 2000), 60000);
  }

  private resolveSuccessLogCooldownMs(rawValue?: string): number {
    const parsed = Number.parseInt((rawValue || '').trim(), 10);
    if (!Number.isFinite(parsed)) {
      return 60_000;
    }

    return Math.min(Math.max(parsed, 5_000), 3_600_000);
  }
}
