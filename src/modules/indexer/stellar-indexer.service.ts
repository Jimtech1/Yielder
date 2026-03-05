import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import axios from 'axios';
import { Model } from 'mongoose';
import { StellarDriver } from '../wallet/drivers/stellar.driver';
import { WalletDriverRequestOptions } from '../wallet/interfaces/wallet-driver.interface';
import { PinoLoggerService } from '../../shared/logger';
import { IndexerState, IndexerStateDocument } from './schemas/indexer-state.schema';
import { Wallet, WalletDocument } from '../wallet/schemas/wallet.schema';
import { Transaction, TransactionDocument } from '../wallet/schemas/transaction.schema';

type ExistingIndexedTransaction = {
  _id: unknown;
  hash: string;
  status?: string;
  type?: string;
  from?: string;
  to?: string;
  amount?: unknown;
  fee?: unknown;
  asset?: string;
  metadata?: Record<string, unknown>;
};

type ResolvedTransactionDetails = {
  type: string;
  from: string;
  to: string;
  amount: string;
  asset: string;
  status: 'confirmed' | 'failed';
  metadata: Record<string, unknown>;
};

type OperationTransferDetails = {
  from: string;
  to: string;
  amount: string;
  asset: string;
  fromAsset?: string;
  toAsset?: string;
  sourceAmount?: string;
  destinationAmount?: string;
  involvesWallet: boolean;
};

@Injectable()
export class StellarIndexerService {
  private readonly quietDriverOptions: WalletDriverRequestOptions = {
    suppressWarnings: true,
  };
  private readonly invalidWalletWarnings = new Set<string>();
  private readonly unavailableWalletWarnings = new Set<string>();

  constructor(
    @InjectModel(IndexerState.name) private indexerStateModel: Model<IndexerStateDocument>,
    @InjectModel(Wallet.name) private walletModel: Model<WalletDocument>,
    @InjectModel(Transaction.name) private transactionModel: Model<TransactionDocument>,
    private stellarDriver: StellarDriver,
    private logger: PinoLoggerService,
  ) {}

  async indexLatestLedgers() {
    const state = await this.getOrCreateState();
    
    try {
      // Get all Stellar wallets
      const wallets = await this.walletModel
        .find({ chain: 'stellar', isArchived: { $ne: true } } as any)
        .exec();

      for (const wallet of wallets) {
        if (!(await this.stellarDriver.validateAddress(wallet.address))) {
          this.logInvalidWalletRecord(wallet.address);
          continue;
        }

        await this.indexWallet(wallet);
      }

      // Update indexer state
      await this.indexerStateModel.updateOne(
        { _id: state._id },
        { 
          lastSync: new Date(),
          status: 'active',
        }
      );
    } catch (error) {
       if (error instanceof Error) {
        this.logger.error('Stellar indexing failed', error.stack, 'StellarIndexer');
      } else {
         this.logger.error('Stellar indexing failed', String(error), 'StellarIndexer');
      }
      
      await this.indexerStateModel.updateOne(
        { _id: state._id },
        { status: 'error' }
      );
    }
  }

  private async indexWallet(wallet: WalletDocument) {
    // Update balances
    const balances = await this.stellarDriver.getBalance(wallet.address, this.quietDriverOptions);

    // Index recent transactions
    // Use a wider lookback so recently-created pending tx records can still be reconciled
    // even when wallets are moderately active between index cycles.
    const transactions: any[] = await this.stellarDriver.getTransactions(
      wallet.address,
      100,
      this.quietDriverOptions,
    );

    if (balances.length === 0 && transactions.length === 0) {
      this.logUnavailableWalletRecord(wallet.address);
      return;
    }

    // Map driver balances to schema format
    const walletBalances = balances.map((b) => ({
      asset: b.asset,
      amount: b.balance,
      updatedAt: new Date(),
    }));
    
    // Logic: Iterate found transactions, check if hash exists for this wallet/chain, insert if new.
    // Optimization: Bulk check existing hashes to reduce DB calls.
    if (transactions.length > 0) {
      const hashes = [
        ...new Set(
          transactions
            .map((tx) => (typeof tx?.id === 'string' ? tx.id.trim() : ''))
            .filter((hash): hash is string => hash.length > 0),
        ),
      ];
      const existingDocs =
        hashes.length === 0
          ? []
          : await this.transactionModel
              .find({
                chain: 'stellar',
                hash: { $in: hashes },
              })
              .select('_id hash status type from to amount fee asset metadata')
              .lean()
              .exec();

      const existingByHash = new Map<string, ExistingIndexedTransaction>(
        existingDocs.map((doc) => [doc.hash, doc as ExistingIndexedTransaction]),
      );
      const seenHashes = new Set<string>();
      const newTransactionUpserts: Array<{
        updateOne: {
          filter: { chain: string; hash: string };
          update: { $setOnInsert: Record<string, unknown> };
          upsert: true;
        };
      }> = [];
      const txUpdates: Array<{
        updateOne: {
          filter: { _id: unknown };
          update: { $set: Record<string, unknown> };
        };
      }> = [];

      for (const tx of transactions) {
        const txHash = typeof tx?.id === 'string' ? tx.id.trim() : '';
        if (!txHash || seenHashes.has(txHash)) {
          continue;
        }
        seenHashes.add(txHash);

        const txRecord = tx as Record<string, unknown>;
        const blockNumber = this.resolveLedgerSequence(tx);
        const txTimestamp = tx?.created_at ? new Date(tx.created_at) : new Date();
        const existing = existingByHash.get(txHash);
        const indexedFee = this.resolveIndexedTransactionFee(txRecord);
        const resolvedDetails = this.shouldHydrateTransaction(existing)
          ? await this.resolveTransactionDetails(txRecord, wallet.address)
          : null;

        if (existing) {
          const updateSet: Record<string, unknown> = {};
          const resolvedStatus = resolvedDetails?.status || this.resolveTransactionStatus(txRecord);
          if (resolvedStatus === 'failed') {
            updateSet.status = 'failed';
            updateSet.timestamp = txTimestamp;
          } else if (this.shouldMarkAsConfirmed(existing.status)) {
            updateSet.status = 'confirmed';
            updateSet.timestamp = txTimestamp;
          }

          if (this.shouldUpdateIndexedFee(existing.fee, indexedFee)) {
            updateSet.fee = indexedFee;
          }

          if (this.shouldApplyHydratedDetails(existing, resolvedDetails)) {
            const existingType = this.toNonEmptyString(existing.type).toLowerCase();
            if (!existingType || existingType === 'transfer') {
              updateSet.type = resolvedDetails.type;
            }
            updateSet.from = resolvedDetails.from;
            updateSet.to = resolvedDetails.to;
            updateSet.amount = resolvedDetails.amount;
            updateSet.asset = resolvedDetails.asset;
            const existingMetadata =
              existing.metadata && typeof existing.metadata === 'object'
                ? (existing.metadata as Record<string, unknown>)
                : {};
            updateSet.metadata = {
              ...existingMetadata,
              ...resolvedDetails.metadata,
            };
            updateSet.timestamp = txTimestamp;
          }

          if (Object.keys(updateSet).length > 0) {
            if (typeof blockNumber === 'number') {
              updateSet.blockNumber = blockNumber;
            }
            txUpdates.push({
              updateOne: {
                filter: { _id: existing._id },
                update: { $set: updateSet },
              },
            });
          }
          continue;
        }

        const txDetails = resolvedDetails || this.buildFallbackTransactionDetails(txRecord, wallet.address);

        // Use upsert by unique key so indexer replays and concurrent writes remain idempotent.
        newTransactionUpserts.push({
          updateOne: {
            filter: {
              chain: 'stellar',
              hash: txHash,
            },
            update: {
              $setOnInsert: {
                walletId: wallet._id,
                hash: txHash,
                chain: 'stellar',
                type: txDetails.type,
                from: txDetails.from,
                to: txDetails.to,
                amount: txDetails.amount,
                fee: indexedFee,
                asset: txDetails.asset,
                status: txDetails.status,
                timestamp: txTimestamp,
                blockNumber,
                metadata: txDetails.metadata,
              },
            },
            upsert: true,
          },
        });
      }

      if (newTransactionUpserts.length > 0) {
        const upsertResult = await this.transactionModel.bulkWrite(newTransactionUpserts, {
          ordered: false,
        });
        if (upsertResult.upsertedCount > 0) {
          this.logger.log(
            `Indexed ${upsertResult.upsertedCount} new transactions for ${wallet.address}`,
            'StellarIndexer',
          );
        }
      }

      if (txUpdates.length > 0) {
        await this.transactionModel.bulkWrite(txUpdates);
        this.logger.log(
          `Updated ${txUpdates.length} indexed transactions for ${wallet.address}`,
          'StellarIndexer',
        );
      }
    }

    // Update wallet document with new balances
    await this.walletModel.updateOne(
        { _id: wallet._id },
        { $set: { balances: walletBalances } }
    );
  }

  private buildFallbackTransactionDetails(
    tx: Record<string, unknown>,
    walletAddress: string,
  ): ResolvedTransactionDetails {
    const sourceAccount = this.toNonEmptyString(tx.source_account);
    const asset = 'XLM';
    const amount = '0';

    return {
      type: this.resolveTransferType(sourceAccount, walletAddress, walletAddress),
      from: sourceAccount,
      to: walletAddress,
      amount,
      asset,
      status: this.resolveTransactionStatus(tx),
      metadata: {
        memo: tx.memo,
        fee: tx.fee_charged,
        fromAsset: asset,
        toAsset: asset,
        amount,
      },
    };
  }

  private async resolveTransactionDetails(
    tx: Record<string, unknown>,
    walletAddress: string,
  ): Promise<ResolvedTransactionDetails> {
    const fallback = this.buildFallbackTransactionDetails(tx, walletAddress);
    const operations = await this.fetchTransactionOperations(tx);
    const selectedOperationTransfer = this.selectBestTransferOperation(operations, walletAddress);
    const selectedTransfer = await this.resolveBestTransferDetails(
      tx,
      walletAddress,
      selectedOperationTransfer,
    );
    if (!selectedTransfer) {
      return fallback;
    }

    const fromAsset = selectedTransfer.fromAsset || selectedTransfer.asset || fallback.asset;
    const toAsset = selectedTransfer.toAsset || selectedTransfer.asset || fallback.asset;
    const amount = selectedTransfer.amount;
    const metadata: Record<string, unknown> = {
      ...fallback.metadata,
      fromAsset,
      toAsset,
      amount,
    };
    if (this.toPositiveNumber(selectedTransfer.sourceAmount) > 0) {
      metadata.sourceAmount = selectedTransfer.sourceAmount;
    }
    if (this.toPositiveNumber(selectedTransfer.destinationAmount) > 0) {
      metadata.destinationAmount = selectedTransfer.destinationAmount;
    }

    return {
      type: this.resolveTransferType(selectedTransfer.from, selectedTransfer.to, walletAddress),
      from: selectedTransfer.from || fallback.from,
      to: selectedTransfer.to || fallback.to,
      amount,
      asset: selectedTransfer.asset || fallback.asset,
      status: fallback.status,
      metadata,
    };
  }

  private async resolveBestTransferDetails(
    tx: Record<string, unknown>,
    walletAddress: string,
    operationTransfer: OperationTransferDetails | null,
  ): Promise<OperationTransferDetails | null> {
    const operationAmount = operationTransfer ? this.toPositiveNumber(operationTransfer.amount) : 0;
    if (operationTransfer && operationAmount > 0) {
      return operationTransfer;
    }

    const effects = await this.fetchTransactionEffects(tx);
    const txSourceAccount = this.toNonEmptyString(tx.source_account);
    const effectTransfer = this.selectBestTransferEffect(effects, walletAddress, txSourceAccount);
    if (!effectTransfer) {
      return operationTransfer;
    }

    const effectAmount = this.toPositiveNumber(effectTransfer.amount);
    if (effectAmount <= 0 && operationTransfer) {
      return operationTransfer;
    }

    return effectTransfer;
  }

  private async fetchTransactionOperations(
    tx: Record<string, unknown>,
  ): Promise<Array<Record<string, unknown>>> {
    const operationsHref = this.resolveOperationsHref(tx);
    if (!operationsHref) {
      return [];
    }

    const operationsUrl = this.buildOperationsUrl(operationsHref);
    try {
      const response = await axios.get<{ _embedded?: { records?: unknown[] } }>(operationsUrl, {
        timeout: 10_000,
        validateStatus: () => true,
      });
      if (response.status >= 400) {
        this.logger.debug(
          `Failed to load operations for tx ${this.toNonEmptyString(tx.id)}: HTTP ${response.status}`,
          'StellarIndexer',
        );
        return [];
      }

      const records = response.data?._embedded?.records;
      if (!Array.isArray(records)) {
        return [];
      }

      return records.filter((record): record is Record<string, unknown> => (
        Boolean(record) && typeof record === 'object'
      ));
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.debug(
        `Failed to hydrate tx operations for ${this.toNonEmptyString(tx.id)}: ${message}`,
        'StellarIndexer',
      );
      return [];
    }
  }

  private async fetchTransactionEffects(
    tx: Record<string, unknown>,
  ): Promise<Array<Record<string, unknown>>> {
    const effectsHref = this.resolveEffectsHref(tx);
    if (!effectsHref) {
      return [];
    }

    const effectsUrl = this.buildOperationsUrl(effectsHref);
    try {
      const response = await axios.get<{ _embedded?: { records?: unknown[] } }>(effectsUrl, {
        timeout: 10_000,
        validateStatus: () => true,
      });
      if (response.status >= 400) {
        this.logger.debug(
          `Failed to load effects for tx ${this.toNonEmptyString(tx.id)}: HTTP ${response.status}`,
          'StellarIndexer',
        );
        return [];
      }

      const records = response.data?._embedded?.records;
      if (!Array.isArray(records)) {
        return [];
      }

      return records.filter((record): record is Record<string, unknown> => (
        Boolean(record) && typeof record === 'object'
      ));
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.debug(
        `Failed to hydrate tx effects for ${this.toNonEmptyString(tx.id)}: ${message}`,
        'StellarIndexer',
      );
      return [];
    }
  }

  private resolveOperationsHref(tx: Record<string, unknown>): string | null {
    const links = tx._links;
    if (!links || typeof links !== 'object') {
      return null;
    }

    const operations = (links as Record<string, unknown>).operations;
    if (!operations || typeof operations !== 'object') {
      return null;
    }

    const href = this.toNonEmptyString((operations as Record<string, unknown>).href);
    if (!href) {
      return null;
    }

    return href.replace(/\{.*$/, '');
  }

  private resolveEffectsHref(tx: Record<string, unknown>): string | null {
    const links = tx._links;
    if (!links || typeof links !== 'object') {
      return null;
    }

    const effects = (links as Record<string, unknown>).effects;
    if (!effects || typeof effects !== 'object') {
      return null;
    }

    const href = this.toNonEmptyString((effects as Record<string, unknown>).href);
    if (!href) {
      return null;
    }

    return href.replace(/\{.*$/, '');
  }

  private buildOperationsUrl(href: string): string {
    try {
      const url = new URL(href);
      url.searchParams.set('order', 'desc');
      url.searchParams.set('limit', '200');
      return url.toString();
    } catch {
      const separator = href.includes('?') ? '&' : '?';
      return `${href}${separator}order=desc&limit=200`;
    }
  }

  private selectBestTransferOperation(
    operations: Array<Record<string, unknown>>,
    walletAddress: string,
  ): OperationTransferDetails | null {
    const candidates = operations
      .map((operation) => this.extractTransferDetails(operation, walletAddress))
      .filter((details): details is OperationTransferDetails => details !== null);
    if (candidates.length === 0) {
      return null;
    }

    const walletRelated = candidates.filter((candidate) => candidate.involvesWallet);
    const prioritized = walletRelated.length > 0 ? walletRelated : candidates;
    const withAmount = prioritized.find((candidate) => this.toPositiveNumber(candidate.amount) > 0);
    return withAmount || prioritized[0];
  }

  private selectBestTransferEffect(
    effects: Array<Record<string, unknown>>,
    walletAddress: string,
    txSourceAccount: string,
  ): OperationTransferDetails | null {
    const candidates = effects
      .map((effect) => this.extractTransferDetailsFromEffect(effect, walletAddress, txSourceAccount))
      .filter((details): details is OperationTransferDetails => details !== null);
    if (candidates.length === 0) {
      return null;
    }

    const walletRelated = candidates.filter((candidate) => candidate.involvesWallet);
    const prioritized = walletRelated.length > 0 ? walletRelated : candidates;
    const positiveCandidates = prioritized.filter((candidate) => this.toPositiveNumber(candidate.amount) > 0);
    if (positiveCandidates.length > 0) {
      return positiveCandidates.sort(
        (left, right) => this.toPositiveNumber(right.amount) - this.toPositiveNumber(left.amount),
      )[0];
    }

    return prioritized[0];
  }

  private extractTransferDetails(
    operation: Record<string, unknown>,
    walletAddress: string,
  ): OperationTransferDetails | null {
    const operationType = this.toNonEmptyString(operation.type).toLowerCase();
    if (!operationType) {
      return null;
    }

    if (operationType === 'payment') {
      const from = this.toNonEmptyString(operation.from) || this.toNonEmptyString(operation.source_account);
      const to = this.toNonEmptyString(operation.to);
      const amount = this.normalizeAmountString(operation.amount);
      const asset = this.resolveOperationAsset(operation) || 'XLM';
      if (!from && !to) {
        return null;
      }
      return {
        from,
        to,
        amount,
        asset,
        fromAsset: asset,
        toAsset: asset,
        sourceAmount: amount,
        destinationAmount: amount,
        involvesWallet: from === walletAddress || to === walletAddress,
      };
    }

    if (operationType === 'create_claimable_balance') {
      const from = this.toNonEmptyString(operation.from) || this.toNonEmptyString(operation.source_account);
      const claimants = Array.isArray(operation.claimants) ? operation.claimants : [];
      const claimantDestinations = claimants
        .map((claimant) =>
          claimant && typeof claimant === 'object'
            ? this.toNonEmptyString((claimant as Record<string, unknown>).destination)
            : '',
        )
        .filter((value) => value.length > 0);
      const to = claimantDestinations[0] || this.toNonEmptyString(operation.claimant);
      const amount = this.normalizeAmountString(operation.amount);
      const asset = this.resolveOperationAsset(operation) || 'XLM';

      if (!from && !to) {
        return null;
      }

      return {
        from,
        to,
        amount,
        asset,
        fromAsset: asset,
        toAsset: asset,
        sourceAmount: amount,
        destinationAmount: amount,
        involvesWallet:
          from === walletAddress ||
          to === walletAddress ||
          claimantDestinations.includes(walletAddress),
      };
    }

    if (operationType === 'claim_claimable_balance') {
      const from = this.toNonEmptyString(operation.from) || this.toNonEmptyString(operation.source_account);
      const to = this.toNonEmptyString(operation.claimant) || this.toNonEmptyString(operation.to);
      const amount = this.normalizeAmountString(operation.amount);
      const asset = this.resolveOperationAsset(operation) || 'XLM';

      if (!from && !to) {
        return null;
      }

      return {
        from,
        to,
        amount,
        asset,
        fromAsset: asset,
        toAsset: asset,
        sourceAmount: amount,
        destinationAmount: amount,
        involvesWallet: from === walletAddress || to === walletAddress,
      };
    }

    if (operationType === 'create_account') {
      const from = this.toNonEmptyString(operation.funder) || this.toNonEmptyString(operation.source_account);
      const to = this.toNonEmptyString(operation.account);
      const amount = this.normalizeAmountString(operation.starting_balance);
      if (!from && !to) {
        return null;
      }
      return {
        from,
        to,
        amount,
        asset: 'XLM',
        fromAsset: 'XLM',
        toAsset: 'XLM',
        sourceAmount: amount,
        destinationAmount: amount,
        involvesWallet: from === walletAddress || to === walletAddress,
      };
    }

    if (operationType === 'path_payment_strict_send' || operationType === 'path_payment_strict_receive') {
      const from = this.toNonEmptyString(operation.from) || this.toNonEmptyString(operation.source_account);
      const to = this.toNonEmptyString(operation.to);
      if (!from && !to) {
        return null;
      }

      const sourceAsset = this.resolveOperationAsset(operation, 'source');
      const destinationAsset = this.resolveOperationAsset(operation);
      const sourceAmount = this.normalizeAmountString(operation.source_amount);
      const destinationAmount =
        this.normalizeAmountString(operation.amount) !== '0'
          ? this.normalizeAmountString(operation.amount)
          : this.normalizeAmountString(operation.destination_amount);

      const walletIsSender = from === walletAddress;
      const amount = walletIsSender
        ? (sourceAmount !== '0' ? sourceAmount : destinationAmount)
        : (destinationAmount !== '0' ? destinationAmount : sourceAmount);
      const asset = walletIsSender
        ? sourceAsset || destinationAsset || 'XLM'
        : destinationAsset || sourceAsset || 'XLM';

      return {
        from,
        to,
        amount,
        asset,
        fromAsset: sourceAsset || asset,
        toAsset: destinationAsset || asset,
        sourceAmount,
        destinationAmount,
        involvesWallet: walletIsSender || to === walletAddress,
      };
    }

    return null;
  }

  private extractTransferDetailsFromEffect(
    effect: Record<string, unknown>,
    walletAddress: string,
    txSourceAccount: string,
  ): OperationTransferDetails | null {
    const effectType = this.toNonEmptyString(effect.type).toLowerCase();
    if (!effectType) {
      return null;
    }

    const account = this.toNonEmptyString(effect.account);
    if (!account) {
      return null;
    }

    const amount = this.normalizeAmountString(effect.amount);
    const asset = this.resolveEffectAsset(effect);
    const involvesWallet = account === walletAddress;

    if (effectType === 'account_credited') {
      const fromCandidate = this.toNonEmptyString(effect.source_account) || txSourceAccount;
      const from = !fromCandidate || fromCandidate === account ? 'external' : fromCandidate;
      return {
        from,
        to: account,
        amount,
        asset,
        fromAsset: asset,
        toAsset: asset,
        sourceAmount: amount,
        destinationAmount: amount,
        involvesWallet,
      };
    }

    if (effectType === 'account_debited') {
      const toCandidate = this.toNonEmptyString(effect.destination) || txSourceAccount;
      const to = !toCandidate || toCandidate === account ? 'external' : toCandidate;
      return {
        from: account,
        to,
        amount,
        asset,
        fromAsset: asset,
        toAsset: asset,
        sourceAmount: amount,
        destinationAmount: amount,
        involvesWallet,
      };
    }

    return null;
  }

  private resolveOperationAsset(
    operation: Record<string, unknown>,
    prefix?: 'source',
  ): string {
    const keyPrefix = prefix ? `${prefix}_` : '';
    const compositeAsset = this.parseOperationAssetValue(operation[`${keyPrefix}asset`]);
    if (compositeAsset) {
      return compositeAsset;
    }

    const assetType = this.toNonEmptyString(operation[`${keyPrefix}asset_type`]).toLowerCase();
    if (assetType === 'native') {
      return 'XLM';
    }

    const assetCode = this.toNonEmptyString(operation[`${keyPrefix}asset_code`]).toUpperCase();
    const assetIssuer = this.toNonEmptyString(operation[`${keyPrefix}asset_issuer`]);
    if (assetCode && assetIssuer) {
      return `${assetCode}:${assetIssuer}`;
    }

    return assetCode;
  }

  private parseOperationAssetValue(value: unknown): string {
    const raw = this.toNonEmptyString(value);
    if (!raw) {
      return '';
    }

    if (raw.toLowerCase() === 'native') {
      return 'XLM';
    }

    const [rawCode, rawIssuer] = raw.split(':');
    const code = rawCode.trim().toUpperCase();
    const issuer = (rawIssuer || '').trim();
    if (!code) {
      return '';
    }

    return issuer ? `${code}:${issuer}` : code;
  }

  private resolveEffectAsset(effect: Record<string, unknown>): string {
    return this.resolveOperationAsset(effect) || 'XLM';
  }

  private shouldHydrateTransaction(existing?: ExistingIndexedTransaction): boolean {
    if (!existing) {
      return true;
    }

    if (this.toPositiveNumber(existing.amount) <= 0) {
      return true;
    }

    if (!this.toNonEmptyString(existing.from) || !this.toNonEmptyString(existing.to)) {
      return true;
    }

    if (!this.toNonEmptyString(existing.asset)) {
      return true;
    }

    const metadata =
      existing.metadata && typeof existing.metadata === 'object'
        ? (existing.metadata as Record<string, unknown>)
        : {};
    if (!this.toNonEmptyString(metadata.fromAsset)) {
      return true;
    }

    return false;
  }

  private shouldApplyHydratedDetails(
    existing: ExistingIndexedTransaction,
    resolvedDetails: ResolvedTransactionDetails | null,
  ): resolvedDetails is ResolvedTransactionDetails {
    if (!resolvedDetails) {
      return false;
    }

    const resolvedAmount = this.toPositiveNumber(resolvedDetails.amount);
    const existingAmount = this.toPositiveNumber(existing.amount);
    if (resolvedAmount > 0 && existingAmount <= 0) {
      return true;
    }

    if (!this.toNonEmptyString(existing.from) && resolvedDetails.from) {
      return true;
    }

    if (!this.toNonEmptyString(existing.to) && resolvedDetails.to) {
      return true;
    }

    if (!this.toNonEmptyString(existing.asset) && resolvedDetails.asset) {
      return true;
    }

    const existingType = this.toNonEmptyString(existing.type).toLowerCase();
    const hasGenericType = !existingType || existingType === 'transfer';
    if ((existingType === 'transfer' || !existingType) && resolvedDetails.type !== 'transfer') {
      return true;
    }

    const existingMetadata =
      existing.metadata && typeof existing.metadata === 'object'
        ? (existing.metadata as Record<string, unknown>)
        : {};
    if (
      hasGenericType &&
      !this.toNonEmptyString(existingMetadata.fromAsset) &&
      this.toNonEmptyString(resolvedDetails.metadata.fromAsset)
    ) {
      return true;
    }

    if (
      hasGenericType &&
      !this.toNonEmptyString(existingMetadata.toAsset) &&
      this.toNonEmptyString(resolvedDetails.metadata.toAsset)
    ) {
      return true;
    }

    return false;
  }

  private resolveTransactionStatus(tx: Record<string, unknown>): 'confirmed' | 'failed' {
    const successful = tx.successful;
    if (typeof successful === 'boolean') {
      return successful ? 'confirmed' : 'failed';
    }

    const normalized = this.toNonEmptyString(successful).toLowerCase();
    if (normalized === 'false' || normalized === '0' || normalized === 'failed') {
      return 'failed';
    }

    return 'confirmed';
  }

  private resolveTransferType(from: string, to: string, walletAddress: string): string {
    if (!from || !to) {
      return 'transfer';
    }
    if (to === walletAddress && from !== walletAddress) {
      return 'deposit';
    }
    if (from === walletAddress && to !== walletAddress) {
      return 'withdrawal';
    }
    return 'transfer';
  }

  private resolveIndexedTransactionFee(tx: Record<string, unknown>): string {
    const directCandidates = [tx.fee_charged, tx.feeCharged, tx.fee];
    for (const candidate of directCandidates) {
      const amount = this.normalizeAmountString(candidate);
      if (this.toPositiveNumber(amount) > 0) {
        return amount;
      }
    }

    return '0';
  }

  private shouldUpdateIndexedFee(existingFee: unknown, indexedFee: string): boolean {
    if (this.toPositiveNumber(indexedFee) <= 0) {
      return false;
    }

    return this.toPositiveNumber(existingFee) <= 0;
  }

  private toNonEmptyString(value: unknown): string {
    if (typeof value !== 'string') {
      return '';
    }
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : '';
  }

  private normalizeAmountString(value: unknown): string {
    const raw = this.toNonEmptyString(value);
    if (!raw) {
      return '0';
    }

    const parsed = Number(raw);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return '0';
    }

    return raw;
  }

  private toPositiveNumber(value: unknown): number {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return 0;
    }
    return parsed;
  }

  private resolveLedgerSequence(tx: any): number | undefined {
    const rawLedger = tx?.ledger_attr ?? tx?.ledger;

    if (typeof rawLedger === 'number' && Number.isFinite(rawLedger)) {
      return rawLedger;
    }

    if (typeof rawLedger === 'string') {
      const parsed = Number(rawLedger);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }

    return undefined;
  }

  private shouldMarkAsConfirmed(status?: string): boolean {
    const normalized = (status || '').trim().toLowerCase();
    return normalized === 'pending' || normalized === 'submitted' || normalized === 'processing';
  }

  private logInvalidWalletRecord(address: string): void {
    const normalized = this.toNonEmptyString(address);
    if (!normalized || this.invalidWalletWarnings.has(normalized)) {
      return;
    }

    this.invalidWalletWarnings.add(normalized);
    this.logger.debug(
      `Skipping indexed wallet with invalid Stellar address: ${normalized}. Archive or correct this record to resume indexing.`,
      'StellarIndexer',
    );
  }

  private logUnavailableWalletRecord(address: string): void {
    const normalized = this.toNonEmptyString(address);
    if (!normalized || this.unavailableWalletWarnings.has(normalized)) {
      return;
    }

    this.unavailableWalletWarnings.add(normalized);
    this.logger.debug(
      `Skipping indexed wallet ${normalized} because account data is unavailable on configured Horizon endpoints (likely unfunded, wrong network, or temporarily unreachable).`,
      'StellarIndexer',
    );
  }

  private async getOrCreateState() {
    let state = await this.indexerStateModel.findOne({ chain: 'stellar' }).exec();

    if (!state) {
      state = await this.indexerStateModel.create({
        chain: 'stellar',
        lastBlock: 0,
        lastSync: new Date(),
        status: 'active',
      });
    }

    return state;
  }
}
