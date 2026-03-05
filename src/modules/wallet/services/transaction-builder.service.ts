import { Injectable, BadRequestException } from '@nestjs/common';
import * as StellarSdk from 'stellar-sdk';
import { FeeEstimationService } from './fee-estimation.service';

@Injectable()
export class TransactionBuilderService {
  constructor(private feeEstimationService: FeeEstimationService) {}

  async buildTransferTransaction(
    sourcePublicKey: string,
    destinationAddress: string,
    assetCode: string,
    amount: string,
    memo?: string
  ): Promise<string> {
    // 1. Load Account (Sequence number)
    // In a real app, we need to fetch the sequence number from Horizon.
    // Since we don't have direct access to "loadAccount" here without the driver/server context,
    // we strictly need the server instance or pass the current sequence.
    
    // For this service to be pure, it should probably take the sequence number or use the driver to fetch it.
    // Let's assume the caller passes the sequence or we fetch it.
    throw new Error('Method requires account sequence fetching logic which needs to be integrated with StellarDriver');
  }

  // Refactored to return XDR string
  async buildStellarTransfer(
    server: StellarSdk.Horizon.Server,
    sourcePublicKey: string,
    destinationAddress: string,
    asset: StellarSdk.Asset,
    amount: string,
    memoStr?: string
  ): Promise<string> {
    try {
      const account = await server.loadAccount(sourcePublicKey);
      const fee = await this.feeEstimationService.estimateFee('stellar', 'transfer');

      let transactionBuilder = new StellarSdk.TransactionBuilder(account, {
        fee: fee,
        networkPassphrase: StellarSdk.Networks.TESTNET, // specific to env
      });

      transactionBuilder.addOperation(
        StellarSdk.Operation.payment({
          destination: destinationAddress,
          asset: asset,
          amount: amount,
        })
      );

      if (memoStr) {
        transactionBuilder.addMemo(StellarSdk.Memo.text(memoStr));
      }

      transactionBuilder.setTimeout(30);
      
      const transaction = transactionBuilder.build();
      return transaction.toXDR();
    } catch (error) {
       // @ts-ignore
      throw new BadRequestException(`Failed to build transaction: ${error.message}`);
    }
  }
}
