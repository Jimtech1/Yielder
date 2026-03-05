import { Injectable } from '@nestjs/common';
import { Wallet, isAddress, verifyMessage } from 'ethers';
import { IWalletDriver, WalletDriverRequestOptions } from '../interfaces/wallet-driver.interface';

@Injectable()
export class EvmDriver implements IWalletDriver {
  async generateWallet() {
    const wallet = Wallet.createRandom();
    return {
      address: wallet.address,
      publicKey: wallet.address,
      secret: wallet.privateKey,
    };
  }

  async validateAddress(address: string): Promise<boolean> {
    return isAddress(address);
  }

  async getBalance(
    _address: string,
    _options?: WalletDriverRequestOptions,
  ): Promise<Array<{ asset: string; balance: string }>> {
    // Balances are resolved via chain-specific indexers/providers, not this lightweight driver.
    return [];
  }

  async getTransactions(
    _address: string,
    _limit = 20,
    _options?: WalletDriverRequestOptions,
  ): Promise<any[]> {
    // Transaction discovery for EVM chains is handled by dedicated indexers.
    return [];
  }

  async verifySignature(message: string, signature: string, address: string): Promise<boolean> {
    try {
      const signer = verifyMessage(message, signature);
      return signer.toLowerCase() === address.toLowerCase();
    } catch {
      return false;
    }
  }
}
