export type WalletDriverRequestOptions = {
  suppressWarnings?: boolean;
};

export interface IWalletDriver {
  generateWallet(): Promise<{
    address: string;
    publicKey: string;
    secret: string;
  }>;

  validateAddress(address: string): Promise<boolean>;

  getBalance(address: string, options?: WalletDriverRequestOptions): Promise<Array<{
    asset: string;
    balance: string;
  }>>;

  getTransactions(
    address: string,
    limit?: number,
    options?: WalletDriverRequestOptions,
  ): Promise<any[]>;

  verifySignature(message: string, signature: string, address: string): Promise<boolean>;
}
