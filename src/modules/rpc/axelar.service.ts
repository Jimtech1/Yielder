import { Injectable, Logger } from '@nestjs/common';
import { ethers } from 'ethers';
import {
  AxelarQueryAPI,
  Environment,
  EvmChain,
  GasToken,
  AxelarGMPRecoveryAPI,
} from '@axelar-network/axelarjs-sdk';

// Standard Production Gateway/GasService addresses usually constant per env,
// but best fetched or configured. For simplicity in this service we can use known constants
// or rely on the SDK where possible.
const AXELAR_GATEWAY_ABI = [
  'function callContract(string calldata destinationChain, string calldata destinationAddress, bytes calldata payload) external',
];
const AXELAR_GAS_SERVICE_ABI = [
  'function payNativeGasForContractCall(address sender, string calldata destinationChain, string calldata destinationAddress, bytes calldata payload, address refundAddress) external payable',
];

@Injectable()
export class AxelarService {
  private readonly logger = new Logger(AxelarService.name);
  private provider: ethers.JsonRpcProvider;
  private signer: ethers.Wallet;
  private axelarQuery?: AxelarQueryAPI;
  private axelarRecovery?: AxelarGMPRecoveryAPI;
  private readonly axelarEnv: Environment;

  private gatewayAddress: string | undefined;
  private gasServiceAddress: string | undefined;

  constructor() {
    const rpcUrl = process.env.AXELAR_RPC_URL || 'https://rpc-axelar-testnet.imperator.co';
    this.provider = new ethers.JsonRpcProvider(rpcUrl);

    // Delay SDK initialization to first use to avoid startup crashes on
    // transient network/DNS failures.
    this.axelarEnv = (process.env.AXELAR_ENV as Environment) || Environment.TESTNET;

    // Initialize Signer (Required for sending transactions)
    const privateKey = process.env.AXELAR_PRIVATE_KEY;
    if (privateKey) {
      this.signer = new ethers.Wallet(privateKey, this.provider);
    } else {
      this.logger.warn('AXELAR_PRIVATE_KEY not set. GMP sending will fail.');
    }
  }

  async onModuleInit() {
    // Dynamically fetch contract addresses based on the chain we are connected to.
    // For this example, assuming we are on an EVM source chain (e.g. Ethereum, Avalanche, etc.)
    // In a real multi-chain setup, you might need to configure this service *per chain*
    // or pass the source chain details.
    // For now, let's assume we are sending FROM the chain this provider is connected to.
    
    // Note: In a robust app, these should be verified against known Axelar docs/config
    this.gatewayAddress = process.env.AXELAR_GATEWAY_ADDRESS; 
    this.gasServiceAddress = process.env.AXELAR_GAS_SERVICE_ADDRESS;
  }

  async executeCall(method: string, params: any[]) {
    try {
      return await this.provider.send(method, params);
    } catch (error) {
      throw new Error(`Axelar RPC call failed: ${error.message}`);
    }
  }

  async getStatus() {
    try {
      const blockNumber = await this.provider.getBlockNumber();
      return {
        chain: 'axelar',
        status: 'connected',
        blockNumber,
        gateway: this.gatewayAddress,
        gasService: this.gasServiceAddress,
      };
    } catch (error) {
      return {
        chain: 'axelar',
        status: 'disconnected',
        error: error.message,
      };
    }
  }

  /**
   * 
   * 1. Estimate Gas: Use AxelarQueryAPI to get the cost for the cross-chain call.
   * 2. Pay Gas: Call the AxelarGasService contract to pay the relayer.
   * 3. Call Contract: Call the AxelarGateway contract to initiate the messages.
   * 
   * @param destinationChain The name of the destination chain (e.g., "Avalanche", "Moonbeam")
   * @param destinationAddress The contract address on the destination chain
   * @param payload The encoded data payload
   */
  async sendGMPMessage(destinationChain: string, destinationAddress: string, payload: string) {
    if (!this.signer) {
      throw new Error('Signer not initialized. Cannot send GMP message.');
    }
    if (!this.gatewayAddress || !this.gasServiceAddress) {
      throw new Error('Axelar contracts not configured (Gateway/GasService).');
    }

    // 1. Estimate Gas
    // Assuming source chain is the one we are connected to. 
    // You might need a way to map your RPC chain ID to Axelar Chain Name.
    const sourceChainName = EvmChain.ETHEREUM; // EXAMPLE: Replace with dynamic lookup
    const gasLimit = 250000; // Estimated gas limit for the destination contract execution
    const gasToken = GasToken.ETH; // The native token of the source chain

    let estimatedGasFee = '0';
    try {
      const axelarQuery = this.getAxelarQuery();
      // In production, you might want to add a buffer to the estimated fee
      const fees = await axelarQuery.estimateGasFee(
        sourceChainName,
        destinationChain as EvmChain,
        gasToken,
        gasLimit, 
        1.1 // gasMultiplier
      );
      estimatedGasFee = (fees as any).toString(); // Ensure string for ethers
    } catch (e) {
      this.logger.error(`Failed to estimate gas: ${e.message}`);
      // Fallback or re-throw depending on policy
      throw e;
    }

    // 2. Prepare Contracts
    const gateway = new ethers.Contract(this.gatewayAddress, AXELAR_GATEWAY_ABI, this.signer);
    const gasService = new ethers.Contract(this.gasServiceAddress, AXELAR_GAS_SERVICE_ABI, this.signer);

   // 3. Execute Transactions
    
    try {
      // A. Pay Gas
      this.logger.log(`Paying gas: ${estimatedGasFee} for message to ${destinationChain}`);
      const gasTx = await gasService.payNativeGasForContractCall(
        this.signer.address,
        destinationChain,
        destinationAddress,
        payload,
        this.signer.address, // Refund address
        { value: estimatedGasFee }
      );
      await gasTx.wait(1); // Wait for confirmation

      // B. Call Contract (Initiate GMP)
      this.logger.log(`Initiating GMP message...`);
      const apiTx = await gateway.callContract(
        destinationChain,
        destinationAddress,
        payload
      );
      const receipt = await apiTx.wait(1);

      return {
        success: true,
        messageId: receipt.hash, // The Tx Hash is often used as the ID reference initially
        monitorUrl: `https://axelarscan.io/gmp/${receipt.hash}`,
        gasPaid: estimatedGasFee,
        rawReceipt: receipt
      };

    } catch (error) {
      this.logger.error(`GMP Execution failed: ${error.message}`);
      throw error;
    }
  }

  /**
   * Production Capabilities & Suggestions:
   * 
   * 1. **Robust Gas Estimation**: 
   *    - Implement dynamic source chain detection.
   *    - Allow specific gas limit overrides per message type.
   * 
   * 2. **Message Recovery**:
   *    - Use `AxelarGMPRecoveryAPI` to manually execute messages on destination if they fail (e.g., due to low gas).
   *    - Implement a background job to scan for "stuck" messages and top up gas.
   * 
   * 3. **Token Transfers**:
   *    - Extend to support `sendToken` (Gateway) and `payNativeGasForExpressCall` for faster execution.
   * 
   * 4. **Atomicity**:
   *    - DEPLOY A PROXY CONTRACT. Instead of calling Gateway directly from EOA, call a custom contract
   *      that calls `gasService.payGas` AND `gateway.callContract` in one atomic transaction.
   *      This prevents cases where gas is paid but the message tx fails.
   */
  async recoverMessage(txHash: string) {
    return await this.getAxelarRecovery().queryTransactionStatus(txHash);
  }

  private getAxelarQuery(): AxelarQueryAPI {
    if (!this.axelarQuery) {
      this.axelarQuery = new AxelarQueryAPI({ environment: this.axelarEnv });
    }
    return this.axelarQuery;
  }

  private getAxelarRecovery(): AxelarGMPRecoveryAPI {
    if (!this.axelarRecovery) {
      this.axelarRecovery = new AxelarGMPRecoveryAPI({ environment: this.axelarEnv });
    }
    return this.axelarRecovery;
  }
}
