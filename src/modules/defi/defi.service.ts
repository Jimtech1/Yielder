import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import * as StellarSdk from 'stellar-sdk';
import axios from 'axios';
import { ethers } from 'ethers';
import { DeFiPosition, DeFiPositionDocument } from './schemas/defi-position.schema';
import { FeeEstimationService } from '../wallet/services/fee-estimation.service';
import { Wallet, WalletDocument } from '../wallet/schemas/wallet.schema';
import { Transaction, TransactionDocument } from '../wallet/schemas/transaction.schema';
import {
  ConnectedWallet,
  ConnectedWalletDocument,
} from '../auth/schemas/connected-wallet.schema';
import { PathPaymentQuoteDto } from './dto/path-payment-quote.dto';
import { BuildPathPaymentDto } from './dto/build-path-payment.dto';
import { BridgeQuoteDto } from './dto/bridge-quote.dto';
import { BuildBridgeTransactionDto } from './dto/build-bridge-transaction.dto';
import { CreateOptimizerPlanDto } from './dto/create-optimizer-plan.dto';
import { ExecuteOptimizerDto } from './dto/execute-optimizer.dto';
import { ExecuteBorrowDto } from './dto/execute-borrow.dto';
import { CompleteOptimizerDepositDto } from './dto/complete-optimizer-deposit.dto';
import { SettleOptimizerFeeDto } from './dto/settle-optimizer-fee.dto';
import { RecordBridgeHistoryDto } from './dto/record-bridge-history.dto';
import { BuildOptimizerStellarDepositTxDto } from './dto/build-optimizer-stellar-deposit-tx.dto';
import { CollectPlatformFeesDto } from './dto/collect-platform-fees.dto';
import { BridgeHistory, BridgeHistoryDocument } from './schemas/bridge-history.schema';
import {
  OptimizerExecution,
  OptimizerExecutionDocument,
} from './schemas/optimizer-execution.schema';
import {
  OptimizerFeeSettlement,
  OptimizerFeeSettlementDocument,
} from './schemas/optimizer-fee-settlement.schema';
import {
  PlatformFeeAction,
  PlatformFeeStatus,
} from '../platform-fee/schemas/platform-fee-record.schema';
import { PlatformFeeQuote, PlatformFeeService } from '../platform-fee/platform-fee.service';
import { AccessControlService } from '../access/access-control.service';
import { MarketService, Protocol as StellarProtocol } from '../market/market.service';
import { CircleCctpV2CustomTxBuilderService } from './services/circle-cctp-v2-custom-tx-builder.service';

type StrictSendPathRecord = {
  path: Array<{
    asset_type: string;
    asset_code?: string;
    asset_issuer?: string;
  }>;
  source_amount: string;
  source_asset_type: string;
  source_asset_code?: string;
  source_asset_issuer?: string;
  destination_amount: string;
  destination_asset_type: string;
  destination_asset_code?: string;
  destination_asset_issuer?: string;
};

type YieldCategory = 'lending' | 'liquidity' | 'staking';
type YieldRisk = 'Low' | 'Medium' | 'High';
type OptimizerSourceType = 'wallet-cache' | 'request' | 'connected-wallet' | 'unknown';

export interface YieldOpportunity {
  id: string;
  name: string;
  protocol: string;
  protocolLogo: string;
  logo: string;
  asset: string;
  chain: string;
  category: YieldCategory;
  apy: number;
  tvl: number;
  riskScore: number;
  risk: YieldRisk;
}

type DefiLlamaYieldPool = {
  pool?: unknown;
  chain?: unknown;
  project?: unknown;
  symbol?: unknown;
  apy?: unknown;
  apyBase?: unknown;
  apyReward?: unknown;
  tvlUsd?: unknown;
  stablecoin?: unknown;
  ilRisk?: unknown;
  category?: unknown;
  poolMeta?: unknown;
  exposure?: unknown;
};

type DefiLlamaYieldResponse = {
  data?: DefiLlamaYieldPool[];
};

type DefiLlamaProtocolMeta = {
  name?: unknown;
  slug?: unknown;
  logo?: unknown;
};

type DefiLlamaProtocolLogoLookup = {
  bySlug: Map<string, string>;
  byName: Map<string, string>;
};

type RankedYieldEntry = {
  opportunity: YieldOpportunity;
  chainKey: string;
};

type StargateChain = {
  chainKey: string;
  chainType?: string;
  chainId?: number;
  shortName?: string;
  name?: string;
  nativeCurrency?: {
    chainKey?: string;
    name?: string;
    symbol?: string;
    decimals?: number;
    address?: string;
  };
};

type StargateToken = {
  isBridgeable?: boolean;
  chainKey: string;
  address: string;
  decimals: number;
  symbol: string;
  name?: string;
  price?: {
    usd?: number;
  };
};

type StargateQuoteFee = {
  token?: string;
  chainKey?: string;
  amount?: string;
  type?: string;
};

type StargateTransactionData = {
  data?: string;
  to?: string;
  from?: string;
  value?: string;
};

type StargateQuoteStep = {
  type?: string;
  sender?: string;
  chainKey?: string;
  transaction?: StargateTransactionData;
};

type StargateQuote = {
  route?: string;
  error?: {
    message?: string;
  } | null;
  srcAmount?: string;
  dstAmount?: string;
  srcAmountMin?: string;
  srcAmountMax?: string;
  dstAmountMin?: string;
  srcToken?: string;
  dstToken?: string;
  srcAddress?: string;
  dstAddress?: string;
  srcChainKey?: string;
  dstChainKey?: string;
  dstNativeAmount?: string;
  duration?: {
    estimated?: number;
  };
  fees?: StargateQuoteFee[];
  steps?: StargateQuoteStep[];
};

type EvmTransactionRequest = {
  to: string;
  data: string;
  from?: string;
  value: string;
};

type StellarTransactionPayload = {
  xdr: string;
  network: 'testnet' | 'mainnet';
};

type CircleCctpV2InAppBuildPayload = {
  executionType: 'evm' | 'external';
  externalUrl: string | null;
  approvalTransaction: EvmTransactionRequest | null;
  bridgeTransaction: EvmTransactionRequest | null;
  bridgeStellarTransaction?: StellarTransactionPayload;
};

type CircleCctpV2TxBuilderRequest = {
  route: string;
  srcChainKey: string;
  dstChainKey: string;
  srcToken: string;
  dstToken: string;
  srcAmount: string;
  srcAmountBaseUnits: string;
  dstAmountMin: string;
  dstAmountMinBaseUnits: string;
  srcAddress: string;
  dstAddress: string;
  slippageBps: number;
};

type CircleBridgeApiTransactionData = {
  to?: string;
  data?: string;
  value?: string | number;
};

type CircleBridgeApiStep = {
  action?: {
    type?: string;
    data?: Record<string, unknown>;
  };
};

type CircleBridgeApiQuoteResponse = {
  quotes?: Array<Record<string, unknown>>;
};

type CircleBridgeApiRouteResponse = {
  steps?: CircleBridgeApiStep[];
};

type BridgeExecutionType = 'evm' | 'external';

type BridgeTokenInfo = {
  chainKey: string;
  address: string;
  decimals: number;
  symbol: string;
  name: string;
  isBridgeable: boolean;
  priceUsd: number | null;
};

type NormalizedBridgeQuote = {
  route: string;
  executionType: BridgeExecutionType;
  externalUrl: string | null;
  srcChainKey: string;
  dstChainKey: string;
  srcChainId: number | null;
  dstChainId: number | null;
  srcAmount: string;
  srcAmountBaseUnits: string;
  dstAmount: string;
  dstAmountBaseUnits: string;
  dstAmountMin: string;
  dstAmountMinBaseUnits: string;
  estimatedDurationSeconds: number | null;
  fees: Array<{
    type: string;
    chainKey: string;
    tokenAddress: string;
    tokenSymbol: string;
    amount: string;
    amountBaseUnits: string;
  }>;
  transactions: {
    approve: EvmTransactionRequest | null;
    bridge: EvmTransactionRequest | null;
  };
  error: string | null;
};

type OptimizerBalanceCandidate = {
  chainKey: string;
  address: string | null;
  availableAmount: number | null;
  source: OptimizerSourceType;
};

type OptimizerExecutionStatus =
  | 'planned'
  | 'bridge-submitted'
  | 'bridge-external'
  | 'awaiting-bridge-finality'
  | 'deposit-pending'
  | 'wallet-action-required'
  | 'position-opened'
  | 'failed';

type OptimizerProtocolCallTemplate = {
  executionType?: 'evm' | 'external' | 'synthetic';
  chainKey?: string;
  protocol?: string;
  externalUrl?: string;
  target?: string;
  abi?: string[];
  method?: string;
  args?: unknown[];
  value?: string;
  approveTokenAddress?: string;
  approveSpender?: string;
  approveAmount?: string;
  requiredSignerMatch?: boolean;
};

type ResolvedOptimizerProtocolCallTemplate = {
  key: string;
  template: OptimizerProtocolCallTemplate;
};

type OptimizerProtocolDepositResultStatus =
  | 'executed'
  | 'awaiting-bridge-finality'
  | 'wallet-action-required'
  | 'external-action-required';

type OptimizerProtocolDepositResult = {
  status: OptimizerProtocolDepositResultStatus;
  message: string;
  depositTxHash?: string;
  externalUrl?: string;
  metadata?: Record<string, unknown>;
};

type OptimizerNextAction =
  | 'wallet-execution-required'
  | 'open-external-bridge'
  | 'await-bridge-finality'
  | 'open-external-deposit'
  | 'complete-protocol-deposit'
  | 'fee-wallet-sign-required'
  | 'none';

type BorrowExecutionStatus = 'executed' | 'wallet-action-required' | 'external-action-required';
type BorrowNextAction = 'wallet-execution-required' | 'open-external-borrow' | 'none';

type OptimizerFeeSettlementStatus =
  | 'wallet-action-required'
  | 'submitted'
  | 'confirmed'
  | 'failed';

type OptimizerFeeSettlementMode = 'wallet' | 'backend';

type ResolvedFeeToken = {
  chainKey: string;
  symbol: string;
  decimals: number;
  tokenAddress: string;
  isNative: boolean;
  stellarAsset: string;
};

type EvmFeeSettlementPayload = {
  chainKey: string;
  chainId: number | null;
  payerAddress: string;
  collectorAddress: string;
  assetSymbol: string;
  tokenAddress: string;
  tokenDecimals: number;
  amount: string;
  amountBaseUnits: string;
  transaction: EvmTransactionRequest;
};

type StellarFeeSettlementPayload = {
  chainKey: string;
  payerAddress: string;
  collectorAddress: string;
  assetSymbol: string;
  asset: string;
  amount: string;
  fee: string;
  xdr: string;
  network: 'testnet' | 'mainnet';
};

const STELLAR_CHAIN_KEY = 'stellar';
type StellarNetwork = 'testnet' | 'mainnet';
const STELLAR_USDC_ISSUER =
  'GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5';
const ERC20_TRANSFER_ABI = ['function transfer(address to, uint256 value) returns (bool)'];
const ERC20_APPROVE_ABI = ['function approve(address spender, uint256 amount) returns (bool)'];
const CIRCLE_BRIDGE_API_DEFAULT_BASE_URL = 'https://api.bridging.circle.com/v2';
const CIRCLE_BRIDGE_API_CHAIN_KEY_ALIASES: Record<string, string> = {
  ethereum: 'ethereum',
  eth: 'ethereum',
  arbitrum: 'arbitrum',
  arb: 'arbitrum',
  avalanche: 'avalanche',
  avax: 'avalanche',
  base: 'base',
  optimism: 'optimism',
  op: 'optimism',
  polygon: 'polygon',
  matic: 'polygon',
  linea: 'linea',
  unichain: 'unichain',
  world: 'worldchain',
  worldchain: 'worldchain',
  sonic: 'sonic',
  aptos: 'aptos',
  solana: 'solana',
  sui: 'sui',
  immutablex: 'immutablex',
  immutable: 'immutablex',
  noble: 'noble',
  codex: 'codex',
  katana: 'katana',
  hyperliquid: 'hyperliquid',
  xrplevm: 'xrplevm',
  lisk: 'lisk',
  megalith: 'megalith',
  polygonzkevm: 'polygonzkevm',
  'polygon-zkevm': 'polygonzkevm',
  plasma: 'plasma',
  monad: 'monad',
  monadtestnet: 'monad-testnet',
  'monad-testnet': 'monad-testnet',
  arbitrumsepolia: 'arbitrum-sepolia',
  'arbitrum-sepolia': 'arbitrum-sepolia',
  avaxfuji: 'avalanche-fuji',
  'avalanche-fuji': 'avalanche-fuji',
  basesepolia: 'base-sepolia',
  'base-sepolia': 'base-sepolia',
  ethereumsepolia: 'ethereum-sepolia',
  'ethereum-sepolia': 'ethereum-sepolia',
  lineasepolia: 'linea-sepolia',
  'linea-sepolia': 'linea-sepolia',
  monadtestnet2: 'monad-testnet-2',
  'monad-testnet-2': 'monad-testnet-2',
  megalithtestnet: 'megalith-testnet',
  'megalith-testnet': 'megalith-testnet',
  polygonamoy: 'polygon-amoy',
  'polygon-amoy': 'polygon-amoy',
  seismictestnet: 'seismic-testnet',
  'seismic-testnet': 'seismic-testnet',
  seievmtestnet: 'sei-evm-testnet',
  'sei-evm-testnet': 'sei-evm-testnet',
  unichaintestnet: 'unichain-testnet',
  'unichain-testnet': 'unichain-testnet',
};

const DEFAULT_BORROW_PROTOCOL_CALL_TEMPLATES: Record<string, OptimizerProtocolCallTemplate> = {
  '*:*': {
    executionType: 'synthetic',
  },
  'stellar:blend-pools-v2': {
    executionType: 'external',
    externalUrl: 'https://www.blend.capital/',
  },
  'stellar:blend': {
    executionType: 'external',
    externalUrl: 'https://www.blend.capital/',
  },
};

@Injectable()
export class DeFiService {
  private readonly logger = new Logger(DeFiService.name);
  private readonly server: StellarSdk.Horizon.Server;
  private readonly testnetServer: StellarSdk.Horizon.Server;
  private readonly mainnetServer: StellarSdk.Horizon.Server;
  private readonly yieldCacheTtlMs = 2 * 60 * 1000;
  private readonly defiLlamaYieldsEnabled =
    process.env.DEFI_LLAMA_YIELDS_ENABLED === undefined
      ? true
      : this.resolveBooleanEnv(process.env.DEFI_LLAMA_YIELDS_ENABLED);
  private readonly defiLlamaYieldsUrl =
    process.env.DEFI_LLAMA_YIELDS_URL?.trim() || 'https://yields.llama.fi/pools';
  private readonly defiLlamaProtocolsUrl =
    process.env.DEFI_LLAMA_PROTOCOLS_URL?.trim() || 'https://api.llama.fi/protocols';
  private readonly defiLlamaIconsBaseUrl =
    process.env.DEFI_LLAMA_ICONS_BASE_URL?.trim() || 'https://icons.llama.fi';
  private readonly defiLlamaApiTimeoutMs = this.resolveDefiLlamaApiTimeoutMs(
    process.env.DEFI_LLAMA_API_TIMEOUT_MS,
  );
  private readonly defiLlamaMaxPools = this.resolveDefiLlamaMaxPools(
    process.env.DEFI_LLAMA_MAX_POOLS,
  );
  private readonly defiLlamaProtocolsCacheTtlMs = 30 * 60 * 1000;
  private readonly stargateApiBaseUrl =
    process.env.STARGATE_API_BASE_URL || 'https://stargate.finance/api/v1';
  private readonly stargateCacheTtlMs = this.resolveStargateCacheTtl(
    process.env.STARGATE_CACHE_TTL_MS,
  );
  private readonly stargateRpcUrls = this.resolveStargateRpcUrls(
    process.env.STARGATE_EVM_RPC_URLS,
  );
  private readonly bridgeExecutorEnabled = this.resolveBooleanEnv(
    process.env.BRIDGE_EXECUTOR_ENABLED,
  );
  private readonly bridgeExecutorPrivateKey = process.env.BRIDGE_EXECUTOR_PRIVATE_KEY;
  private readonly bridgeExecutorPrivateKeys = this.resolveBridgeExecutorPrivateKeys(
    process.env.BRIDGE_EXECUTOR_PRIVATE_KEYS,
  );
  private readonly bridgeExecutionReceiptTimeoutMs = this.resolveBridgeExecutionReceiptTimeoutMs(
    process.env.BRIDGE_EXECUTION_RECEIPT_TIMEOUT_MS,
  );
  private readonly allowExternalBridgeRoutes = this.resolveBooleanEnv(
    process.env.BRIDGE_ALLOW_EXTERNAL_ROUTES,
  );
  private readonly stargateExternalFallbackBaseUrl =
    process.env.STARGATE_EXTERNAL_FALLBACK_BASE_URL || 'https://stargate.finance/transfer';
  private readonly stargateExternalFallbackProviderName =
    process.env.STARGATE_EXTERNAL_FALLBACK_PROVIDER_NAME || 'stargate-external';
  private readonly stellarBridgeBaseUrl = process.env.STELLAR_BRIDGE_BASE_URL;
  private readonly stellarBridgeProviderName =
    process.env.STELLAR_BRIDGE_PROVIDER_NAME || 'stellar-external';
  private readonly stellarBridgeFeeBps = this.resolveStellarBridgeFeeBps(
    process.env.STELLAR_BRIDGE_FEE_BPS,
  );
  private readonly stellarCircleCctpV2BaseUrl = process.env.STELLAR_CIRCLE_CCTP_V2_BASE_URL;
  private readonly stellarCircleCctpV2TxApiUrl =
    process.env.STELLAR_CIRCLE_CCTP_V2_TX_API_URL || this.stellarCircleCctpV2BaseUrl;
  private readonly stellarCircleCctpV2UpstreamTxApiUrl =
    process.env.STELLAR_CIRCLE_CCTP_V2_UPSTREAM_TX_API_URL;
  private readonly stellarCircleCctpV2ApiKey = process.env.STELLAR_CIRCLE_CCTP_V2_API_KEY;
  private readonly stellarCircleCctpV2UpstreamApiKey =
    process.env.STELLAR_CIRCLE_CCTP_V2_UPSTREAM_API_KEY || this.stellarCircleCctpV2ApiKey;
  private readonly stellarCircleCctpV2ApiTimeoutMs = this.resolveStellarCctpApiTimeout(
    process.env.STELLAR_CIRCLE_CCTP_V2_API_TIMEOUT_MS,
  );
  private readonly stellarCircleCctpV2ProviderName =
    process.env.STELLAR_CIRCLE_CCTP_V2_PROVIDER_NAME || 'circle-cctp-v2';
  private readonly stellarCircleCctpV2FeeBps = this.resolveStellarBridgeFeeBps(
    process.env.STELLAR_CIRCLE_CCTP_V2_FEE_BPS,
  );
  private readonly optimizerFeeCollectorAddress = process.env.OPTIMIZER_FEE_COLLECTOR_ADDRESS;
  private readonly optimizerFeeCollectorAddresses = this.resolveAddressMap(
    process.env.OPTIMIZER_FEE_COLLECTOR_ADDRESSES,
  );
  private readonly optimizerFeeSettleOnBackendDefault = this.resolveBooleanEnv(
    process.env.OPTIMIZER_FEE_SETTLE_ON_BACKEND,
  );
  private readonly optimizerSyntheticPositionEnabled = this.resolveBooleanEnv(
    process.env.OPTIMIZER_ENABLE_SYNTHETIC_POSITION,
  );
  private readonly optimizerProtocolExecutorEnabled = this.resolveBooleanEnv(
    process.env.OPTIMIZER_PROTOCOL_EXECUTOR_ENABLED,
  );
  private readonly optimizerBridgeToExecutorForAutoDeposit = this.resolveBooleanEnv(
    process.env.OPTIMIZER_BRIDGE_TO_EXECUTOR_FOR_AUTO_DEPOSIT,
  );
  private readonly optimizerProtocolCallTemplates = this.resolveOptimizerProtocolCallTemplates(
    process.env.OPTIMIZER_PROTOCOL_CALL_TEMPLATES,
  );
  private readonly borrowProtocolExecutorEnabled =
    process.env.BORROW_PROTOCOL_EXECUTOR_ENABLED === undefined
      ? this.optimizerProtocolExecutorEnabled
      : this.resolveBooleanEnv(process.env.BORROW_PROTOCOL_EXECUTOR_ENABLED);
  private readonly borrowSyntheticExecutionEnabled =
    process.env.BORROW_SYNTHETIC_EXECUTION_ENABLED === undefined
      ? true
      : this.resolveBooleanEnv(process.env.BORROW_SYNTHETIC_EXECUTION_ENABLED);
  private readonly borrowProtocolCallTemplates = this.resolveOptimizerProtocolCallTemplates(
    process.env.BORROW_PROTOCOL_CALL_TEMPLATES || process.env.OPTIMIZER_PROTOCOL_CALL_TEMPLATES,
  );
  private readonly optimizerFeeStellarSecret = process.env.OPTIMIZER_FEE_STELLAR_SECRET;
  private yieldsCache: { timestamp: number; data: YieldOpportunity[] } | null = null;
  private defiLlamaProtocolLogoCache: {
    timestamp: number;
    data: DefiLlamaProtocolLogoLookup;
  } | null = null;
  private defiLlamaProtocolLogoInFlight: Promise<DefiLlamaProtocolLogoLookup> | null = null;
  private stargateChainsCache: { timestamp: number; data: StargateChain[] } | null = null;
  private stargateTokensCache: { timestamp: number; data: StargateToken[] } | null = null;

  constructor(
    @InjectModel(DeFiPosition.name) private defiPositionModel: Model<DeFiPositionDocument>,
    @InjectModel(BridgeHistory.name) private bridgeHistoryModel: Model<BridgeHistoryDocument>,
    @InjectModel(OptimizerExecution.name)
    private optimizerExecutionModel: Model<OptimizerExecutionDocument>,
    @InjectModel(OptimizerFeeSettlement.name)
    private optimizerFeeSettlementModel: Model<OptimizerFeeSettlementDocument>,
    @InjectModel(Wallet.name) private walletModel: Model<WalletDocument>,
    @InjectModel(Transaction.name) private transactionModel: Model<TransactionDocument>,
    @InjectModel(ConnectedWallet.name)
    private connectedWalletModel: Model<ConnectedWalletDocument>,
    private feeEstimationService: FeeEstimationService,
    private platformFeeService: PlatformFeeService,
    private accessControlService: AccessControlService,
    private marketService: MarketService,
    private circleCctpV2CustomTxBuilderService: CircleCctpV2CustomTxBuilderService,
  ) {
    const configuredNetwork = this.resolveStellarNetwork(process.env.STELLAR_NETWORK);
    const testnetHorizonUrl =
      process.env.STELLAR_HORIZON_URL_TESTNET ||
      (configuredNetwork === 'testnet' ? process.env.STELLAR_HORIZON_URL : undefined) ||
      'https://horizon-testnet.stellar.org';
    const mainnetHorizonUrl =
      process.env.STELLAR_HORIZON_URL_MAINNET ||
      (configuredNetwork === 'mainnet' || configuredNetwork === 'public'
        ? process.env.STELLAR_HORIZON_URL
        : undefined) ||
      'https://horizon.stellar.org';
    const horizonUrl =
      process.env.STELLAR_HORIZON_URL ||
      (configuredNetwork === 'mainnet' || configuredNetwork === 'public'
        ? mainnetHorizonUrl
        : testnetHorizonUrl);

    this.server = new StellarSdk.Horizon.Server(horizonUrl);
    this.testnetServer = new StellarSdk.Horizon.Server(testnetHorizonUrl);
    this.mainnetServer = new StellarSdk.Horizon.Server(mainnetHorizonUrl);
  }

  async getPositions(walletId: string) {
    return this.defiPositionModel.find({ walletId } as any).exec();
  }

  async getYieldOpportunities(
    chain?: string,
    category?: string,
    options?: { limit?: number },
  ): Promise<YieldOpportunity[]> {
    const limit =
      typeof options?.limit === 'number' && Number.isFinite(options.limit)
        ? Math.max(Math.floor(options.limit), 0)
        : 120;
    const now = Date.now();
    if (this.yieldsCache && now - this.yieldsCache.timestamp < this.yieldCacheTtlMs) {
      return this.filterYields(this.yieldsCache.data, chain, category, limit);
    }

    const [defiLlamaResult, stellarResult] = await Promise.allSettled([
      this.getDefiLlamaYieldOpportunities(),
      this.getStellarYieldOpportunities(),
    ]);

    const merged: YieldOpportunity[] = [];

    if (defiLlamaResult.status === 'fulfilled') {
      merged.push(...defiLlamaResult.value);
    } else if (this.defiLlamaYieldsEnabled) {
      this.logger.warn(
        `Failed to fetch DeFiLlama yield opportunities: ${this.errorMessage(defiLlamaResult.reason)}`,
      );
    }

    if (stellarResult.status === 'fulfilled') {
      merged.push(...stellarResult.value);
    } else {
      this.logger.warn(
        `Failed to fetch Stellar yield opportunities: ${this.errorMessage(stellarResult.reason)}`,
      );
    }

    if (merged.length === 0) {
      return [];
    }

    const normalized = this.mergeYieldOpportunities(merged);

    this.yieldsCache = {
      timestamp: now,
      data: normalized,
    };

    return this.filterYields(normalized, chain, category, limit);
  }

  async createOptimizerPlan(userId: string, dto: CreateOptimizerPlanDto) {
    const accessProfile = await this.accessControlService.assertFeatureForUser(
      userId,
      'optimizer',
    );
    const assetSymbol = dto.assetSymbol.trim().toUpperCase();
    const amount = this.normalizeDecimal(dto.amount);
    const amountNumber = Number(amount);
    const inAppOnly = dto.inAppOnly ?? false;
    if (!Number.isFinite(amountNumber) || amountNumber <= 0) {
      throw new BadRequestException('amount must be greater than zero');
    }

    const performanceFeeBps = dto.performanceFeeBps ?? 1000;
    const strategyName = dto.strategyName?.trim() || `${assetSymbol} Yield Optimizer`;

    const [yields, bridgeChains, wallets, connectedWallets] = await Promise.all([
      this.getYieldOpportunities(undefined, dto.category, { limit: 0 }),
      this.getBridgeChains(),
      this.walletModel.find({ userId, isArchived: false } as any).lean().exec(),
      this.connectedWalletModel.find({ userId } as any).lean().exec(),
    ]);

    const bridgeChainLookup = this.createBridgeChainLookup(bridgeChains);
    const chainNameByKey = new Map(
      bridgeChains.map((chain) => [chain.chainKey.toLowerCase(), chain.name]),
    );

    let enrichedYields = yields
      .filter((opportunity) => this.yieldMatchesAsset(opportunity.asset, assetSymbol))
      .map((opportunity) => ({
        opportunity,
        chainKey: this.resolveBridgeChainKeyForYield(opportunity.chain, bridgeChainLookup),
      }))
      .filter(
        (
          item,
        ): item is {
          opportunity: YieldOpportunity;
          chainKey: string;
        } => Boolean(item.chainKey),
      );

    if (enrichedYields.length === 0) {
      const fallbackOpportunity = this.resolvePreferredOptimizerOpportunity(
        dto,
        assetSymbol,
        bridgeChainLookup,
      );
      if (fallbackOpportunity) {
        enrichedYields = [fallbackOpportunity];
        this.logger.warn(
          `Using preferred optimizer opportunity fallback for ${assetSymbol}: ${fallbackOpportunity.opportunity.protocol} on ${fallbackOpportunity.chainKey}`,
        );
      }
    }

    if (enrichedYields.length === 0) {
      throw new BadRequestException(
        `No supported yield opportunities found for asset "${assetSymbol}"`,
      );
    }

    let sortedYields = [...enrichedYields].sort((left, right) => {
      if (right.opportunity.apy !== left.opportunity.apy) {
        return right.opportunity.apy - left.opportunity.apy;
      }
      return right.opportunity.tvl - left.opportunity.tvl;
    });

    const preferredProtocol = this.normalizeAddressString(dto.preferredProtocol);
    const preferredChain = this.normalizeAddressString(dto.preferredChain);
    const preferredProtocolKey = preferredProtocol
      ? this.normalizeProtocolKey(preferredProtocol)
      : null;
    const preferredChainKey = preferredChain
      ? this.resolveBridgeChainKeyForYield(preferredChain, bridgeChainLookup) ||
        this.normalizeChainKeyHint(preferredChain)
      : null;

    if (preferredProtocolKey || preferredChainKey) {
      const preferredIndex = sortedYields.findIndex((entry) => {
        const protocolMatch = preferredProtocolKey
          ? this.normalizeProtocolKey(entry.opportunity.protocol) === preferredProtocolKey
          : true;
        const chainMatch = preferredChainKey ? entry.chainKey === preferredChainKey : true;
        return protocolMatch && chainMatch;
      });

      if (preferredIndex > 0) {
        const [preferredEntry] = sortedYields.splice(preferredIndex, 1);
        sortedYields = [preferredEntry, ...sortedYields];
      }
    }

    const rankedYields = await this.applyOptimizerProtocolLimit(
      userId,
      sortedYields,
      accessProfile.limits.maxProtocols,
    );

    const walletRecords = wallets as Array<Record<string, unknown>>;
    const connectedWalletRecords = connectedWallets as Array<Record<string, unknown>>;
    const balances = this.resolveOptimizerBalanceCandidates(
      walletRecords,
      connectedWalletRecords,
      {
        assetSymbol,
        sourceChainKey: dto.sourceChainKey,
        sourceAddress: dto.sourceAddress,
        sourceBalance: dto.sourceBalance,
      },
    );

    const preferredSource = this.selectOptimizerSourceCandidate(
      balances,
      dto.sourceChainKey,
      amountNumber,
    );

    const preferredChainFromDto = this.normalizeChainKeyHint(dto.sourceChainKey);
    const explicitSourceAddress = this.normalizeAddressString(dto.sourceAddress);
    const explicitDestinationAddress = this.normalizeAddressString(dto.destinationAddress);
    const isAddressCompatibleWithChain = (
      address: string | null,
      chainKey: string,
    ): boolean => {
      if (!address) {
        return false;
      }
      const normalizedChainKey =
        this.normalizeChainKeyHint(chainKey) || chainKey.trim().toLowerCase();
      if (normalizedChainKey === STELLAR_CHAIN_KEY) {
        return this.looksLikeStellarAddress(address);
      }
      return Boolean(this.normalizeAddressIfPossible(address));
    };
    const resolveSourceAddress = (sourceChainKey: string): string | null => {
      if (isAddressCompatibleWithChain(explicitSourceAddress, sourceChainKey)) {
        return explicitSourceAddress;
      }
      if (preferredSource?.chainKey === sourceChainKey && preferredSource.address) {
        return preferredSource.address;
      }
      return this.resolveConnectedWalletAddress(connectedWalletRecords, sourceChainKey) || null;
    };
    const canReuseSourceAddressForDestination = (
      sourceAddress: string | null,
      destinationChainKey: string,
    ): boolean => {
      return isAddressCompatibleWithChain(sourceAddress, destinationChainKey);
    };
    const resolveDestinationAddress = (
      sourceAddress: string | null,
      sourceChainKey: string,
      destinationChainKey: string,
    ): string | null => {
      if (
        isAddressCompatibleWithChain(explicitDestinationAddress, destinationChainKey)
      ) {
        return explicitDestinationAddress;
      }
      const normalizedSourceChainKey =
        this.normalizeChainKeyHint(sourceChainKey) || sourceChainKey.trim().toLowerCase();
      const normalizedDestinationChainKey =
        this.normalizeChainKeyHint(destinationChainKey) || destinationChainKey.trim().toLowerCase();
      if (
        normalizedSourceChainKey === normalizedDestinationChainKey &&
        canReuseSourceAddressForDestination(sourceAddress, destinationChainKey)
      ) {
        return sourceAddress;
      }
      const connectedDestinationAddress =
        this.resolveConnectedWalletAddress(connectedWalletRecords, destinationChainKey) || null;
      if (connectedDestinationAddress) {
        return connectedDestinationAddress;
      }
      if (canReuseSourceAddressForDestination(sourceAddress, destinationChainKey)) {
        return sourceAddress;
      }
      return null;
    };

    let bestYield = rankedYields[0];
    let destinationChainKey = bestYield.chainKey;
    let sourceChainKey =
      preferredSource?.chainKey ||
      preferredChainFromDto ||
      destinationChainKey;
    let sourceAddress = resolveSourceAddress(sourceChainKey);
    let destinationAddress = resolveDestinationAddress(
      sourceAddress,
      sourceChainKey,
      destinationChainKey,
    );
    let routing = await this.buildOptimizerRoutingPlan({
      sourceChainKey,
      destinationChainKey,
      sourceAddress,
      destinationAddress,
      assetSymbol,
      amount,
      chainNameByKey,
    });

    if (routing.status !== 'ready' && routing.required) {
      const maxCandidateChecks = Math.min(rankedYields.length, 12);
      for (const candidate of rankedYields.slice(1, maxCandidateChecks)) {
        const candidateDestinationChainKey = candidate.chainKey;
        const candidateSourceChainKey =
          preferredSource?.chainKey ||
          preferredChainFromDto ||
          candidateDestinationChainKey;
        const candidateSourceAddress = resolveSourceAddress(candidateSourceChainKey);
        const candidateDestinationAddress = resolveDestinationAddress(
          candidateSourceAddress,
          candidateSourceChainKey,
          candidateDestinationChainKey,
        );
        const candidateRouting = await this.buildOptimizerRoutingPlan({
          sourceChainKey: candidateSourceChainKey,
          destinationChainKey: candidateDestinationChainKey,
          sourceAddress: candidateSourceAddress,
          destinationAddress: candidateDestinationAddress,
          assetSymbol,
          amount,
          chainNameByKey,
        });

        if (!candidateRouting.required || candidateRouting.status === 'ready') {
          bestYield = candidate;
          destinationChainKey = candidateDestinationChainKey;
          sourceChainKey = candidateSourceChainKey;
          sourceAddress = candidateSourceAddress;
          destinationAddress = candidateDestinationAddress;
          routing = candidateRouting;
          break;
        }
      }
    }

    if (inAppOnly) {
      const maxCandidateChecks = Math.min(rankedYields.length, 12);
      let inAppSelection:
        | {
            yieldEntry: (typeof rankedYields)[number];
            sourceChainKey: string;
            sourceAddress: string | null;
            destinationAddress: string | null;
            routing: Record<string, unknown>;
          }
        | null = null;

      for (const candidate of rankedYields.slice(0, maxCandidateChecks)) {
        const candidateDestinationChainKey = candidate.chainKey;
        const candidateSourceChainKey =
          preferredSource?.chainKey ||
          preferredChainFromDto ||
          candidateDestinationChainKey;
        const candidateSourceAddress = resolveSourceAddress(candidateSourceChainKey);
        const candidateDestinationAddress = resolveDestinationAddress(
          candidateSourceAddress,
          candidateSourceChainKey,
          candidateDestinationChainKey,
        );
        const candidateRouting = await this.buildOptimizerRoutingPlan({
          sourceChainKey: candidateSourceChainKey,
          destinationChainKey: candidateDestinationChainKey,
          sourceAddress: candidateSourceAddress,
          destinationAddress: candidateDestinationAddress,
          assetSymbol,
          amount,
          chainNameByKey,
        });
        const candidateRequiresBridge =
          candidateSourceChainKey !== candidateDestinationChainKey;

        if (
          !this.isOptimizerRoutingInAppCapable(candidateRouting) ||
          !this.isOptimizerProtocolInAppCapable({
            destinationChainKey: candidateDestinationChainKey,
            protocol: candidate.opportunity.protocol,
            requiresBridge: candidateRequiresBridge,
          })
        ) {
          continue;
        }

        inAppSelection = {
          yieldEntry: candidate,
          sourceChainKey: candidateSourceChainKey,
          sourceAddress: candidateSourceAddress,
          destinationAddress: candidateDestinationAddress,
          routing: candidateRouting,
        };
        break;
      }

      if (!inAppSelection) {
        throw new BadRequestException(
          `No fully in-app executable optimizer route is currently available for ${assetSymbol}. ` +
            'Try connecting a different source-chain wallet or reduce the amount.',
        );
      }

      bestYield = inAppSelection.yieldEntry;
      destinationChainKey = inAppSelection.yieldEntry.chainKey;
      sourceChainKey = inAppSelection.sourceChainKey;
      sourceAddress = inAppSelection.sourceAddress;
      destinationAddress = inAppSelection.destinationAddress;
      routing = inAppSelection.routing;
    }

    const currentChainBest = rankedYields.find((item) => item.chainKey === sourceChainKey);
    const baselineApy = currentChainBest?.opportunity.apy || 0;
    const optimizedApy = bestYield.opportunity.apy;
    const grossBoostApy = Math.max(optimizedApy - baselineApy, 0);
    const feeShareOnBoostApy = grossBoostApy * (performanceFeeBps / 10000);
    const userNetApy = baselineApy + Math.max(grossBoostApy - feeShareOnBoostApy, 0);

    const baselineAnnual = amountNumber * (baselineApy / 100);
    const optimizedGrossAnnual = amountNumber * (optimizedApy / 100);
    const optimizedUpliftAnnual = Math.max(optimizedGrossAnnual - baselineAnnual, 0);
    const performanceFeeAnnual = optimizedUpliftAnnual * (performanceFeeBps / 10000);
    const optimizedNetAnnual = optimizedGrossAnnual - performanceFeeAnnual;

    const sourceAvailableAmount =
      typeof preferredSource?.availableAmount === 'number'
        ? preferredSource.availableAmount
        : null;
    const hasSufficientBalance =
      sourceAvailableAmount === null ? null : sourceAvailableAmount >= amountNumber;
    const requiresBridge = sourceChainKey !== destinationChainKey;

    const flowSteps = [
      {
        index: 1,
        title: 'User selects strategy',
        detail: `Strategy "${strategyName}" selected for ${assetSymbol}.`,
      },
      {
        index: 2,
        title: inAppOnly
          ? 'Yielder analyzes best in-app yield'
          : 'Yielder analyzes best yield',
        detail: `Best APY found on ${chainNameByKey.get(destinationChainKey) || destinationChainKey}: ${bestYield.opportunity.apy.toFixed(2)}%.`,
      },
      {
        index: 3,
        title: 'Check user balance',
        detail:
          sourceAvailableAmount === null
            ? `Balance check for ${assetSymbol} on ${sourceChainKey} is unavailable from cache.`
            : `Detected ${sourceAvailableAmount.toFixed(4)} ${assetSymbol} on ${sourceChainKey}.`,
      },
      {
        index: 4,
        title: 'Auto-route bridge',
        detail: requiresBridge
          ? `${sourceChainKey} -> ${destinationChainKey} via ${String(routing.route || 'best available bridge route')}.`
          : `Bridge not required. Funds already on ${destinationChainKey}.`,
      },
      {
        index: 5,
        title: 'Execute deposit',
        detail: inAppOnly
          ? `Deposit ${assetSymbol} into ${bestYield.opportunity.protocol} directly in-app on ${destinationChainKey}.`
          : `Deposit ${assetSymbol} into ${bestYield.opportunity.protocol} on ${destinationChainKey}.`,
      },
      {
        index: 6,
        title: 'Dashboard update',
        detail: `Show the position in ${destinationChainKey} tab with a cross-chain badge.`,
      },
    ];

    return {
      strategy: {
        name: strategyName,
        assetSymbol,
        amount,
        category: dto.category || null,
      },
      recommendation: {
        protocol: bestYield.opportunity.protocol,
        chain: bestYield.opportunity.chain,
        chainKey: destinationChainKey,
        asset: bestYield.opportunity.asset,
        category: bestYield.opportunity.category,
        apy: bestYield.opportunity.apy,
        tvl: bestYield.opportunity.tvl,
        risk: bestYield.opportunity.risk,
        riskScore: bestYield.opportunity.riskScore,
        protocolUrl: this.buildStellarProtocolUrl(bestYield.opportunity.protocol),
      },
      balanceCheck: {
        sourceChainKey,
        sourceAddress,
        assetSymbol,
        availableAmount:
          typeof sourceAvailableAmount === 'number' ? sourceAvailableAmount.toFixed(8) : null,
        hasSufficientBalance,
        source: preferredSource?.source || 'unknown',
      },
      routing,
      performanceFee: {
        performanceFeeBps,
        baselineApy: this.roundTo4Decimals(baselineApy),
        optimizedApy: this.roundTo4Decimals(optimizedApy),
        grossBoostApy: this.roundTo4Decimals(grossBoostApy),
        feeShareOnBoostApy: this.roundTo4Decimals(feeShareOnBoostApy),
        userNetApy: this.roundTo4Decimals(userNetApy),
        estimatedAnnual: {
          principal: this.roundTo4Decimals(amountNumber),
          baseline: this.roundTo4Decimals(baselineAnnual),
          optimizedGross: this.roundTo4Decimals(optimizedGrossAnnual),
          uplift: this.roundTo4Decimals(optimizedUpliftAnnual),
          performanceFee: this.roundTo4Decimals(performanceFeeAnnual),
          optimizedNet: this.roundTo4Decimals(optimizedNetAnnual),
        },
      },
      flow: flowSteps,
      generatedAt: new Date().toISOString(),
    };
  }

  private async buildOptimizerRoutingPlan(params: {
    sourceChainKey: string;
    destinationChainKey: string;
    sourceAddress: string | null;
    destinationAddress: string | null;
    assetSymbol: string;
    amount: string;
    chainNameByKey: Map<string, string>;
  }): Promise<Record<string, unknown>> {
    const requiresBridge = params.sourceChainKey !== params.destinationChainKey;
    const routing: Record<string, unknown> = {
      required: requiresBridge,
      sourceChainKey: params.sourceChainKey,
      destinationChainKey: params.destinationChainKey,
      sourceAddress: params.sourceAddress,
      destinationAddress: params.destinationAddress,
      sourceChain:
        params.chainNameByKey.get(params.sourceChainKey) || this.titleCase(params.sourceChainKey),
      destinationChain:
        params.chainNameByKey.get(params.destinationChainKey) ||
        this.titleCase(params.destinationChainKey),
      status: requiresBridge ? 'pending' : 'not-required',
      route: null,
      executionType: requiresBridge ? null : 'none',
      externalUrl: null,
      bridgeTransaction: null,
      quote: null,
      error: null,
    };

    if (!requiresBridge) {
      return routing;
    }

    if (!params.sourceAddress || !params.destinationAddress) {
      routing.status = 'unavailable';
      routing.error = 'Missing source/destination wallet address for bridge route';
      return routing;
    }

    try {
      const [srcToken, dstToken] = await Promise.all([
        this.resolveBridgeTokenIdentifierForSymbol(params.sourceChainKey, params.assetSymbol),
        this.resolveBridgeTokenIdentifierForSymbol(
          params.destinationChainKey,
          params.assetSymbol,
        ),
      ]);

      const quote = await this.getBridgeQuote({
        srcChainKey: params.sourceChainKey,
        dstChainKey: params.destinationChainKey,
        srcToken,
        dstToken,
        srcAddress: params.sourceAddress,
        dstAddress: params.destinationAddress,
        srcAmount: params.amount,
        slippageBps: 100,
      });
      const preferredQuote = this.selectPreferredBridgeQuote(
        quote.quotes,
        quote.recommendedRoute || undefined,
      );

      if (!preferredQuote) {
        routing.status = 'unavailable';
        routing.error = 'No executable bridge route found for this amount';
        return routing;
      }

      const bridgeTransaction = await this.buildBridgeTransaction({
        srcChainKey: params.sourceChainKey,
        dstChainKey: params.destinationChainKey,
        srcToken,
        dstToken,
        srcAddress: params.sourceAddress,
        dstAddress: params.destinationAddress,
        srcAmount: params.amount,
        route: preferredQuote.route,
      });

      routing.status = 'ready';
      routing.route = preferredQuote.route;
      routing.executionType = preferredQuote.executionType;
      routing.externalUrl = preferredQuote.externalUrl;
      routing.bridgeTransaction = bridgeTransaction;
      routing.quote = {
        route: preferredQuote.route,
        srcAmount: preferredQuote.srcAmount,
        dstAmount: preferredQuote.dstAmount,
        dstAmountMin: preferredQuote.dstAmountMin,
        estimatedDurationSeconds: preferredQuote.estimatedDurationSeconds,
        fees: preferredQuote.fees,
      };
      return routing;
    } catch (error: unknown) {
      routing.status = 'unavailable';
      routing.error = this.errorMessage(error);
      return routing;
    }
  }

  private isOptimizerRoutingInAppCapable(routing: Record<string, unknown>): boolean {
    const requiresBridge = Boolean(routing.required);
    if (!requiresBridge) {
      return true;
    }

    if (!this.bridgeExecutorEnabled) {
      return false;
    }

    const status = typeof routing.status === 'string' ? routing.status : '';
    const executionType = typeof routing.executionType === 'string' ? routing.executionType : '';
    const sourceChainKey =
      typeof routing.sourceChainKey === 'string' ? routing.sourceChainKey.trim().toLowerCase() : '';

    if (status !== 'ready' || executionType !== 'evm' || !sourceChainKey) {
      return false;
    }

    const rpcUrl = this.resolveRpcUrlForChain(sourceChainKey);
    if (!rpcUrl) {
      return false;
    }

    try {
      this.resolveBridgeExecutorAddress(sourceChainKey);
      return true;
    } catch {
      return false;
    }
  }

  private isOptimizerProtocolInAppCapable(params: {
    destinationChainKey: string;
    protocol: string;
    requiresBridge: boolean;
  }): boolean {
    const normalizedDestinationChainKey =
      this.normalizeChainKeyHint(params.destinationChainKey) ||
      params.destinationChainKey.trim().toLowerCase();
    if (normalizedDestinationChainKey === STELLAR_CHAIN_KEY) {
      // Stellar routes can complete in-app via wallet-signed deposit tx flow.
      return true;
    }

    if (this.optimizerSyntheticPositionEnabled) {
      return true;
    }

    if (!this.optimizerProtocolExecutorEnabled) {
      return false;
    }

    const resolvedTemplate = this.resolveOptimizerProtocolCallTemplate(
      params.destinationChainKey,
      params.protocol,
    );
    if (!resolvedTemplate) {
      return false;
    }

    const templateExecutionType = (
      resolvedTemplate.template.executionType ||
      (resolvedTemplate.template.externalUrl ? 'external' : 'evm')
    )
      .trim()
      .toLowerCase();
    if (templateExecutionType !== 'evm') {
      return false;
    }

    if (!this.resolveRpcUrlForChain(normalizedDestinationChainKey)) {
      return false;
    }

    try {
      this.resolveBridgeExecutorAddress(normalizedDestinationChainKey);
    } catch {
      return false;
    }

    const requiredSignerMatch = resolvedTemplate.template.requiredSignerMatch !== false;
    if (!requiredSignerMatch) {
      return true;
    }

    if (!params.requiresBridge) {
      return false;
    }

    return this.optimizerBridgeToExecutorForAutoDeposit;
  }

  async executeOptimizer(userId: string, dto: ExecuteOptimizerDto) {
    const accessProfile = await this.accessControlService.assertFeatureForUser(
      userId,
      'optimizer',
    );
    const inAppOnly = dto.inAppOnly ?? false;
    const autoExecuteBridge = inAppOnly ? true : dto.autoExecuteBridge ?? true;
    const executeBridgeOnBackend = inAppOnly ? true : dto.executeBridgeOnBackend ?? true;
    const openPositionImmediately =
      dto.openPositionImmediately ?? this.optimizerSyntheticPositionEnabled;
    const allowExternalBridgeRedirect = inAppOnly
      ? false
      : dto.allowExternalBridgeRedirect ?? true;
    const autoSettleFee = dto.autoSettleFee ?? false;
    const settleFeeOnBackend = dto.settleFeeOnBackend ?? this.optimizerFeeSettleOnBackendDefault;
    let protocolDepositTxHash = this.normalizeOptimizerDepositTxHash(dto.depositTxHash);

    const plan = await this.createOptimizerPlan(userId, dto);
    await this.assertOptimizerProtocolAllowed(
      userId,
      String(plan.recommendation.protocol || ''),
      accessProfile.limits.maxProtocols,
    );
    const routing = plan.routing as {
      required: boolean;
      status: string;
      route: string | null;
      executionType: 'evm' | 'external' | 'none' | null;
      sourceChainKey: string;
      destinationChainKey: string;
      sourceAddress: string | null;
      destinationAddress: string | null;
      externalUrl: string | null;
      error: string | null;
    };
    if (
      inAppOnly &&
      (!this.isOptimizerRoutingInAppCapable(plan.routing as Record<string, unknown>) ||
        !this.isOptimizerProtocolInAppCapable({
          destinationChainKey: routing.destinationChainKey,
          protocol: String(plan.recommendation.protocol || ''),
          requiresBridge: routing.required,
        }))
    ) {
      throw new BadRequestException(
        'No fully in-app executable optimizer route is available for this request.',
      );
    }

    let status: OptimizerExecutionStatus = 'planned';
    let bridgeTxHash: string | undefined;
    let approvalTxHash: string | undefined;
    let externalUrl: string | undefined;
    let positionId: string | undefined;
    let lastError: string | undefined;
    let feeSettlement: Record<string, unknown> | undefined;
    let protocolDepositStatus: OptimizerProtocolDepositResultStatus | 'none' = 'none';
    let protocolDepositMessage: string | undefined;
    let protocolDepositExternalUrl: string | undefined;
    const protocolDepositBeneficiaryAddress = routing.destinationAddress || routing.sourceAddress || null;
    let protocolDepositPayerAddress = protocolDepositBeneficiaryAddress;
    let bridgeDestinationAddress = routing.destinationAddress || routing.sourceAddress || null;

    const execution = await this.optimizerExecutionModel.create({
      userId,
      strategyName: plan.strategy.name,
      assetSymbol: plan.strategy.assetSymbol,
      amount: plan.strategy.amount,
      sourceChainKey: routing.sourceChainKey,
      destinationChainKey: routing.destinationChainKey,
      sourceAddress: routing.sourceAddress || undefined,
      destinationAddress: routing.destinationAddress || undefined,
      protocol: plan.recommendation.protocol,
      category: plan.recommendation.category,
      baselineApy: plan.performanceFee.baselineApy,
      optimizedApy: plan.performanceFee.optimizedApy,
      netApy: plan.performanceFee.userNetApy,
      performanceFeeBps: plan.performanceFee.performanceFeeBps,
      estimatedAnnualFee: plan.performanceFee.estimatedAnnual.performanceFee,
      estimatedAnnualNetYield: plan.performanceFee.estimatedAnnual.optimizedNet,
      flow: plan.flow,
      status,
      metadata: {
        recommendation: plan.recommendation,
      },
    } as Partial<OptimizerExecution>);

    try {
      if (routing.required) {
        if (routing.status !== 'ready') {
          status = 'failed';
          lastError = routing.error || 'Optimizer bridge route is unavailable';
          throw new BadRequestException(lastError);
        }

        if (!autoExecuteBridge) {
          status = 'wallet-action-required';
        } else if (routing.executionType === 'external') {
          if (!allowExternalBridgeRedirect) {
            status = 'failed';
            lastError = 'Selected route requires external redirect and redirects are disabled';
            throw new BadRequestException(lastError);
          }
          externalUrl = routing.externalUrl || undefined;
          bridgeTxHash = this.generateOptimizerExternalReference();
          status = 'bridge-external';
        } else if (routing.executionType === 'evm') {
          if (!routing.sourceAddress || !routing.destinationAddress) {
            status = 'failed';
            lastError = 'Bridge execution requires both source and destination addresses';
            throw new BadRequestException(lastError);
          }

          const [srcToken, dstToken] = await Promise.all([
            this.resolveBridgeTokenIdentifierForSymbol(
              routing.sourceChainKey,
              plan.strategy.assetSymbol,
            ),
            this.resolveBridgeTokenIdentifierForSymbol(
              routing.destinationChainKey,
              plan.strategy.assetSymbol,
            ),
          ]);

          if (
            executeBridgeOnBackend &&
            openPositionImmediately &&
            !this.optimizerSyntheticPositionEnabled &&
            this.optimizerProtocolExecutorEnabled &&
            this.optimizerBridgeToExecutorForAutoDeposit
          ) {
            try {
              const destinationExecutorAddress = this.resolveBridgeExecutorAddress(
                routing.destinationChainKey,
              );
              bridgeDestinationAddress = destinationExecutorAddress;
              protocolDepositPayerAddress = destinationExecutorAddress;
            } catch (error: unknown) {
              this.logger.warn(
                `Could not route bridge to destination executor for protocol auto-deposit: ${this.errorMessage(error)}`,
              );
            }
          }

          const bridgePayload: BuildBridgeTransactionDto = {
            srcChainKey: routing.sourceChainKey,
            dstChainKey: routing.destinationChainKey,
            srcToken,
            dstToken,
            srcAddress: routing.sourceAddress,
            dstAddress: bridgeDestinationAddress || routing.destinationAddress,
            srcAmount: plan.strategy.amount,
            ...(routing.route ? { route: routing.route } : {}),
            ...(executeBridgeOnBackend ? { executeOnBackend: true } : {}),
          };
          const bridgeExecutionPlan = (await this.buildBridgeTransaction(
            bridgePayload,
            userId,
          )) as {
            executionType: 'evm' | 'external';
            externalUrl: string | null;
            executedOnBackend?: boolean;
            bridgeTxHash?: string;
            approvalTxHash?: string;
          };

          if (bridgeExecutionPlan.executionType === 'external') {
            if (!allowExternalBridgeRedirect) {
              status = 'failed';
              lastError = 'Bridge fallback requires external redirect and redirects are disabled';
              throw new BadRequestException(lastError);
            }
            externalUrl = bridgeExecutionPlan.externalUrl || undefined;
            bridgeTxHash = this.generateOptimizerExternalReference();
            status = 'bridge-external';
          } else if (bridgeExecutionPlan.executedOnBackend && bridgeExecutionPlan.bridgeTxHash) {
            bridgeTxHash = bridgeExecutionPlan.bridgeTxHash;
            approvalTxHash = bridgeExecutionPlan.approvalTxHash;
            status = 'bridge-submitted';
          } else {
            status = 'wallet-action-required';
            lastError =
              'Bridge route requires wallet-side execution. Configure backend executor for full automation.';
          }
        }
      }

      if (bridgeTxHash && routing.required) {
        const bridgeMetadata: Record<string, unknown> = {};
        if (externalUrl) {
          bridgeMetadata.externalUrl = externalUrl;
        }
        if (
          bridgeDestinationAddress &&
          protocolDepositBeneficiaryAddress &&
          bridgeDestinationAddress !== protocolDepositBeneficiaryAddress
        ) {
          bridgeMetadata.protocolBeneficiaryAddress = protocolDepositBeneficiaryAddress;
          bridgeMetadata.protocolPayerAddress = bridgeDestinationAddress;
        }

        await this.recordBridgeHistory(userId, {
          srcChainKey: routing.sourceChainKey,
          dstChainKey: routing.destinationChainKey,
          srcAddress: routing.sourceAddress || '',
          dstAddress: bridgeDestinationAddress || routing.destinationAddress || routing.sourceAddress || '',
          srcTokenSymbol: plan.strategy.assetSymbol,
          dstTokenSymbol: plan.strategy.assetSymbol,
          srcAmount: plan.strategy.amount,
          ...(plan.routing.quote && typeof plan.routing.quote === 'object'
            ? {
                dstAmount: String(
                  (plan.routing.quote as { dstAmount?: string }).dstAmount || plan.strategy.amount,
                ),
                dstAmountMin: String(
                  (plan.routing.quote as { dstAmountMin?: string }).dstAmountMin ||
                    plan.strategy.amount,
                ),
              }
            : {}),
          ...(routing.route ? { route: routing.route } : {}),
          ...(approvalTxHash ? { approvalTxHash } : {}),
          bridgeTxHash,
          status: status === 'bridge-external' ? 'redirected' : 'submitted',
          ...(Object.keys(bridgeMetadata).length > 0 ? { metadata: bridgeMetadata } : {}),
        });
      }

      const canOpenSyntheticPosition =
        openPositionImmediately && this.optimizerSyntheticPositionEnabled;

      if (canOpenSyntheticPosition && status !== 'wallet-action-required') {
        const wallet = await this.resolveOrCreateOptimizerWallet(
          userId,
          routing.destinationChainKey,
          routing.destinationAddress || routing.sourceAddress,
        );
        positionId = await this.createOptimizerPosition(wallet._id as unknown as string, {
          strategyName: plan.strategy.name,
          protocol: plan.recommendation.protocol,
          category: plan.recommendation.category,
          assetSymbol: plan.strategy.assetSymbol,
          amount: plan.strategy.amount,
          sourceChainKey: routing.sourceChainKey,
          destinationChainKey: routing.destinationChainKey,
          sourceAddress: routing.sourceAddress,
          destinationAddress: routing.destinationAddress,
          bridgeTxHash,
          depositTxHash: protocolDepositTxHash,
          netApy: plan.performanceFee.userNetApy,
          estimatedAnnualNetYield: plan.performanceFee.estimatedAnnual.optimizedNet,
        });
        status = 'position-opened';
      } else if (
        openPositionImmediately &&
        !this.optimizerSyntheticPositionEnabled &&
        status !== 'wallet-action-required'
      ) {
        const protocolDepositResult = await this.attemptOptimizerProtocolDeposit({
          executionId: String((execution as any)._id || ''),
          protocol: plan.recommendation.protocol,
          sourceChainKey: routing.sourceChainKey,
          chainKey: routing.destinationChainKey,
          assetSymbol: plan.strategy.assetSymbol,
          amount: plan.strategy.amount,
          sourceAddress: routing.sourceAddress,
          destinationAddress: protocolDepositBeneficiaryAddress,
          payerAddress: protocolDepositPayerAddress,
          bridgeRequired: routing.required,
          bridgeStatus: status,
          bridgeTxHash,
          bridgeExternalUrl: externalUrl,
          autoExecute: true,
          depositTxHash: protocolDepositTxHash,
          allowExternalRedirect: allowExternalBridgeRedirect,
        });

        protocolDepositStatus = protocolDepositResult.status;
        protocolDepositMessage = protocolDepositResult.message;
        protocolDepositExternalUrl = protocolDepositResult.externalUrl;
        if (protocolDepositResult.depositTxHash) {
          protocolDepositTxHash = protocolDepositResult.depositTxHash;
        }

        if (protocolDepositResult.status === 'executed') {
          const wallet = await this.resolveOrCreateOptimizerWallet(
            userId,
            routing.destinationChainKey,
            protocolDepositBeneficiaryAddress,
          );
          positionId = await this.createOptimizerPosition(wallet._id as unknown as string, {
            strategyName: plan.strategy.name,
            protocol: plan.recommendation.protocol,
            category: plan.recommendation.category,
            assetSymbol: plan.strategy.assetSymbol,
            amount: plan.strategy.amount,
            sourceChainKey: routing.sourceChainKey,
            destinationChainKey: routing.destinationChainKey,
            sourceAddress: routing.sourceAddress,
            destinationAddress: protocolDepositBeneficiaryAddress,
            bridgeTxHash,
            depositTxHash: protocolDepositTxHash,
            netApy: plan.performanceFee.userNetApy,
            estimatedAnnualNetYield: plan.performanceFee.estimatedAnnual.optimizedNet,
          });
          status = 'position-opened';
          lastError = undefined;
        } else if (protocolDepositResult.status === 'awaiting-bridge-finality') {
          status = 'awaiting-bridge-finality';
          lastError = protocolDepositResult.message;
        } else {
          if (protocolDepositResult.status === 'external-action-required' && protocolDepositResult.externalUrl) {
            protocolDepositExternalUrl = protocolDepositResult.externalUrl;
            externalUrl = externalUrl || protocolDepositResult.externalUrl;
          }
          if (protocolDepositResult.status === 'wallet-action-required') {
            status = 'wallet-action-required';
          } else if (status !== 'bridge-external') {
            status = 'deposit-pending';
          }
          lastError = protocolDepositResult.message;
        }
      } else if (openPositionImmediately && !this.optimizerSyntheticPositionEnabled) {
        protocolDepositStatus = 'wallet-action-required';
        protocolDepositMessage =
          'Protocol deposit is blocked until bridge execution is completed in the connected wallet.';
        lastError = lastError || protocolDepositMessage;
      }

      await this.recordOptimizerActivityTransactions({
        userId,
        sourceChainKey: routing.sourceChainKey,
        destinationChainKey: routing.destinationChainKey,
        sourceAddress: routing.sourceAddress,
        destinationAddress: protocolDepositPayerAddress || routing.destinationAddress,
        category: plan.recommendation.category,
        assetSymbol: plan.strategy.assetSymbol,
        amount: plan.strategy.amount,
        bridgeTxHash,
        externalUrl,
        status,
        route: routing.route,
        positionId,
        depositTxHash: protocolDepositTxHash,
      });

      if (autoSettleFee) {
        const defaultRealizedProfit = this.normalizeDecimal(
          String(this.roundTo4Decimals(plan.performanceFee.estimatedAnnual.uplift)),
        );
        const realizedProfitAmount = dto.realizedProfitAmount || defaultRealizedProfit;
        try {
          feeSettlement = (await this.settleOptimizerFee(userId, {
            optimizerExecutionId: String((execution as any)._id || ''),
            realizedProfitAmount,
            performanceFeeBps: plan.performanceFee.performanceFeeBps,
            assetSymbol: plan.strategy.assetSymbol,
            chainKey: routing.destinationChainKey,
            payerAddress: protocolDepositPayerAddress || routing.destinationAddress || routing.sourceAddress || undefined,
            settleOnBackend: settleFeeOnBackend,
          })) as unknown as Record<string, unknown>;
        } catch (error: unknown) {
          const feeError = this.errorMessage(error);
          feeSettlement = {
            status: 'failed',
            error: feeError,
            nextAction: 'none',
          };
          lastError = lastError || `Fee settlement failed: ${feeError}`;
        }
      }
    } catch (error: unknown) {
      if (!lastError) {
        lastError = this.errorMessage(error);
      }
      if (
        status !== 'wallet-action-required' &&
        status !== 'bridge-external' &&
        status !== 'awaiting-bridge-finality' &&
        status !== 'deposit-pending'
      ) {
        status = 'failed';
      }
    }

    execution.status = status;
    execution.route = routing.route || undefined;
    execution.bridgeExecutionType = routing.executionType || undefined;
    execution.bridgeTxHash = bridgeTxHash;
    execution.approvalTxHash = approvalTxHash;
    execution.externalUrl = externalUrl;
    execution.positionId = positionId;
    execution.lastError = lastError;
    execution.metadata = {
      ...(execution.metadata || {}),
      dashboard: {
        tab: routing.destinationChainKey,
        badge: routing.required ? 'cross-chain' : 'local',
      },
      executionMode: this.optimizerSyntheticPositionEnabled ? 'synthetic' : 'production',
      protocolDepositRequired:
        status !== 'failed' && status !== 'wallet-action-required' && !positionId,
      protocolDeposit: {
        status: protocolDepositStatus,
        message: protocolDepositMessage || null,
        externalUrl: protocolDepositExternalUrl || null,
        payerAddress: protocolDepositPayerAddress,
        beneficiaryAddress: protocolDepositBeneficiaryAddress,
      },
      ...(protocolDepositTxHash ? { depositTxHash: protocolDepositTxHash } : {}),
      ...(feeSettlement ? { feeSettlement } : {}),
    };
    await execution.save();

    const feeSettlementNextAction =
      feeSettlement && typeof feeSettlement.nextAction === 'string'
        ? feeSettlement.nextAction
        : 'none';
    const protocolDepositRequired =
      status !== 'failed' && status !== 'wallet-action-required' && !positionId;
    const nextAction = this.resolveOptimizerNextAction({
      status,
      bridgeExternalUrl: externalUrl,
      protocolDepositRequired,
      protocolDepositStatus,
      protocolDepositExternalUrl,
      feeSettlementNextAction,
    });

    return {
      execution: this.mapOptimizerExecutionRecord(
        execution.toObject() as unknown as Record<string, unknown>,
      ),
      plan,
      ...(feeSettlement ? { feeSettlement } : {}),
      dashboard: {
        targetTab: routing.destinationChainKey,
        badge: routing.required ? 'cross-chain' : 'local',
      },
      nextAction,
    };
  }

  async executeBorrow(userId: string, dto: ExecuteBorrowDto) {
    const accessProfile = await this.accessControlService.assertFeatureForUser(
      userId,
      'optimizer',
    );
    const protocol = dto.protocol.trim();
    const protocolKey = this.normalizeProtocolKey(protocol);
    const assetSymbol = dto.assetSymbol.trim().toUpperCase();
    const amount = this.normalizeDecimal(dto.amount);
    const amountNumber = Number(amount);
    if (!Number.isFinite(amountNumber) || amountNumber <= 0) {
      throw new BadRequestException('amount must be greater than zero');
    }
    await this.assertOptimizerProtocolAllowed(userId, protocol, accessProfile.limits.maxProtocols);

    const [connectedWallets, bridgeChains] = await Promise.all([
      this.connectedWalletModel.find({ userId } as any).lean().exec(),
      this.getBridgeChains(),
    ]);
    const connectedWalletRecords = connectedWallets as Array<Record<string, unknown>>;
    const bridgeChainLookup = this.createBridgeChainLookup(bridgeChains);
    const requestedChain = this.normalizeAddressString(dto.chainKey);
    const chainKey =
      (requestedChain
        ? this.resolveBridgeChainKeyForYield(requestedChain, bridgeChainLookup) ||
          this.normalizeChainKeyHint(requestedChain)
        : null) || STELLAR_CHAIN_KEY;
    const normalizedChainKey = this.normalizeChainKeyHint(chainKey) || chainKey;

    const requestedWalletAddress = this.normalizeAddressString(dto.walletAddress);
    if (requestedWalletAddress) {
      if (normalizedChainKey === STELLAR_CHAIN_KEY && !this.looksLikeStellarAddress(requestedWalletAddress)) {
        throw new BadRequestException('walletAddress must be a valid Stellar address for stellar chain');
      }
      if (
        normalizedChainKey !== STELLAR_CHAIN_KEY &&
        !this.normalizeAddressIfPossible(requestedWalletAddress)
      ) {
        throw new BadRequestException('walletAddress must be a valid EVM address for EVM chains');
      }
    }
    const sourceAddress =
      requestedWalletAddress ||
      this.resolveConnectedWalletAddress(connectedWalletRecords, normalizedChainKey) ||
      null;

    const executionId = this
      .generateOptimizerExternalReference()
      .replace('optimizer_external_', 'borrow_external_');
    const allowExternalRedirect = dto.allowExternalRedirect ?? true;
    const fallbackExternalUrl =
      this.normalizeHttpUrlCandidate(dto.protocolUrl) || this.buildStellarProtocolUrl(protocol);

    const resolveBorrowResponse = (params: {
      status: BorrowExecutionStatus;
      message: string;
      externalUrl?: string | null;
      txHash?: string | null;
      metadata?: Record<string, unknown>;
    }) => {
      const normalizedExternalUrl = this.normalizeHttpUrlCandidate(params.externalUrl || undefined);
      const nextAction = this.resolveBorrowNextAction(params.status, normalizedExternalUrl);
      return {
        execution: {
          id: executionId,
          protocol,
          chainKey: normalizedChainKey,
          assetSymbol,
          amount,
          sourceAddress,
          status: params.status,
          message: params.message,
          externalUrl: normalizedExternalUrl || null,
          txHash: params.txHash || null,
          metadata: params.metadata || {},
        },
        nextAction,
      };
    };

    const resolveSyntheticBorrowResponse = (params?: {
      reason?: string;
      message?: string;
      metadata?: Record<string, unknown>;
    }) =>
      resolveBorrowResponse({
        status: 'executed',
        message: params?.message || 'Borrow completed in-app using synthetic template mode.',
        txHash: this
          .generateOptimizerExternalReference()
          .replace('optimizer_external_', 'borrow_synthetic_'),
        metadata: {
          synthetic: true,
          executionType: 'synthetic',
          ...(params?.reason ? { reason: params.reason } : {}),
          ...(params?.metadata || {}),
        },
      });

    if (protocolKey.includes('templar')) {
      if (this.borrowSyntheticExecutionEnabled) {
        return resolveSyntheticBorrowResponse({
          reason: 'templar-in-app-not-supported',
          metadata: {
            protocolKey,
          },
        });
      }
      return resolveBorrowResponse({
        status:
          allowExternalRedirect && Boolean(fallbackExternalUrl)
            ? 'external-action-required'
            : 'wallet-action-required',
        message:
          'Templar borrow is not integrated for in-app execution in this app yet. ' +
          'Use Templar-compatible wallet flow to complete this borrow.',
        externalUrl: fallbackExternalUrl,
        metadata: {
          protocolKey,
          reason: 'templar-in-app-not-supported',
        },
      });
    }

    const resolvedTemplate = this.resolveBorrowProtocolCallTemplate(normalizedChainKey, protocol);
    if (!resolvedTemplate) {
      if (this.borrowSyntheticExecutionEnabled) {
        return resolveSyntheticBorrowResponse({
          reason: 'no-template-configured',
          metadata: {
            protocolKey,
            chainKey: normalizedChainKey,
          },
        });
      }
      return resolveBorrowResponse({
        status:
          allowExternalRedirect && Boolean(fallbackExternalUrl)
            ? 'external-action-required'
            : 'wallet-action-required',
        message:
          `No borrow template is configured for ${protocol}. ` +
          'Complete the borrow in the protocol app.',
        externalUrl: fallbackExternalUrl,
      });
    }

    const templateExecutionType =
      (
        resolvedTemplate.template.executionType ||
        (resolvedTemplate.template.externalUrl ? 'external' : 'evm')
      )
        .trim()
        .toLowerCase();

    if (templateExecutionType === 'synthetic') {
      return resolveSyntheticBorrowResponse({
        reason: 'template-synthetic',
        metadata: {
          templateKey: resolvedTemplate.key,
        },
      });
    }

    if (templateExecutionType === 'external') {
      const templateExternalUrlCandidate = resolvedTemplate.template.externalUrl;
      const templateExternalUrl =
        typeof templateExternalUrlCandidate === 'string' && templateExternalUrlCandidate.trim().length > 0
          ? this.interpolateOptimizerTemplateString(templateExternalUrlCandidate, {
              executionId,
              protocol,
              protocolKey,
              chainKey: normalizedChainKey,
              assetSymbol,
              amount,
              sourceAddress: sourceAddress || '',
              destinationAddress: sourceAddress || '',
              payerAddress: sourceAddress || '',
            })
          : null;
      const resolvedExternalUrl = templateExternalUrl || fallbackExternalUrl;
      if (this.borrowSyntheticExecutionEnabled) {
        return resolveSyntheticBorrowResponse({
          reason: 'template-external-fallback',
          metadata: {
            templateKey: resolvedTemplate.key,
            ...(resolvedExternalUrl ? { suggestedExternalUrl: resolvedExternalUrl } : {}),
          },
        });
      }
      return resolveBorrowResponse({
        status:
          allowExternalRedirect && Boolean(resolvedExternalUrl)
            ? 'external-action-required'
            : 'wallet-action-required',
        message: 'Complete borrow on the external provider and return to refresh your portfolio state.',
        externalUrl: resolvedExternalUrl,
        metadata: {
          templateKey: resolvedTemplate.key,
          executionType: 'external',
        },
      });
    }

    if (!this.borrowProtocolExecutorEnabled) {
      if (this.borrowSyntheticExecutionEnabled) {
        return resolveSyntheticBorrowResponse({
          reason: 'borrow-executor-disabled',
          metadata: {
            templateKey: resolvedTemplate.key,
          },
        });
      }
      return resolveBorrowResponse({
        status:
          allowExternalRedirect && Boolean(fallbackExternalUrl)
            ? 'external-action-required'
            : 'wallet-action-required',
        message:
          'Automatic protocol borrow execution is unavailable for this route. Complete borrow manually in your wallet/protocol.',
        externalUrl: fallbackExternalUrl,
      });
    }

    if (normalizedChainKey === STELLAR_CHAIN_KEY) {
      if (this.borrowSyntheticExecutionEnabled) {
        return resolveSyntheticBorrowResponse({
          reason: 'stellar-manual-route-fallback',
          metadata: {
            templateKey: resolvedTemplate.key,
            chainKey: normalizedChainKey,
          },
        });
      }
      return resolveBorrowResponse({
        status:
          allowExternalRedirect && Boolean(fallbackExternalUrl)
            ? 'external-action-required'
            : 'wallet-action-required',
        message:
          'Stellar borrow must be completed in-wallet on the protocol app. Complete the borrow and refresh.',
        externalUrl: fallbackExternalUrl,
      });
    }

    if (!sourceAddress) {
      if (this.borrowSyntheticExecutionEnabled) {
        return resolveSyntheticBorrowResponse({
          reason: 'missing-wallet-address',
          metadata: {
            templateKey: resolvedTemplate.key,
            chainKey: normalizedChainKey,
          },
        });
      }
      return resolveBorrowResponse({
        status:
          allowExternalRedirect && Boolean(fallbackExternalUrl)
            ? 'external-action-required'
            : 'wallet-action-required',
        message:
          `No wallet address is connected for chain "${normalizedChainKey}". Connect wallet and retry borrow.`,
        externalUrl: fallbackExternalUrl,
      });
    }

    const protocolResult = await this.executeEvmProtocolDepositTemplate({
      templateKey: resolvedTemplate.key,
      template: resolvedTemplate.template,
      executionId,
      protocol,
      chainKey: normalizedChainKey,
      assetSymbol,
      amount,
      sourceAddress,
      destinationAddress: sourceAddress,
      payerAddress: sourceAddress,
    });

    const status: BorrowExecutionStatus =
      protocolResult.status === 'executed'
        ? 'executed'
        : protocolResult.status === 'external-action-required'
          ? 'external-action-required'
          : 'wallet-action-required';
    const message =
      protocolResult.status === 'executed'
        ? 'Protocol borrow executed on-chain.'
        : this.rewriteDepositMessageForBorrow(protocolResult.message);

    if (status !== 'executed' && this.borrowSyntheticExecutionEnabled) {
      return resolveSyntheticBorrowResponse({
        reason: 'protocol-execution-fallback',
        metadata: {
          ...(protocolResult.metadata || {}),
          templateKey: resolvedTemplate.key,
          protocolResultStatus: protocolResult.status,
          protocolResultMessage: message,
        },
      });
    }

    return resolveBorrowResponse({
      status,
      message,
      externalUrl: protocolResult.externalUrl || fallbackExternalUrl,
      txHash: protocolResult.depositTxHash || null,
      metadata: {
        ...(protocolResult.metadata || {}),
        templateKey: resolvedTemplate.key,
      },
    });
  }

  async completeOptimizerDeposit(
    userId: string,
    executionId: string,
    dto: CompleteOptimizerDepositDto,
  ) {
    const execution = await this.optimizerExecutionModel
      .findOne({ _id: executionId, userId } as any)
      .exec();

    if (!execution) {
      throw new NotFoundException('Optimizer execution not found');
    }

    if (execution.positionId) {
      return {
        execution: this.mapOptimizerExecutionRecord(
          execution.toObject() as unknown as Record<string, unknown>,
        ),
        nextAction: 'none',
      };
    }

    const bridgeExternalUrl = execution.externalUrl ? String(execution.externalUrl) : undefined;
    const existingMetadata =
      execution.metadata && typeof execution.metadata === 'object'
        ? ({ ...(execution.metadata as Record<string, unknown>) } as Record<string, unknown>)
        : {};
    const existingProtocolDeposit =
      existingMetadata.protocolDeposit && typeof existingMetadata.protocolDeposit === 'object'
        ? ({ ...(existingMetadata.protocolDeposit as Record<string, unknown>) } as Record<string, unknown>)
        : {};

    const payerAddress =
      this.normalizeAddressString(
        typeof existingProtocolDeposit.payerAddress === 'string'
          ? existingProtocolDeposit.payerAddress
          : undefined,
      ) ||
      this.normalizeAddressString(execution.destinationAddress) ||
      this.normalizeAddressString(execution.sourceAddress);
    const beneficiaryAddress =
      this.normalizeAddressString(
        typeof existingProtocolDeposit.beneficiaryAddress === 'string'
          ? existingProtocolDeposit.beneficiaryAddress
          : undefined,
      ) ||
      this.normalizeAddressString(execution.destinationAddress) ||
      this.normalizeAddressString(execution.sourceAddress);
    const protocolDepositTxHashFromDto = this.normalizeOptimizerDepositTxHash(dto.depositTxHash);
    const bridgeTxHashFromDto = dto.bridgeTxHash
      ? this.normalizeBridgeReference(
          dto.bridgeTxHash,
          'submitted',
          String(execution.sourceChainKey || ''),
        )
      : undefined;
    const bridgeRequired =
      String(execution.sourceChainKey || '') !== String(execution.destinationChainKey || '');
    let bridgeTxHash = execution.bridgeTxHash ? String(execution.bridgeTxHash) : undefined;
    if (bridgeTxHashFromDto) {
      bridgeTxHash = bridgeTxHashFromDto;
    }
    let bridgeStatusForAttempt = String(execution.status || 'planned') as OptimizerExecutionStatus;
    if (
      bridgeRequired &&
      bridgeTxHashFromDto &&
      (bridgeStatusForAttempt === 'wallet-action-required' ||
        bridgeStatusForAttempt === 'planned' ||
        bridgeStatusForAttempt === 'bridge-external')
    ) {
      bridgeStatusForAttempt = 'bridge-submitted';
    }

    if (this.optimizerSyntheticPositionEnabled) {
      const wallet = await this.resolveOrCreateOptimizerWallet(
        userId,
        String(execution.destinationChainKey || ''),
        beneficiaryAddress,
      );
      const syntheticPositionId = await this.createOptimizerPosition(String(wallet._id || ''), {
        strategyName: String(execution.strategyName || ''),
        protocol: String(execution.protocol || ''),
        category: String(execution.category || ''),
        assetSymbol: String(execution.assetSymbol || ''),
        amount: String(execution.amount || '0'),
        sourceChainKey: String(execution.sourceChainKey || ''),
        destinationChainKey: String(execution.destinationChainKey || ''),
        sourceAddress: this.normalizeAddressString(execution.sourceAddress),
        destinationAddress: beneficiaryAddress,
        bridgeTxHash,
        depositTxHash: protocolDepositTxHashFromDto,
        netApy: this.toPositiveNumber(execution.netApy),
        estimatedAnnualNetYield: this.toPositiveNumber(execution.estimatedAnnualNetYield),
      });

      await this.recordOptimizerActivityTransactions({
        userId,
        sourceChainKey: String(execution.sourceChainKey || ''),
        destinationChainKey: String(execution.destinationChainKey || ''),
        sourceAddress: this.normalizeAddressString(execution.sourceAddress),
        destinationAddress: payerAddress || beneficiaryAddress,
        category: String(execution.category || ''),
        assetSymbol: String(execution.assetSymbol || ''),
        amount: String(execution.amount || '0'),
        bridgeTxHash,
        externalUrl: bridgeExternalUrl,
        status: 'position-opened',
        route: execution.route ? String(execution.route) : null,
        positionId: syntheticPositionId,
        depositTxHash: protocolDepositTxHashFromDto,
      });

      const syntheticMessage = protocolDepositTxHashFromDto
        ? 'Recorded protocol deposit transaction hash and opened synthetic optimizer position.'
        : 'Opened synthetic optimizer position.';
      const syntheticProtocolDepositResult: OptimizerProtocolDepositResult = {
        status: 'executed',
        message: syntheticMessage,
        ...(protocolDepositTxHashFromDto ? { depositTxHash: protocolDepositTxHashFromDto } : {}),
      };

      execution.status = 'position-opened';
      execution.bridgeTxHash = bridgeTxHash;
      execution.positionId = syntheticPositionId;
      execution.lastError = undefined;
      execution.metadata = {
        ...existingMetadata,
        protocolDeposit: {
          ...existingProtocolDeposit,
          status: syntheticProtocolDepositResult.status,
          message: syntheticProtocolDepositResult.message,
          externalUrl: null,
          payerAddress: payerAddress || null,
          beneficiaryAddress: beneficiaryAddress || null,
        },
        protocolDepositRequired: false,
        ...(protocolDepositTxHashFromDto ? { depositTxHash: protocolDepositTxHashFromDto } : {}),
      };
      await execution.save();

      return {
        execution: this.mapOptimizerExecutionRecord(
          execution.toObject() as unknown as Record<string, unknown>,
        ),
        protocolDeposit: syntheticProtocolDepositResult,
        nextAction: 'none',
      };
    }

    const protocolDepositResult = await this.attemptOptimizerProtocolDeposit({
      executionId: String((execution as any)._id || executionId),
      protocol: String(execution.protocol || ''),
      sourceChainKey: String(execution.sourceChainKey || ''),
      chainKey: String(execution.destinationChainKey || ''),
      assetSymbol: String(execution.assetSymbol || ''),
      amount: String(execution.amount || '0'),
      sourceAddress: this.normalizeAddressString(execution.sourceAddress),
      destinationAddress: beneficiaryAddress,
      payerAddress,
      bridgeRequired,
      bridgeStatus: bridgeStatusForAttempt,
      bridgeTxHash,
      bridgeExternalUrl,
      autoExecute: dto.autoExecute ?? true,
      depositTxHash: dto.depositTxHash,
      allowExternalRedirect: dto.allowExternalRedirect ?? true,
    });

    let nextStatus = bridgeStatusForAttempt;
    let positionId = execution.positionId ? String(execution.positionId) : undefined;
    let protocolDepositTxHash = protocolDepositTxHashFromDto;
    let externalUrl = bridgeExternalUrl;

    if (protocolDepositResult.depositTxHash) {
      protocolDepositTxHash = protocolDepositResult.depositTxHash;
    }

    if (protocolDepositResult.status === 'executed') {
      const wallet = await this.resolveOrCreateOptimizerWallet(
        userId,
        String(execution.destinationChainKey || ''),
        beneficiaryAddress,
      );
      positionId = await this.createOptimizerPosition(String(wallet._id || ''), {
        strategyName: String(execution.strategyName || ''),
        protocol: String(execution.protocol || ''),
        category: String(execution.category || ''),
        assetSymbol: String(execution.assetSymbol || ''),
        amount: String(execution.amount || '0'),
        sourceChainKey: String(execution.sourceChainKey || ''),
        destinationChainKey: String(execution.destinationChainKey || ''),
        sourceAddress: this.normalizeAddressString(execution.sourceAddress),
        destinationAddress: beneficiaryAddress,
        bridgeTxHash,
        depositTxHash: protocolDepositTxHash,
        netApy: this.toPositiveNumber(execution.netApy),
        estimatedAnnualNetYield: this.toPositiveNumber(execution.estimatedAnnualNetYield),
      });
      nextStatus = 'position-opened';
      execution.lastError = undefined;
    } else if (protocolDepositResult.status === 'awaiting-bridge-finality') {
      nextStatus = 'awaiting-bridge-finality';
      execution.lastError = protocolDepositResult.message;
    } else {
      if (protocolDepositResult.status === 'external-action-required' && protocolDepositResult.externalUrl) {
        externalUrl = protocolDepositResult.externalUrl;
      }
      if (protocolDepositResult.status === 'wallet-action-required') {
        nextStatus = 'wallet-action-required';
      } else if (nextStatus !== 'wallet-action-required' && nextStatus !== 'bridge-external') {
        nextStatus = 'deposit-pending';
      }
      execution.lastError = protocolDepositResult.message;
    }

    await this.recordOptimizerActivityTransactions({
      userId,
      sourceChainKey: String(execution.sourceChainKey || ''),
      destinationChainKey: String(execution.destinationChainKey || ''),
      sourceAddress: this.normalizeAddressString(execution.sourceAddress),
      destinationAddress: payerAddress || beneficiaryAddress,
      category: String(execution.category || ''),
      assetSymbol: String(execution.assetSymbol || ''),
      amount: String(execution.amount || '0'),
      bridgeTxHash,
      externalUrl,
      status: nextStatus,
      route: execution.route ? String(execution.route) : null,
      positionId,
      depositTxHash: protocolDepositTxHash,
    });

    execution.status = nextStatus;
    execution.bridgeTxHash = bridgeTxHash;
    execution.externalUrl = externalUrl;
    execution.positionId = positionId;
    execution.metadata = {
      ...existingMetadata,
      protocolDeposit: {
        ...existingProtocolDeposit,
        status: protocolDepositResult.status,
        message: protocolDepositResult.message,
        externalUrl: protocolDepositResult.externalUrl || null,
        payerAddress: payerAddress || null,
        beneficiaryAddress: beneficiaryAddress || null,
      },
      protocolDepositRequired: nextStatus !== 'wallet-action-required' && !positionId,
      ...(protocolDepositTxHash ? { depositTxHash: protocolDepositTxHash } : {}),
    };
    await execution.save();

    const protocolDepositRequired = nextStatus !== 'wallet-action-required' && !positionId;
    const nextAction = this.resolveOptimizerNextAction({
      status: nextStatus,
      bridgeExternalUrl: externalUrl,
      protocolDepositRequired,
      protocolDepositStatus: protocolDepositResult.status,
      protocolDepositExternalUrl: protocolDepositResult.externalUrl,
      feeSettlementNextAction: 'none',
    });

    return {
      execution: this.mapOptimizerExecutionRecord(
        execution.toObject() as unknown as Record<string, unknown>,
      ),
      protocolDeposit: protocolDepositResult,
      nextAction,
    };
  }

  async buildOptimizerStellarDepositTransaction(
    userId: string,
    executionId: string,
    dto: BuildOptimizerStellarDepositTxDto,
  ): Promise<{
    transactionXdr: string;
    sourcePublicKey: string;
    amount: string;
    asset: string;
    lockType: 'claimable-balance';
    network: 'testnet' | 'mainnet';
    fee: string;
    platformFee?: {
      action: PlatformFeeAction;
      feeBps: number;
      amount: string;
      asset: string;
      assetSymbol: string;
      collectorAddress: string;
      feeRecordId?: string;
    };
  }> {
    const execution = await this.optimizerExecutionModel
      .findOne({ _id: executionId, userId } as any)
      .lean()
      .exec();

    if (!execution) {
      throw new NotFoundException('Optimizer execution not found');
    }

    if (execution.positionId) {
      throw new BadRequestException('Optimizer position is already open for this execution');
    }

    const destinationChainKey =
      this.normalizeChainKeyHint(String(execution.destinationChainKey || '')) ||
      String(execution.destinationChainKey || '').trim().toLowerCase();
    if (destinationChainKey !== STELLAR_CHAIN_KEY) {
      throw new BadRequestException('Only Stellar optimizer executions can build Stellar deposit tx');
    }

    const executionSourceAddress = this.normalizeAddressString(execution.sourceAddress);
    const executionDestinationAddress = this.normalizeAddressString(execution.destinationAddress);
    const executionStellarSourceAddress =
      executionSourceAddress && this.looksLikeStellarAddress(executionSourceAddress)
        ? executionSourceAddress
        : null;
    const executionStellarDestinationAddress =
      executionDestinationAddress && this.looksLikeStellarAddress(executionDestinationAddress)
        ? executionDestinationAddress
        : null;
    const requestedSource = this.normalizeAddressString(dto.sourcePublicKey);
    let fallbackConnectedStellarAddress: string | null = null;
    if (
      !requestedSource &&
      !executionStellarDestinationAddress &&
      !executionStellarSourceAddress
    ) {
      const connectedWallets = (await this.connectedWalletModel
        .find({ userId } as any)
        .lean()
        .exec()) as Array<Record<string, unknown>>;
      fallbackConnectedStellarAddress = this.resolveConnectedWalletAddress(
        connectedWallets,
        STELLAR_CHAIN_KEY,
      );
    }
    const sourcePublicKey =
      requestedSource ||
      executionStellarDestinationAddress ||
      executionStellarSourceAddress ||
      fallbackConnectedStellarAddress;

    if (!sourcePublicKey) {
      throw new BadRequestException('No Stellar source address is available for this execution');
    }
    if (!this.looksLikeStellarAddress(sourcePublicKey)) {
      throw new BadRequestException('sourcePublicKey must be a valid Stellar address');
    }

    const executionStellarAddressSet = new Set<string>(
      [executionStellarSourceAddress, executionStellarDestinationAddress].filter(
        (value): value is string => Boolean(value),
      ),
    );
    if (
      requestedSource &&
      executionStellarAddressSet.size > 0 &&
      !executionStellarAddressSet.has(requestedSource)
    ) {
      throw new BadRequestException(
        'sourcePublicKey must match execution source/destination Stellar address',
      );
    }

    const assetSymbol = String(execution.assetSymbol || '').trim().toUpperCase();
    if (!assetSymbol) {
      throw new BadRequestException('Optimizer execution asset symbol is required');
    }

    const requestedAmount = this.normalizeDecimal(String(execution.amount || '0'));
    if (Number(requestedAmount) <= 0) {
      throw new BadRequestException('Optimizer execution amount must be greater than zero');
    }

    const account = await this.server.loadAccount(sourcePublicKey);
    const accountBalances = Array.isArray((account as any).balances)
      ? ((account as any).balances as Array<Record<string, unknown>>)
      : [];

    let stellarAsset: StellarSdk.Asset;
    let responseAsset = assetSymbol;
    if (assetSymbol === 'XLM') {
      stellarAsset = StellarSdk.Asset.native();
    } else {
      let tokenIdentifier: string | null = null;
      try {
        tokenIdentifier = await this.resolveBridgeTokenIdentifierForSymbol(
          STELLAR_CHAIN_KEY,
          assetSymbol,
        );
      } catch {
        tokenIdentifier = null;
      }

      if (tokenIdentifier && tokenIdentifier.trim().toLowerCase() !== 'native') {
        stellarAsset = this.parseAsset(tokenIdentifier);
        responseAsset = tokenIdentifier;
      } else {
        const matchingTrustlines = accountBalances.filter((balance) => {
          const code = typeof balance.asset_code === 'string' ? balance.asset_code.trim().toUpperCase() : '';
          const issuer = typeof balance.asset_issuer === 'string' ? balance.asset_issuer.trim() : '';
          return code === assetSymbol && this.looksLikeStellarAddress(issuer);
        });

        if (matchingTrustlines.length === 0) {
          throw new BadRequestException(
            `No trustline for ${assetSymbol} found on ${sourcePublicKey}. Add trustline or use a supported asset.`,
          );
        }

        if (matchingTrustlines.length > 1) {
          throw new BadRequestException(
            `Multiple ${assetSymbol} issuers found in wallet trustlines. Auto deposit requires a unique issuer asset.`,
          );
        }

        const trustline = matchingTrustlines[0];
        const trustlineCode = String(trustline.asset_code || '').trim().toUpperCase();
        const trustlineIssuer = String(trustline.asset_issuer || '').trim();
        stellarAsset = new StellarSdk.Asset(trustlineCode, trustlineIssuer);
        responseAsset = `${trustlineCode}:${trustlineIssuer}`;
      }
    }

    const requestedAmountBaseUnits = this.parseStellarAmountBaseUnits(requestedAmount);
    if (requestedAmountBaseUnits <= 0n) {
      throw new BadRequestException('Optimizer execution amount must be greater than zero');
    }

    const spendableAmountBaseUnits = this.resolveStellarSpendableBalanceBaseUnits(
      accountBalances,
      stellarAsset,
    );
    if (spendableAmountBaseUnits <= 0n) {
      throw new BadRequestException(
        `No spendable ${responseAsset} balance found on ${sourcePublicKey}.`,
      );
    }

    let lockAmountBaseUnits =
      requestedAmountBaseUnits > spendableAmountBaseUnits
        ? spendableAmountBaseUnits
        : requestedAmountBaseUnits;
    if (lockAmountBaseUnits <= 0n) {
      throw new BadRequestException(
        `Insufficient ${responseAsset} balance on ${sourcePublicKey} for optimizer deposit.`,
      );
    }

    if (lockAmountBaseUnits < requestedAmountBaseUnits) {
      this.logger.warn(
        `Optimizer Stellar deposit amount adjusted for ${executionId}: requested=${requestedAmount}, spendable=${this.fromBaseUnits(
          spendableAmountBaseUnits,
          7,
        )}, source=${sourcePublicKey}, asset=${responseAsset}`,
      );
    }
    const lockAmount = this.fromBaseUnits(lockAmountBaseUnits, 7);

    const fee = await this.feeEstimationService.estimateFee('stellar', 'create_claimable_balance');
    const builder = new StellarSdk.TransactionBuilder(account, {
      fee,
      networkPassphrase: this.getNetworkPassphrase(),
    });

    builder.addOperation(
      StellarSdk.Operation.createClaimableBalance({
        asset: stellarAsset,
        amount: lockAmount,
        claimants: [
          new StellarSdk.Claimant(
            sourcePublicKey,
            StellarSdk.Claimant.predicateUnconditional(),
          ),
        ],
      }),
    );

    let platformFeeQuote = this.platformFeeService.buildQuote({
      action: 'deposit',
      chainKey: STELLAR_CHAIN_KEY,
      payerAddress: sourcePublicKey,
      asset: responseAsset,
      assetSymbol,
      amount: lockAmount,
      decimals: 7,
    });

    if (platformFeeQuote) {
      const collectorCanReceive = await this.canStellarAccountReceiveAsset(
        platformFeeQuote.collectorAddress,
        stellarAsset,
      );
      if (!collectorCanReceive) {
        this.logger.warn(
          `Skipping optimizer Stellar deposit platform fee for ${executionId}: collector ${platformFeeQuote.collectorAddress} cannot receive ${responseAsset}.`,
        );
        platformFeeQuote = null;
      }
    }

    if (platformFeeQuote) {
      const platformFeeBaseUnits = this.parseStellarAmountBaseUnits(platformFeeQuote.feeAmount);
      if (platformFeeBaseUnits <= 0n) {
        platformFeeQuote = null;
      } else if (lockAmountBaseUnits + platformFeeBaseUnits > spendableAmountBaseUnits) {
        this.logger.warn(
          `Skipping optimizer Stellar deposit platform fee for ${executionId}: lockAmount=${lockAmount} plus fee=${platformFeeQuote.feeAmount} exceeds spendable balance.`,
        );
        platformFeeQuote = null;
      }
    }

    if (platformFeeQuote) {
      builder.addOperation(
        StellarSdk.Operation.payment({
          destination: platformFeeQuote.collectorAddress,
          asset: stellarAsset,
          amount: platformFeeQuote.feeAmount,
        }),
      );
    }

    if (dto.memo) {
      builder.addMemo(StellarSdk.Memo.text(dto.memo.slice(0, 28)));
    }

    builder.setTimeout(180);
    const transaction = builder.build();
    const platformFeeReference = platformFeeQuote
      ? this.platformFeeService.buildReference([
          'deposit',
          userId,
          executionId,
          sourcePublicKey,
          transaction.toXDR(),
        ])
      : undefined;
    const platformFeeRecord = platformFeeQuote
      ? await this.platformFeeService.accrueFee({
          userId,
          quote: platformFeeQuote,
          reference: platformFeeReference,
          metadata: {
            optimizerExecutionId: executionId,
            lockType: 'claimable-balance',
          },
        })
      : null;

    return {
      transactionXdr: transaction.toXDR(),
      sourcePublicKey,
      amount: lockAmount,
      asset: responseAsset,
      lockType: 'claimable-balance',
      network:
        this.getNetworkPassphrase() === StellarSdk.Networks.PUBLIC ? 'mainnet' : 'testnet',
      fee,
      ...(platformFeeQuote
        ? {
            platformFee: {
              action: platformFeeQuote.action,
              feeBps: platformFeeQuote.feeBps,
              amount: platformFeeQuote.feeAmount,
              asset: platformFeeQuote.asset,
              assetSymbol: platformFeeQuote.assetSymbol,
              collectorAddress: platformFeeQuote.collectorAddress,
              ...(platformFeeRecord ? { feeRecordId: platformFeeRecord.id } : {}),
            },
          }
        : {}),
    };
  }

  private resolveOptimizerNextAction(params: {
    status: OptimizerExecutionStatus;
    bridgeExternalUrl?: string;
    protocolDepositRequired: boolean;
    protocolDepositStatus: OptimizerProtocolDepositResultStatus | 'none';
    protocolDepositExternalUrl?: string;
    feeSettlementNextAction?: string;
  }): OptimizerNextAction {
    if (params.status === 'wallet-action-required') {
      return 'wallet-execution-required';
    }

    if (params.bridgeExternalUrl) {
      return 'open-external-bridge';
    }

    if (params.protocolDepositRequired) {
      if (
        params.status === 'bridge-submitted' ||
        params.status === 'awaiting-bridge-finality' ||
        params.protocolDepositStatus === 'awaiting-bridge-finality'
      ) {
        return 'await-bridge-finality';
      }
      if (
        params.protocolDepositStatus === 'external-action-required' ||
        params.protocolDepositExternalUrl
      ) {
        return 'open-external-deposit';
      }
      return 'complete-protocol-deposit';
    }

    if (params.feeSettlementNextAction === 'wallet-sign-required') {
      return 'fee-wallet-sign-required';
    }

    return 'none';
  }

  private resolveBorrowNextAction(
    status: BorrowExecutionStatus,
    externalUrl: string | null,
  ): BorrowNextAction {
    if (status === 'executed') {
      return 'none';
    }
    if (status === 'external-action-required' && externalUrl) {
      return 'open-external-borrow';
    }
    return 'wallet-execution-required';
  }

  private rewriteDepositMessageForBorrow(message: string): string {
    if (!message.trim()) {
      return 'Borrow action still requires wallet/protocol completion.';
    }

    return message
      .replace(/protocol deposit/gi, 'protocol borrow')
      .replace(/\bdeposit\b/gi, 'borrow')
      .replace(/depositor/gi, 'borrower')
      .replace(/depositTxHash/g, 'borrowTxHash');
  }

  private async attemptOptimizerProtocolDeposit(params: {
    executionId: string;
    protocol: string;
    sourceChainKey: string;
    chainKey: string;
    assetSymbol: string;
    amount: string;
    sourceAddress: string | null;
    destinationAddress: string | null;
    payerAddress: string | null;
    bridgeRequired: boolean;
    bridgeStatus: OptimizerExecutionStatus;
    bridgeTxHash?: string;
    bridgeExternalUrl?: string;
    autoExecute: boolean;
    depositTxHash?: string;
    allowExternalRedirect: boolean;
  }): Promise<OptimizerProtocolDepositResult> {
    const normalizedDepositTxHash = this.normalizeOptimizerDepositTxHash(params.depositTxHash);
    if (normalizedDepositTxHash) {
      return {
        status: 'executed',
        message: 'Recorded protocol deposit transaction hash.',
        depositTxHash: normalizedDepositTxHash,
      };
    }

    if (params.bridgeRequired) {
      const normalizedBridgeTxHash = this.normalizeEvmTxHashIfPossible(params.bridgeTxHash);
      if (normalizedBridgeTxHash) {
        const bridgeConfirmation = await this.resolveBridgeTransactionConfirmation(
          params.sourceChainKey,
          normalizedBridgeTxHash,
        );
        if (bridgeConfirmation === 'failed') {
          return {
            status: 'wallet-action-required',
            message:
              'Bridge transaction failed on source chain. Retry bridge in wallet, then continue protocol deposit.',
          };
        }
        if (bridgeConfirmation !== 'confirmed') {
          return {
            status: 'awaiting-bridge-finality',
            message:
              'Bridge transaction is still in-flight. Retry protocol deposit after destination funds arrive.',
          };
        }
      } else {
        if (params.bridgeStatus === 'wallet-action-required') {
          return {
            status: 'wallet-action-required',
            message: 'Bridge execution requires wallet signature before protocol deposit.',
          };
        }
        if (params.bridgeStatus === 'bridge-external') {
          return {
            status: params.allowExternalRedirect ? 'external-action-required' : 'wallet-action-required',
            message:
              'Complete the bridge route in the external provider first, then continue protocol deposit.',
            ...(params.bridgeExternalUrl ? { externalUrl: params.bridgeExternalUrl } : {}),
          };
        }
        if (
          !params.bridgeTxHash ||
          params.bridgeStatus === 'planned' ||
          params.bridgeStatus === 'bridge-submitted' ||
          params.bridgeStatus === 'awaiting-bridge-finality'
        ) {
          return {
            status: 'awaiting-bridge-finality',
            message:
              'Bridge transaction is still in-flight. Retry protocol deposit after destination funds arrive.',
          };
        }
      }
    }

    if (!params.autoExecute) {
      return {
        status: 'wallet-action-required',
        message:
          'Automatic protocol deposit is disabled. Complete the final deposit in your wallet/protocol, then submit depositTxHash.',
      };
    }

    if (!this.optimizerProtocolExecutorEnabled) {
      return {
        status: 'wallet-action-required',
        message:
          'Automatic protocol deposit is unavailable for this route. Complete the final deposit in your wallet/protocol, then submit depositTxHash.',
      };
    }

    const resolvedTemplate = this.resolveOptimizerProtocolCallTemplate(
      params.chainKey,
      params.protocol,
    );
    if (!resolvedTemplate) {
      return {
        status: 'wallet-action-required',
        message: `Automatic deposit template is unavailable for ${params.protocol}. Complete the final deposit in your wallet/protocol, then submit depositTxHash.`,
      };
    }

    const templateExecutionType =
      (resolvedTemplate.template.executionType || (resolvedTemplate.template.externalUrl ? 'external' : 'evm'))
        .trim()
        .toLowerCase();

    if (templateExecutionType === 'external') {
      const externalUrlCandidate = resolvedTemplate.template.externalUrl;
      const externalUrl =
        typeof externalUrlCandidate === 'string' && externalUrlCandidate.trim().length > 0
          ? this.interpolateOptimizerTemplateString(externalUrlCandidate, {
              executionId: params.executionId,
              protocol: params.protocol,
              protocolKey: this.normalizeProtocolKey(params.protocol),
              chainKey: params.chainKey,
              assetSymbol: params.assetSymbol,
              amount: params.amount,
              sourceAddress: params.sourceAddress || '',
              destinationAddress: params.destinationAddress || '',
              payerAddress: params.payerAddress || '',
            })
          : null;

      return {
        status: params.allowExternalRedirect ? 'external-action-required' : 'wallet-action-required',
        message: 'Complete final protocol deposit on the external provider to open the position.',
        ...(externalUrl ? { externalUrl } : {}),
      };
    }

    if ((this.normalizeChainKeyHint(params.chainKey) || params.chainKey) === STELLAR_CHAIN_KEY) {
      return {
        status: 'wallet-action-required',
        message:
          'Stellar final deposit must be completed in-wallet. Submit depositTxHash after signing.',
      };
    }

    return this.executeEvmProtocolDepositTemplate({
      templateKey: resolvedTemplate.key,
      template: resolvedTemplate.template,
      executionId: params.executionId,
      protocol: params.protocol,
      chainKey: params.chainKey,
      assetSymbol: params.assetSymbol,
      amount: params.amount,
      sourceAddress: params.sourceAddress,
      destinationAddress: params.destinationAddress,
      payerAddress: params.payerAddress,
    });
  }

  private async executeEvmProtocolDepositTemplate(params: {
    templateKey: string;
    template: OptimizerProtocolCallTemplate;
    executionId: string;
    protocol: string;
    chainKey: string;
    assetSymbol: string;
    amount: string;
    sourceAddress: string | null;
    destinationAddress: string | null;
    payerAddress: string | null;
  }): Promise<OptimizerProtocolDepositResult> {
    const normalizedChainKey = this.normalizeChainKeyHint(params.chainKey) || params.chainKey;
    const rpcUrl = this.resolveRpcUrlForChain(normalizedChainKey);
    if (!rpcUrl) {
      return {
        status: 'wallet-action-required',
        message:
          `No RPC URL configured for chain "${normalizedChainKey}". ` +
          `Set STARGATE_EVM_RPC_URLS or STARGATE_RPC_URL_${this.chainEnvSuffix(normalizedChainKey)}.`,
      };
    }

    let signer: ethers.Wallet;
    try {
      signer = this.createBridgeExecutorSigner(normalizedChainKey, rpcUrl);
    } catch (error: unknown) {
      return {
        status: 'wallet-action-required',
        message: `Unable to initialize backend signer: ${this.errorMessage(error)}`,
      };
    }

    const requiredSignerMatch = params.template.requiredSignerMatch !== false;
    const normalizedPayerAddress = params.payerAddress
      ? this.normalizeAddressIfPossible(params.payerAddress)
      : null;
    if (
      requiredSignerMatch &&
      (!normalizedPayerAddress ||
        normalizedPayerAddress.toLowerCase() !== signer.address.toLowerCase())
    ) {
      return {
        status: 'wallet-action-required',
        message:
          `Protocol payer ${params.payerAddress || 'unknown'} is not controlled by backend signer ${signer.address}. ` +
          'Wallet-side deposit is required.',
      };
    }

    let assetToken: BridgeTokenInfo;
    try {
      const tokenIdentifier = await this.resolveBridgeTokenIdentifierForSymbol(
        normalizedChainKey,
        params.assetSymbol,
      );
      assetToken = await this.resolveBridgeToken(normalizedChainKey, tokenIdentifier);
    } catch (error: unknown) {
      return {
        status: 'wallet-action-required',
        message: `Unable to resolve asset token for protocol deposit: ${this.errorMessage(error)}`,
      };
    }

    let amountBaseUnits: bigint;
    try {
      amountBaseUnits = this.toBaseUnits(params.amount, assetToken.decimals, 'amount');
    } catch (error: unknown) {
      return {
        status: 'wallet-action-required',
        message: this.errorMessage(error),
      };
    }

    const context = this.buildOptimizerTemplateContext({
      executionId: params.executionId,
      protocol: params.protocol,
      chainKey: normalizedChainKey,
      assetSymbol: params.assetSymbol,
      amount: params.amount,
      amountBaseUnits,
      sourceAddress: params.sourceAddress,
      destinationAddress: params.destinationAddress,
      payerAddress: params.payerAddress,
      signerAddress: signer.address,
      assetTokenAddress: assetToken.address,
    });

    const targetResolved = this.resolveOptimizerTemplateValue(params.template.target, context);
    if (typeof targetResolved !== 'string' || !targetResolved.trim()) {
      return {
        status: 'wallet-action-required',
        message: `Protocol template "${params.templateKey}" is missing a target contract address.`,
      };
    }

    let targetAddress: string;
    try {
      targetAddress = this.normalizeEvmAddress(targetResolved, 'target');
    } catch (error: unknown) {
      return {
        status: 'wallet-action-required',
        message: this.errorMessage(error),
      };
    }

    const abi = Array.isArray(params.template.abi)
      ? params.template.abi.filter((item): item is string => typeof item === 'string' && item.trim().length > 0)
      : [];
    const method = typeof params.template.method === 'string' ? params.template.method.trim() : '';
    if (abi.length === 0 || !method) {
      return {
        status: 'wallet-action-required',
        message: `Protocol template "${params.templateKey}" must define abi[] and method.`,
      };
    }

    const argsRaw = Array.isArray(params.template.args) ? params.template.args : [];
    const args = argsRaw.map((arg) => this.resolveOptimizerTemplateValue(arg, context));

    let txData: string;
    try {
      const protocolInterface = new ethers.Interface(abi);
      txData = protocolInterface.encodeFunctionData(method, args);
    } catch (error: unknown) {
      return {
        status: 'wallet-action-required',
        message: `Invalid protocol template call data: ${this.errorMessage(error)}`,
      };
    }

    let txValue = 0n;
    if (typeof params.template.value !== 'undefined') {
      try {
        txValue = this.toNonNegativeBigInt(
          this.resolveOptimizerTemplateValue(params.template.value, context),
          'value',
        );
      } catch (error: unknown) {
        return {
          status: 'wallet-action-required',
          message: this.errorMessage(error),
        };
      }
    }

    const hasApprovalConfig =
      typeof params.template.approveTokenAddress !== 'undefined' ||
      typeof params.template.approveSpender !== 'undefined';
    let approvalTxHash: string | undefined;

    if (hasApprovalConfig) {
      if (!params.template.approveTokenAddress || !params.template.approveSpender) {
        return {
          status: 'wallet-action-required',
          message:
            `Protocol template "${params.templateKey}" must define both approveTokenAddress and approveSpender.`,
        };
      }

      const approveTokenAddressResolved = this.resolveOptimizerTemplateValue(
        params.template.approveTokenAddress,
        context,
      );
      const approveSpenderResolved = this.resolveOptimizerTemplateValue(
        params.template.approveSpender,
        context,
      );

      if (
        typeof approveTokenAddressResolved !== 'string' ||
        typeof approveSpenderResolved !== 'string'
      ) {
        return {
          status: 'wallet-action-required',
          message: `Protocol template "${params.templateKey}" approval fields must resolve to strings.`,
        };
      }

      if (!this.isNativeTokenAddress(approveTokenAddressResolved)) {
        let approveAmount = amountBaseUnits;
        if (typeof params.template.approveAmount !== 'undefined') {
          try {
            approveAmount = this.toNonNegativeBigInt(
              this.resolveOptimizerTemplateValue(params.template.approveAmount, context),
              'approveAmount',
            );
          } catch (error: unknown) {
            return {
              status: 'wallet-action-required',
              message: this.errorMessage(error),
            };
          }
        }

        let approveSpenderAddress: string;
        let approveTokenAddress: string;
        try {
          approveSpenderAddress = this.normalizeEvmAddress(approveSpenderResolved, 'approveSpender');
          approveTokenAddress = this.normalizeEvmAddress(
            approveTokenAddressResolved,
            'approveTokenAddress',
          );
        } catch (error: unknown) {
          return {
            status: 'wallet-action-required',
            message: this.errorMessage(error),
          };
        }

        const approveData = new ethers.Interface(ERC20_APPROVE_ABI).encodeFunctionData(
          'approve',
          [approveSpenderAddress, approveAmount],
        );

        try {
          const approvalResponse = await signer.sendTransaction({
            to: approveTokenAddress,
            data: approveData,
            value: 0n,
          });
          approvalTxHash = approvalResponse.hash.toLowerCase();
          await this.waitForBridgeExecutionReceipt(
            signer.provider as ethers.Provider,
            approvalResponse.hash,
            'protocol approval',
          );
        } catch (error: unknown) {
          return {
            status: 'wallet-action-required',
            message: `Protocol approval execution failed: ${this.errorMessage(error)}`,
          };
        }
      }
    }

    try {
      const depositResponse = await signer.sendTransaction({
        to: targetAddress,
        data: txData,
        value: txValue,
      });
      await this.waitForBridgeExecutionReceipt(
        signer.provider as ethers.Provider,
        depositResponse.hash,
        'protocol deposit',
      );

      return {
        status: 'executed',
        message: 'Protocol deposit executed on-chain.',
        depositTxHash: depositResponse.hash.toLowerCase(),
        ...(approvalTxHash
          ? {
              metadata: {
                approvalTxHash,
                templateKey: params.templateKey,
                signerAddress: signer.address,
              },
            }
          : {
              metadata: {
                templateKey: params.templateKey,
                signerAddress: signer.address,
              },
            }),
      };
    } catch (error: unknown) {
      return {
        status: 'wallet-action-required',
        message: `Protocol deposit execution failed: ${this.errorMessage(error)}`,
      };
    }
  }

  private resolveOptimizerProtocolCallTemplate(
    chainKey: string,
    protocol: string,
  ): ResolvedOptimizerProtocolCallTemplate | null {
    const normalizedChainKey = this.normalizeChainKeyHint(chainKey) || chainKey.trim().toLowerCase();
    const normalizedProtocol = this.normalizeProtocolKey(protocol);
    const candidates = [
      `${normalizedChainKey}:${normalizedProtocol}`,
      `${normalizedChainKey}:*`,
      `*:${normalizedProtocol}`,
      '*:*',
    ];

    for (const key of candidates) {
      const match = this.optimizerProtocolCallTemplates[key];
      if (match) {
        return { key, template: match };
      }
    }

    return null;
  }

  private resolveBorrowProtocolCallTemplate(
    chainKey: string,
    protocol: string,
  ): ResolvedOptimizerProtocolCallTemplate | null {
    const normalizedChainKey = this.normalizeChainKeyHint(chainKey) || chainKey.trim().toLowerCase();
    const normalizedProtocol = this.normalizeProtocolKey(protocol);
    const candidates = [
      `${normalizedChainKey}:${normalizedProtocol}`,
      `${normalizedChainKey}:*`,
      `*:${normalizedProtocol}`,
      '*:*',
    ];

    for (const key of candidates) {
      const explicitBorrowMatch = this.borrowProtocolCallTemplates[key];
      if (explicitBorrowMatch) {
        return { key, template: explicitBorrowMatch };
      }

      const defaultBorrowMatch = DEFAULT_BORROW_PROTOCOL_CALL_TEMPLATES[key];
      if (defaultBorrowMatch) {
        return { key, template: defaultBorrowMatch };
      }

      const fallbackOptimizerMatch = this.optimizerProtocolCallTemplates[key];
      if (fallbackOptimizerMatch) {
        return { key, template: fallbackOptimizerMatch };
      }
    }

    return null;
  }

  private resolveOptimizerProtocolCallTemplates(
    value: string | undefined,
  ): Record<string, OptimizerProtocolCallTemplate> {
    if (!value) {
      return {};
    }

    try {
      const parsed = JSON.parse(value);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        return {};
      }

      const templates: Record<string, OptimizerProtocolCallTemplate> = {};
      for (const [rawKey, rawTemplate] of Object.entries(parsed as Record<string, unknown>)) {
        if (!rawTemplate || typeof rawTemplate !== 'object' || Array.isArray(rawTemplate)) {
          continue;
        }
        const key = rawKey.trim().toLowerCase();
        if (!key) {
          continue;
        }

        const candidate = rawTemplate as Record<string, unknown>;
        const normalizedExecutionType =
          typeof candidate.executionType === 'string'
            ? candidate.executionType.trim().toLowerCase()
            : '';
        const executionType: OptimizerProtocolCallTemplate['executionType'] =
          normalizedExecutionType === 'external'
            ? 'external'
            : normalizedExecutionType === 'synthetic'
              ? 'synthetic'
              : 'evm';
        const template: OptimizerProtocolCallTemplate = {
          ...(normalizedExecutionType ? { executionType } : {}),
          ...(typeof candidate.chainKey === 'string' ? { chainKey: candidate.chainKey } : {}),
          ...(typeof candidate.protocol === 'string' ? { protocol: candidate.protocol } : {}),
          ...(typeof candidate.externalUrl === 'string' ? { externalUrl: candidate.externalUrl } : {}),
          ...(typeof candidate.target === 'string' ? { target: candidate.target } : {}),
          ...(Array.isArray(candidate.abi)
            ? {
                abi: candidate.abi.filter(
                  (item): item is string => typeof item === 'string' && item.trim().length > 0,
                ),
              }
            : {}),
          ...(typeof candidate.method === 'string' ? { method: candidate.method } : {}),
          ...(Array.isArray(candidate.args) ? { args: candidate.args } : {}),
          ...(typeof candidate.value === 'string' ? { value: candidate.value } : {}),
          ...(typeof candidate.approveTokenAddress === 'string'
            ? { approveTokenAddress: candidate.approveTokenAddress }
            : {}),
          ...(typeof candidate.approveSpender === 'string'
            ? { approveSpender: candidate.approveSpender }
            : {}),
          ...(typeof candidate.approveAmount === 'string'
            ? { approveAmount: candidate.approveAmount }
            : {}),
          ...(typeof candidate.requiredSignerMatch === 'boolean'
            ? { requiredSignerMatch: candidate.requiredSignerMatch }
            : {}),
        };
        templates[key] = template;
      }

      return templates;
    } catch {
      return {};
    }
  }

  private buildOptimizerTemplateContext(params: {
    executionId: string;
    protocol: string;
    chainKey: string;
    assetSymbol: string;
    amount: string;
    amountBaseUnits: bigint;
    sourceAddress: string | null;
    destinationAddress: string | null;
    payerAddress: string | null;
    signerAddress: string;
    assetTokenAddress: string;
  }): Record<string, string | bigint | number | null> {
    return {
      executionId: params.executionId,
      protocol: params.protocol,
      protocolKey: this.normalizeProtocolKey(params.protocol),
      chainKey: params.chainKey,
      assetSymbol: params.assetSymbol,
      amount: params.amount,
      amountBaseUnits: params.amountBaseUnits,
      sourceAddress: params.sourceAddress,
      destinationAddress: params.destinationAddress,
      payerAddress: params.payerAddress,
      signerAddress: params.signerAddress,
      assetTokenAddress: params.assetTokenAddress,
    };
  }

  private resolveOptimizerTemplateValue(
    value: unknown,
    context: Record<string, string | bigint | number | null>,
  ): unknown {
    if (Array.isArray(value)) {
      return value.map((entry) => this.resolveOptimizerTemplateValue(entry, context));
    }

    if (value && typeof value === 'object') {
      const resolvedObject: Record<string, unknown> = {};
      for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
        resolvedObject[key] = this.resolveOptimizerTemplateValue(entry, context);
      }
      return resolvedObject;
    }

    if (typeof value === 'string') {
      const fullReferenceMatch = value.trim().match(/^\$([a-zA-Z0-9_]+)$/);
      if (fullReferenceMatch) {
        const key = fullReferenceMatch[1];
        return context[key] ?? null;
      }
      return this.interpolateOptimizerTemplateString(value, context);
    }

    return value;
  }

  private interpolateOptimizerTemplateString(
    value: string,
    context: Record<string, string | bigint | number | null>,
  ): string {
    return value.replace(/\$([a-zA-Z0-9_]+)/g, (_match, key: string) => {
      const resolved = context[key];
      if (typeof resolved === 'undefined' || resolved === null) {
        return '';
      }
      return String(resolved);
    });
  }

  private toNonNegativeBigInt(value: unknown, fieldName: string): bigint {
    if (typeof value === 'bigint') {
      if (value < 0n) {
        throw new BadRequestException(`${fieldName} must be a non-negative integer`);
      }
      return value;
    }

    if (typeof value === 'number') {
      if (!Number.isFinite(value) || !Number.isInteger(value) || value < 0) {
        throw new BadRequestException(`${fieldName} must be a non-negative integer`);
      }
      return BigInt(value);
    }

    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (!/^\d+$/.test(trimmed)) {
        throw new BadRequestException(`${fieldName} must be a non-negative integer`);
      }
      return BigInt(trimmed);
    }

    throw new BadRequestException(`${fieldName} must be a non-negative integer`);
  }

  private normalizeProtocolKey(protocol: string): string {
    const normalized = protocol
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-|-$/g, '');
    return normalized || 'unknown';
  }

  private async applyOptimizerProtocolLimit(
    userId: string,
    rankedYields: RankedYieldEntry[],
    maxProtocols: number | null,
  ): Promise<RankedYieldEntry[]> {
    if (maxProtocols === null) {
      return rankedYields;
    }

    const activeProtocolKeys = await this.getUserActiveProtocolKeys(userId);
    if (activeProtocolKeys.size < maxProtocols) {
      return rankedYields;
    }

    const filtered = rankedYields.filter((entry) =>
      activeProtocolKeys.has(this.normalizeProtocolKey(entry.opportunity.protocol)),
    );

    if (filtered.length > 0) {
      return filtered;
    }

    throw new ForbiddenException(
      `Protocol limit reached (${activeProtocolKeys.size}/${maxProtocols}). ` +
        'Upgrade your plan to invest across more protocols.',
    );
  }

  private async assertOptimizerProtocolAllowed(
    userId: string,
    protocol: string,
    maxProtocols: number | null,
  ): Promise<void> {
    if (maxProtocols === null) {
      return;
    }

    const protocolKey = this.normalizeProtocolKey(protocol);
    const activeProtocolKeys = await this.getUserActiveProtocolKeys(userId);
    const alreadyActive = activeProtocolKeys.has(protocolKey);
    if (alreadyActive || activeProtocolKeys.size < maxProtocols) {
      return;
    }

    throw new ForbiddenException(
      `Protocol limit reached (${activeProtocolKeys.size}/${maxProtocols}). ` +
        'Upgrade your plan to invest across more protocols.',
    );
  }

  private async getUserActiveProtocolKeys(userId: string): Promise<Set<string>> {
    const walletIds = (await this.walletModel
      .find({ userId, isArchived: false } as any)
      .select('_id')
      .lean()
      .exec()) as Array<{ _id?: unknown }>;

    const [positionProtocols, executionProtocols] = await Promise.all([
      walletIds.length > 0
        ? this.defiPositionModel.distinct('protocol', {
            walletId: { $in: walletIds.map((wallet) => wallet._id).filter(Boolean) },
            status: { $ne: 'closed' },
          } as any)
        : Promise.resolve([] as string[]),
      this.optimizerExecutionModel.distinct('protocol', {
        userId,
        status: { $ne: 'failed' },
      } as any),
    ]);

    const keys = new Set<string>();
    for (const protocol of [...positionProtocols, ...executionProtocols]) {
      if (typeof protocol !== 'string') {
        continue;
      }
      const normalized = this.normalizeProtocolKey(protocol);
      if (normalized) {
        keys.add(normalized);
      }
    }
    return keys;
  }

  async getOptimizerHistory(userId: string, limit = 20) {
    const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 100) : 20;
    const records = await this.optimizerExecutionModel
      .find({ userId } as any)
      .sort({ createdAt: -1 })
      .limit(safeLimit)
      .lean()
      .exec();

    return records.map((record) => this.mapOptimizerExecutionRecord(record));
  }

  async getOptimizerFeeSummary(userId: string) {
    const [records, settlements] = await Promise.all([
      this.optimizerExecutionModel.find({ userId } as any).lean().exec(),
      this.optimizerFeeSettlementModel.find({ userId } as any).lean().exec(),
    ]);

    const activeStatuses = new Set<OptimizerExecutionStatus>([
      'planned',
      'bridge-submitted',
      'awaiting-bridge-finality',
      'position-opened',
    ]);
    const activeRecords = records.filter((record) =>
      activeStatuses.has(String(record.status || '') as OptimizerExecutionStatus),
    );

    const totals = activeRecords.reduce(
      (acc, record) => {
        acc.estimatedAnnualFee += this.toPositiveNumber(record.estimatedAnnualFee);
        acc.estimatedAnnualNetYield += this.toPositiveNumber(record.estimatedAnnualNetYield);
        acc.totalPrincipal += this.toPositiveNumber(record.amount);
        acc.avgNetApy += this.toPositiveNumber(record.netApy);
        return acc;
      },
      {
        estimatedAnnualFee: 0,
        estimatedAnnualNetYield: 0,
        totalPrincipal: 0,
        avgNetApy: 0,
      },
    );

    const settledStatuses = new Set<OptimizerFeeSettlementStatus>(['submitted', 'confirmed']);
    const settledRecords = settlements.filter((record) =>
      settledStatuses.has(
        String(record.status || 'wallet-action-required') as OptimizerFeeSettlementStatus,
      ),
    );
    const pendingRecords = settlements.filter(
      (record) => String(record.status || '') === 'wallet-action-required',
    );

    const settlementTotals = settledRecords.reduce(
      (acc, record) => {
        acc.realizedProfitSettled += this.toPositiveNumber(record.realizedProfitAmount);
        acc.performanceFeeSettled += this.toPositiveNumber(record.feeAmount);
        return acc;
      },
      { realizedProfitSettled: 0, performanceFeeSettled: 0 },
    );
    const performanceFeePending = pendingRecords.reduce(
      (sum, record) => sum + this.toPositiveNumber(record.feeAmount),
      0,
    );

    return {
      executionCount: records.length,
      activeExecutionCount: activeRecords.length,
      totalPrincipal: this.roundTo4Decimals(totals.totalPrincipal),
      estimatedAnnualFee: this.roundTo4Decimals(totals.estimatedAnnualFee),
      estimatedAnnualNetYield: this.roundTo4Decimals(totals.estimatedAnnualNetYield),
      averageNetApy:
        activeRecords.length > 0
          ? this.roundTo4Decimals(totals.avgNetApy / activeRecords.length)
          : 0,
      settlementCount: settlements.length,
      settledCount: settledRecords.length,
      pendingSettlementCount: pendingRecords.length,
      realizedProfitSettled: this.roundTo4Decimals(settlementTotals.realizedProfitSettled),
      performanceFeeSettled: this.roundTo4Decimals(settlementTotals.performanceFeeSettled),
      performanceFeePending: this.roundTo4Decimals(performanceFeePending),
    };
  }

  async getPlatformFeeSummary() {
    return this.platformFeeService.getSummary();
  }

  async getPlatformFeeRecords(options: {
    limit: number;
    status?: PlatformFeeStatus;
    action?: PlatformFeeAction;
  }) {
    return this.platformFeeService.listRecords(options);
  }

  async collectPlatformFees(ownerUserId: string, dto: CollectPlatformFeesDto) {
    return this.platformFeeService.collectAccruedFees(ownerUserId, {
      feeIds: dto.feeIds,
      limit: dto.limit,
      collectionTxHash: dto.collectionTxHash,
    });
  }

  async settleOptimizerFee(userId: string, dto: SettleOptimizerFeeDto) {
    const execution = await this.optimizerExecutionModel
      .findOne({ _id: dto.optimizerExecutionId, userId } as any)
      .lean()
      .exec();

    if (!execution) {
      throw new NotFoundException('Optimizer execution not found');
    }

    const chainKey =
      this.normalizeChainKeyHint(dto.chainKey || String(execution.destinationChainKey || '')) ||
      this.normalizeChainKeyHint(String(execution.sourceChainKey || ''));
    if (!chainKey) {
      throw new BadRequestException('Unable to resolve settlement chain');
    }

    const assetSymbol = (dto.assetSymbol || String(execution.assetSymbol || '')).trim().toUpperCase();
    if (!assetSymbol) {
      throw new BadRequestException('assetSymbol is required');
    }

    const performanceFeeBps =
      typeof dto.performanceFeeBps === 'number'
        ? dto.performanceFeeBps
        : Math.floor(this.toPositiveNumber(execution.performanceFeeBps));
    if (!Number.isInteger(performanceFeeBps) || performanceFeeBps < 0 || performanceFeeBps > 5000) {
      throw new BadRequestException('performanceFeeBps must be between 0 and 5000');
    }

    const realizedProfitAmount = this.normalizeDecimal(dto.realizedProfitAmount);
    const realizedProfitNumber = Number(realizedProfitAmount);
    if (!Number.isFinite(realizedProfitNumber) || realizedProfitNumber <= 0) {
      throw new BadRequestException('realizedProfitAmount must be greater than zero');
    }

    const token = await this.resolveFeeTokenForSettlement(chainKey, assetSymbol);
    const realizedProfitBaseUnits = this.toBaseUnits(
      realizedProfitAmount,
      token.decimals,
      'realizedProfitAmount',
    );
    const feeBaseUnits = (realizedProfitBaseUnits * BigInt(performanceFeeBps)) / 10000n;
    if (feeBaseUnits <= 0n) {
      throw new BadRequestException('Calculated fee amount is zero for this realized profit value');
    }

    const feeAmount = this.fromBaseUnits(feeBaseUnits, token.decimals);
    const payerCandidate =
      dto.payerAddress ||
      this.normalizeAddressString(execution.destinationAddress) ||
      this.normalizeAddressString(execution.sourceAddress) ||
      (await this.resolveConnectedWalletAddressForUser(userId, chainKey));
    const payerAddress = this.normalizeFeeSettlementAddress(chainKey, payerCandidate, 'payerAddress');
    const collectorAddress = this.resolveOptimizerFeeCollectorAddress(chainKey);

    const settleOnBackend = dto.settleOnBackend ?? this.optimizerFeeSettleOnBackendDefault;
    let settlementMode: OptimizerFeeSettlementMode = settleOnBackend ? 'backend' : 'wallet';
    let status: OptimizerFeeSettlementStatus = 'wallet-action-required';
    let txHash: string | undefined;
    let txPayload: Record<string, unknown> | undefined;
    let error: string | undefined;
    let nextAction: 'wallet-sign-required' | 'awaiting-confirmation' | 'none' =
      'wallet-sign-required';
    let executedOnBackend = false;

    if (chainKey === STELLAR_CHAIN_KEY) {
      const stellarPayload = await this.buildStellarFeeSettlementPayload({
        payerAddress,
        collectorAddress,
        token,
        feeAmount,
      });
      txPayload = stellarPayload;

      if (settleOnBackend) {
        const backendResult = await this.tryExecuteStellarFeeSettlementOnBackend({
          payerAddress,
          xdr: stellarPayload.xdr,
        });
        if (backendResult.executed) {
          txHash = backendResult.txHash;
          status = 'confirmed';
          nextAction = 'none';
          executedOnBackend = true;
          txPayload = {
            ...stellarPayload,
            txHash,
          };
        } else {
          settlementMode = 'wallet';
          error = backendResult.reason;
        }
      }
    } else {
      const evmPayload = await this.buildEvmFeeSettlementPayload({
        chainKey,
        payerAddress,
        collectorAddress,
        token,
        feeBaseUnits,
        feeAmount,
      });
      txPayload = evmPayload;

      if (settleOnBackend) {
        const backendResult = await this.tryExecuteEvmFeeSettlementOnBackend({
          chainKey,
          payerAddress,
          transaction: evmPayload.transaction,
        });
        if (backendResult.executed) {
          txHash = backendResult.txHash;
          status = 'submitted';
          nextAction = 'awaiting-confirmation';
          executedOnBackend = true;
          txPayload = {
            ...evmPayload,
            txHash,
          };
        } else {
          settlementMode = 'wallet';
          error = backendResult.reason;
        }
      }
    }

    const settlement = await this.optimizerFeeSettlementModel.create({
      userId,
      optimizerExecutionId: String((execution as any)._id || dto.optimizerExecutionId),
      chainKey,
      assetSymbol: token.symbol,
      payerAddress,
      collectorAddress,
      realizedProfitAmount,
      performanceFeeBps,
      feeAmount,
      settlementMode,
      status,
      ...(txHash ? { txHash } : {}),
      ...(txPayload ? { txPayload } : {}),
      ...(error ? { error } : {}),
      metadata: {
        optimizerExecutionStatus: String(execution.status || 'unknown'),
        settleOnBackendRequested: settleOnBackend,
        executedOnBackend,
      },
    } as Partial<OptimizerFeeSettlement>);

    if (txHash) {
      await this.recordOptimizerFeeTransaction({
        userId,
        chainKey,
        hash: txHash,
        payerAddress,
        collectorAddress,
        amount: feeAmount,
        assetSymbol: token.symbol,
        optimizerExecutionId: String((execution as any)._id || dto.optimizerExecutionId),
        settlementId: String((settlement as any)._id || ''),
        settlementStatus: status,
      });
    }

    return {
      settlement: this.mapOptimizerFeeSettlementRecord(
        settlement.toObject() as unknown as Record<string, unknown>,
      ),
      nextAction,
      executedOnBackend,
      ...(error ? { warning: error } : {}),
    };
  }

  async getOptimizerFeeSettlements(userId: string, limit = 20) {
    const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 100) : 20;
    const records = await this.optimizerFeeSettlementModel
      .find({ userId } as any)
      .sort({ createdAt: -1 })
      .limit(safeLimit)
      .lean()
      .exec();

    return records.map((record) => this.mapOptimizerFeeSettlementRecord(record));
  }

  async confirmOptimizerFeeSettlement(userId: string, settlementId: string, txHash: string) {
    const settlement = await this.optimizerFeeSettlementModel
      .findOne({ _id: settlementId, userId } as any)
      .exec();

    if (!settlement) {
      throw new NotFoundException('Optimizer fee settlement not found');
    }

    const normalizedTxHash = this.normalizeFeeSettlementTxHash(settlement.chainKey, txHash);
    settlement.txHash = normalizedTxHash;
    settlement.status = 'confirmed';
    settlement.error = undefined;
    await settlement.save();

    await this.recordOptimizerFeeTransaction({
      userId,
      chainKey: settlement.chainKey,
      hash: normalizedTxHash,
      payerAddress: settlement.payerAddress,
      collectorAddress: settlement.collectorAddress,
      amount: settlement.feeAmount,
      assetSymbol: settlement.assetSymbol,
      optimizerExecutionId: settlement.optimizerExecutionId,
      settlementId,
      settlementStatus: 'confirmed',
    });

    return this.mapOptimizerFeeSettlementRecord(
      settlement.toObject() as unknown as Record<string, unknown>,
    );
  }

  async scanForPositions(walletId: string) {
    return [];
  }

  async getBridgeChains() {
    const chains = await this.fetchStargateChains();

    const evmChains = chains
      .filter((chain) => (chain.chainType || 'evm').toLowerCase() === 'evm')
      .map((chain) => ({
        chainKey: chain.chainKey,
        chainId: Number.isFinite(chain.chainId) ? Number(chain.chainId) : null,
        shortName: chain.shortName || chain.name || chain.chainKey,
        name: chain.name || chain.shortName || chain.chainKey,
        nativeCurrency: {
          symbol: chain.nativeCurrency?.symbol || 'NATIVE',
          decimals: Number(chain.nativeCurrency?.decimals ?? 18),
          address:
            chain.nativeCurrency?.address || '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',
        },
      }));

    const stellarChain = {
      chainKey: STELLAR_CHAIN_KEY,
      chainId: null,
      shortName: 'Stellar',
      name: 'Stellar',
      nativeCurrency: {
        symbol: 'XLM',
        decimals: 7,
        address: 'native',
      },
    };

    const shouldExposeStellarBridge =
      this.allowExternalBridgeRoutes || this.isStellarCircleCctpV2InAppConfigured();
    const withStellar = evmChains.some((chain) => chain.chainKey === STELLAR_CHAIN_KEY)
      ? evmChains
      : shouldExposeStellarBridge
        ? [...evmChains, stellarChain]
        : evmChains;

    return withStellar.sort((left, right) => left.name.localeCompare(right.name));
  }

  async getBridgeTokens(chainKey?: string, bridgeableOnly = true) {
    const normalizedChainKey = chainKey?.trim().toLowerCase();

    if (normalizedChainKey === STELLAR_CHAIN_KEY) {
      return this.getStellarBridgeTokens(bridgeableOnly);
    }

    if (normalizedChainKey) {
      const chains = await this.fetchStargateChains();
      const knownChain = chains.some(
        (chain) => chain.chainKey.toLowerCase() === normalizedChainKey,
      );
      if (!knownChain) {
        throw new BadRequestException(`Unsupported chainKey "${chainKey}"`);
      }
    }

    const tokens = await this.fetchStargateTokens();
    const stargateMapped = tokens
      .filter((token) =>
        normalizedChainKey ? token.chainKey.toLowerCase() === normalizedChainKey : true,
      )
      .filter((token) => (bridgeableOnly ? Boolean(token.isBridgeable) : true))
      .sort((left, right) => {
        const byChain = left.chainKey.localeCompare(right.chainKey);
        if (byChain !== 0) {
          return byChain;
        }
        return left.symbol.localeCompare(right.symbol);
      })
      .map((token) => ({
        chainKey: token.chainKey,
        address: this.normalizeAddressIfPossible(token.address) || token.address,
        decimals: token.decimals,
        symbol: token.symbol,
        name: token.name || token.symbol,
        isBridgeable: Boolean(token.isBridgeable),
        priceUsd:
          typeof token.price?.usd === 'number' && Number.isFinite(token.price.usd)
            ? token.price.usd
            : null,
      }));

    if (normalizedChainKey) {
      return stargateMapped;
    }

    return [...stargateMapped, ...this.getStellarBridgeTokens(bridgeableOnly)].sort((left, right) => {
      const byChain = left.chainKey.localeCompare(right.chainKey);
      if (byChain !== 0) {
        return byChain;
      }
      return left.symbol.localeCompare(right.symbol);
    });
  }

  async getBridgeQuote(dto: BridgeQuoteDto) {
    const srcChainKey = dto.srcChainKey.trim().toLowerCase();
    const dstChainKey = dto.dstChainKey.trim().toLowerCase();
    if (srcChainKey === dstChainKey) {
      throw new BadRequestException('Source and destination chains must differ');
    }

    if (this.isStellarRoute(srcChainKey, dstChainKey)) {
      return this.getStellarBridgeQuote(dto, srcChainKey, dstChainKey);
    }

    const srcAddress = this.normalizeEvmAddress(dto.srcAddress, 'srcAddress');
    const dstAddress = dto.dstAddress
      ? this.normalizeEvmAddress(dto.dstAddress, 'dstAddress')
      : srcAddress;

    const [srcToken, dstToken, chains, tokens] = await Promise.all([
      this.resolveStargateToken(srcChainKey, dto.srcToken, {
        requireBridgeable: true,
      }),
      this.resolveStargateToken(dstChainKey, dto.dstToken, {
        requireBridgeable: false,
      }),
      this.fetchStargateChains(),
      this.fetchStargateTokens(),
    ]);

    const srcAmountBaseUnits = this.toBaseUnits(dto.srcAmount, srcToken.decimals, 'srcAmount');
    if (srcAmountBaseUnits <= 0n) {
      throw new BadRequestException('srcAmount must be greater than zero');
    }

    const slippageBps = dto.slippageBps ?? 100;
    let dstAmountMinBaseUnits = dto.dstAmountMin
      ? this.toBaseUnits(dto.dstAmountMin, dstToken.decimals, 'dstAmountMin')
      : null;

    if (!dstAmountMinBaseUnits || dstAmountMinBaseUnits <= 0n) {
      const previewQuotes = await this.fetchStargateQuotes({
        srcToken: srcToken.address,
        dstToken: dstToken.address,
        srcAddress,
        dstAddress,
        srcChainKey,
        dstChainKey,
        srcAmount: srcAmountBaseUnits.toString(),
        dstAmountMin: '1',
      });

      const previewQuote = this.selectPreferredRawQuote(previewQuotes, dto.route);
      if (!previewQuote) {
        if (this.allowExternalBridgeRoutes) {
          const fallbackQuote = this.buildStargateExternalFallbackQuote({
            srcChainKey,
            dstChainKey,
            srcAddress,
            dstAddress,
            srcToken,
            dstToken,
            srcAmountBaseUnits,
            slippageBps,
            chains,
          });

          if (fallbackQuote) {
            return {
              srcChainKey,
              dstChainKey,
              srcAddress,
              dstAddress,
              srcToken: {
                address: srcToken.address,
                symbol: srcToken.symbol,
                decimals: srcToken.decimals,
              },
              dstToken: {
                address: dstToken.address,
                symbol: dstToken.symbol,
                decimals: dstToken.decimals,
              },
              srcAmount: this.normalizeDecimal(dto.srcAmount),
              srcAmountBaseUnits: srcAmountBaseUnits.toString(),
              dstAmountMin: fallbackQuote.dstAmountMin,
              dstAmountMinBaseUnits: fallbackQuote.dstAmountMinBaseUnits,
              slippageBps,
              routeCount: 1,
              recommendedRoute: fallbackQuote.route,
              quotes: [fallbackQuote],
            };
          }
        }

        return this.buildEmptyStargateQuoteResponse({
          srcChainKey,
          dstChainKey,
          srcAddress,
          dstAddress,
          srcToken,
          dstToken,
          srcAmount: dto.srcAmount,
          srcAmountBaseUnits,
          dstAmountMinBaseUnits: null,
          slippageBps,
        });
      }

      const previewDstAmount = this.parseRequiredBigInt(previewQuote.dstAmount, 'dstAmount');
      dstAmountMinBaseUnits = this.applySlippage(previewDstAmount, slippageBps);
    }

    const rawQuotes = await this.fetchStargateQuotes({
      srcToken: srcToken.address,
      dstToken: dstToken.address,
      srcAddress,
      dstAddress,
      srcChainKey,
      dstChainKey,
      srcAmount: srcAmountBaseUnits.toString(),
      dstAmountMin: dstAmountMinBaseUnits.toString(),
    });

    let quotes = this.normalizeBridgeQuotes(rawQuotes, {
      srcToken,
      dstToken,
      chains,
      tokens,
    });

    const hasExecutableQuote = Boolean(this.selectPreferredBridgeQuote(quotes, dto.route));
    if (!hasExecutableQuote && this.allowExternalBridgeRoutes) {
      const fallbackQuote = this.buildStargateExternalFallbackQuote({
        srcChainKey,
        dstChainKey,
        srcAddress,
        dstAddress,
        srcToken,
        dstToken,
        srcAmountBaseUnits,
        slippageBps,
        chains,
      });

      if (fallbackQuote) {
        quotes = [...quotes, fallbackQuote];
      }
    }

    if (quotes.length === 0) {
      return this.buildEmptyStargateQuoteResponse({
        srcChainKey,
        dstChainKey,
        srcAddress,
        dstAddress,
        srcToken,
        dstToken,
        srcAmount: dto.srcAmount,
        srcAmountBaseUnits,
        dstAmountMinBaseUnits,
        slippageBps,
      });
    }

    const recommendedQuote = this.selectPreferredBridgeQuote(quotes, dto.route);

    return {
      srcChainKey,
      dstChainKey,
      srcAddress,
      dstAddress,
      srcToken: {
        address: srcToken.address,
        symbol: srcToken.symbol,
        decimals: srcToken.decimals,
      },
      dstToken: {
        address: dstToken.address,
        symbol: dstToken.symbol,
        decimals: dstToken.decimals,
      },
      srcAmount: this.normalizeDecimal(dto.srcAmount),
      srcAmountBaseUnits: srcAmountBaseUnits.toString(),
      dstAmountMin: this.fromBaseUnits(dstAmountMinBaseUnits, dstToken.decimals),
      dstAmountMinBaseUnits: dstAmountMinBaseUnits.toString(),
      slippageBps,
      routeCount: quotes.length,
      recommendedRoute: recommendedQuote?.route || null,
      quotes,
    };
  }

  private buildEmptyStargateQuoteResponse(params: {
    srcChainKey: string;
    dstChainKey: string;
    srcAddress: string;
    dstAddress: string;
    srcToken: StargateToken;
    dstToken: StargateToken;
    srcAmount: string;
    srcAmountBaseUnits: bigint;
    dstAmountMinBaseUnits: bigint | null;
    slippageBps: number;
  }) {
    const dstAmountMinBaseUnits =
      params.dstAmountMinBaseUnits && params.dstAmountMinBaseUnits > 0n
        ? params.dstAmountMinBaseUnits
        : 0n;

    return {
      srcChainKey: params.srcChainKey,
      dstChainKey: params.dstChainKey,
      srcAddress: params.srcAddress,
      dstAddress: params.dstAddress,
      srcToken: {
        address: params.srcToken.address,
        symbol: params.srcToken.symbol,
        decimals: params.srcToken.decimals,
      },
      dstToken: {
        address: params.dstToken.address,
        symbol: params.dstToken.symbol,
        decimals: params.dstToken.decimals,
      },
      srcAmount: this.normalizeDecimal(params.srcAmount),
      srcAmountBaseUnits: params.srcAmountBaseUnits.toString(),
      dstAmountMin: this.fromBaseUnits(dstAmountMinBaseUnits, params.dstToken.decimals),
      dstAmountMinBaseUnits: dstAmountMinBaseUnits.toString(),
      slippageBps: params.slippageBps,
      routeCount: 0,
      recommendedRoute: null,
      quotes: [],
    };
  }

  async buildBridgeTransaction(dto: BuildBridgeTransactionDto, userId?: string) {
    const srcChainKey = dto.srcChainKey.trim().toLowerCase();
    const dstChainKey = dto.dstChainKey.trim().toLowerCase();

    if (this.isStellarRoute(srcChainKey, dstChainKey)) {
      return this.buildStellarBridgeTransaction(dto, srcChainKey, dstChainKey, userId);
    }

    if (dto.executeOnBackend && !this.bridgeExecutorEnabled) {
      throw new BadRequestException(
        'Backend bridge execution is disabled. Set BRIDGE_EXECUTOR_ENABLED=true.',
      );
    }

    const shouldExecuteOnBackend = Boolean(dto.executeOnBackend) && this.bridgeExecutorEnabled;
    const quoteRequest = shouldExecuteOnBackend
      ? this.buildBackendExecutionQuoteRequest(dto, srcChainKey)
      : dto;

    const quoteResponse = await this.getBridgeQuote(quoteRequest);
    const selectedQuote = this.selectPreferredBridgeQuote(quoteResponse.quotes, dto.route);

    if (!selectedQuote) {
      throw new BadRequestException('No valid Stargate quote available for execution');
    }

    const platformFeeQuote =
      !shouldExecuteOnBackend
        ? this.platformFeeService.buildQuote({
            action: 'bridge',
            chainKey: selectedQuote.srcChainKey,
            payerAddress: dto.srcAddress,
            asset: quoteResponse.srcToken.address,
            assetSymbol: quoteResponse.srcToken.symbol,
            amount: selectedQuote.srcAmount,
            decimals: quoteResponse.srcToken.decimals,
          })
        : null;
    const platformFeeTransaction = platformFeeQuote
      ? this.buildEvmPlatformFeeTransferTransaction(
          platformFeeQuote,
          quoteResponse.srcToken.address,
          dto.srcAddress,
        )
      : null;
    const platformFeeReference = platformFeeQuote
      ? this.platformFeeService.buildReference([
          'bridge',
          userId || '',
          selectedQuote.srcChainKey,
          dto.srcAddress,
          dto.dstAddress || '',
          selectedQuote.route,
          selectedQuote.srcAmountBaseUnits,
          platformFeeTransaction ? JSON.stringify(platformFeeTransaction) : '',
        ])
      : undefined;
    const platformFeeRecord =
      userId && platformFeeQuote
        ? await this.platformFeeService.accrueFee({
            userId,
            quote: platformFeeQuote,
            reference: platformFeeReference,
            metadata: {
              route: selectedQuote.route,
              srcToken: quoteResponse.srcToken.address,
              dstToken: quoteResponse.dstToken.address,
              executionType: selectedQuote.executionType,
              executeOnBackend: shouldExecuteOnBackend,
            },
          })
        : null;
    const platformFeePayload = platformFeeQuote
      ? {
          action: platformFeeQuote.action,
          feeBps: platformFeeQuote.feeBps,
          amount: platformFeeQuote.feeAmount,
          asset: platformFeeQuote.asset,
          assetSymbol: platformFeeQuote.assetSymbol,
          collectorAddress: platformFeeQuote.collectorAddress,
          ...(platformFeeRecord ? { feeRecordId: platformFeeRecord.id } : {}),
        }
      : null;

    if (selectedQuote.executionType === 'external') {
      if (!this.allowExternalBridgeRoutes) {
        throw new BadRequestException(
          'External bridge routes are disabled. Use an in-app route with executable transactions.',
        );
      }
      if (!selectedQuote.externalUrl) {
        throw new BadRequestException('Selected external bridge route is missing provider URL');
      }

      return {
        route: selectedQuote.route,
        srcChainKey: selectedQuote.srcChainKey,
        dstChainKey: selectedQuote.dstChainKey,
        srcChainId: selectedQuote.srcChainId,
        dstChainId: selectedQuote.dstChainId,
        srcAmount: selectedQuote.srcAmount,
        srcAmountBaseUnits: selectedQuote.srcAmountBaseUnits,
        dstAmount: selectedQuote.dstAmount,
        dstAmountBaseUnits: selectedQuote.dstAmountBaseUnits,
        dstAmountMin: selectedQuote.dstAmountMin,
        dstAmountMinBaseUnits: selectedQuote.dstAmountMinBaseUnits,
        estimatedDurationSeconds: selectedQuote.estimatedDurationSeconds,
        fees: selectedQuote.fees,
        approvalTransaction: null,
        bridgeTransaction: null,
        executionType: selectedQuote.executionType,
        externalUrl: selectedQuote.externalUrl,
        ...(platformFeePayload ? { platformFee: platformFeePayload } : {}),
        ...(platformFeeTransaction ? { platformFeeTransaction } : {}),
      };
    }

    if (!selectedQuote.transactions.bridge) {
      throw new BadRequestException('Selected route does not include an executable bridge step');
    }

    if (shouldExecuteOnBackend) {
      const executionResult = await this.executeEvmBridgeQuoteOnBackend(selectedQuote);

      return {
        route: selectedQuote.route,
        srcChainKey: selectedQuote.srcChainKey,
        dstChainKey: selectedQuote.dstChainKey,
        srcChainId: selectedQuote.srcChainId,
        dstChainId: selectedQuote.dstChainId,
        srcAmount: selectedQuote.srcAmount,
        srcAmountBaseUnits: selectedQuote.srcAmountBaseUnits,
        dstAmount: selectedQuote.dstAmount,
        dstAmountBaseUnits: selectedQuote.dstAmountBaseUnits,
        dstAmountMin: selectedQuote.dstAmountMin,
        dstAmountMinBaseUnits: selectedQuote.dstAmountMinBaseUnits,
        estimatedDurationSeconds: selectedQuote.estimatedDurationSeconds,
        fees: selectedQuote.fees,
        approvalTransaction: null,
        bridgeTransaction: null,
        executionType: selectedQuote.executionType,
        externalUrl: selectedQuote.externalUrl,
        executedOnBackend: true,
        bridgeTxHash: executionResult.bridgeTxHash,
        executionStatus: 'submitted',
        executorAddress: executionResult.executorAddress,
        ...(executionResult.approvalTxHash ? { approvalTxHash: executionResult.approvalTxHash } : {}),
        ...(platformFeePayload ? { platformFee: platformFeePayload } : {}),
        ...(platformFeeTransaction ? { platformFeeTransaction } : {}),
      };
    }

    return {
      route: selectedQuote.route,
      srcChainKey: selectedQuote.srcChainKey,
      dstChainKey: selectedQuote.dstChainKey,
      srcChainId: selectedQuote.srcChainId,
      dstChainId: selectedQuote.dstChainId,
      srcAmount: selectedQuote.srcAmount,
      srcAmountBaseUnits: selectedQuote.srcAmountBaseUnits,
      dstAmount: selectedQuote.dstAmount,
      dstAmountBaseUnits: selectedQuote.dstAmountBaseUnits,
      dstAmountMin: selectedQuote.dstAmountMin,
      dstAmountMinBaseUnits: selectedQuote.dstAmountMinBaseUnits,
      estimatedDurationSeconds: selectedQuote.estimatedDurationSeconds,
      fees: selectedQuote.fees,
      approvalTransaction: selectedQuote.transactions.approve,
      bridgeTransaction: selectedQuote.transactions.bridge,
      executionType: selectedQuote.executionType,
      externalUrl: selectedQuote.externalUrl,
      ...(platformFeePayload ? { platformFee: platformFeePayload } : {}),
      ...(platformFeeTransaction ? { platformFeeTransaction } : {}),
    };
  }

  private buildEvmPlatformFeeTransferTransaction(
    quote: PlatformFeeQuote,
    tokenAddress: string,
    fromAddress?: string,
  ): EvmTransactionRequest | null {
    const normalizedFrom = fromAddress ? this.normalizeAddressIfPossible(fromAddress) : null;
    const normalizedTokenAddress = this.normalizeAddressIfPossible(tokenAddress) || tokenAddress;

    if (this.isNativeTokenAddress(normalizedTokenAddress)) {
      return {
        to: quote.collectorAddress,
        data: '0x',
        value: quote.feeBaseUnits,
        ...(normalizedFrom ? { from: normalizedFrom } : {}),
      };
    }

    const tokenContractAddress = this.normalizeAddressIfPossible(normalizedTokenAddress);
    if (!tokenContractAddress) {
      return null;
    }

    const transferData = new ethers.Interface(ERC20_TRANSFER_ABI).encodeFunctionData('transfer', [
      quote.collectorAddress,
      BigInt(quote.feeBaseUnits),
    ]);

    return {
      to: tokenContractAddress,
      data: transferData,
      value: '0',
      ...(normalizedFrom ? { from: normalizedFrom } : {}),
    };
  }

  private async buildStellarPlatformFeePaymentPayload(params: {
    payerAddress: string;
    collectorAddress: string;
    asset: string;
    amount: string;
  }): Promise<{
    payerAddress: string;
    collectorAddress: string;
    asset: string;
    amount: string;
    fee: string;
    xdr: string;
    network: 'testnet' | 'mainnet';
  }> {
    if (!this.looksLikeStellarAddress(params.payerAddress)) {
      throw new BadRequestException('payerAddress must be a valid Stellar address');
    }
    if (!this.looksLikeStellarAddress(params.collectorAddress)) {
      throw new BadRequestException('collectorAddress must be a valid Stellar address');
    }

    const account = await this.server.loadAccount(params.payerAddress);
    const fee = await this.feeEstimationService.estimateFee('stellar', 'payment');
    const builder = new StellarSdk.TransactionBuilder(account, {
      fee,
      networkPassphrase: this.getNetworkPassphrase(),
    });

    builder.addOperation(
      StellarSdk.Operation.payment({
        destination: params.collectorAddress,
        asset: this.parseAsset(params.asset),
        amount: params.amount,
      }),
    );
    builder.setTimeout(180);
    const transaction = builder.build();

    return {
      payerAddress: params.payerAddress,
      collectorAddress: params.collectorAddress,
      asset: params.asset,
      amount: params.amount,
      fee,
      xdr: transaction.toXDR(),
      network:
        this.getNetworkPassphrase() === StellarSdk.Networks.PUBLIC ? 'mainnet' : 'testnet',
    };
  }

  private buildBackendExecutionQuoteRequest(
    dto: BuildBridgeTransactionDto,
    srcChainKey: string,
  ): BuildBridgeTransactionDto {
    const executorAddress = this.resolveBridgeExecutorAddress(srcChainKey);
    const destinationAddress = dto.dstAddress?.trim() ? dto.dstAddress.trim() : dto.srcAddress.trim();

    return {
      ...dto,
      srcAddress: executorAddress,
      dstAddress: destinationAddress,
    };
  }

  private async executeEvmBridgeQuoteOnBackend(quote: NormalizedBridgeQuote): Promise<{
    approvalTxHash?: string;
    bridgeTxHash: string;
    executorAddress: string;
  }> {
    if (!quote.transactions.bridge) {
      throw new BadRequestException('Selected route does not include an executable bridge step');
    }

    const rpcUrl = this.resolveRpcUrlForChain(quote.srcChainKey);
    if (!rpcUrl) {
      throw new BadRequestException(
        `No RPC URL configured for chain "${quote.srcChainKey}". ` +
          `Set STARGATE_EVM_RPC_URLS or STARGATE_RPC_URL_${this.chainEnvSuffix(quote.srcChainKey)}.`,
      );
    }

    const signer = this.createBridgeExecutorSigner(quote.srcChainKey, rpcUrl);
    const provider = signer.provider;
    if (!provider) {
      throw new BadRequestException('Bridge execution signer is missing a provider');
    }

    if (typeof quote.srcChainId === 'number') {
      const network = await provider.getNetwork();
      if (Number(network.chainId) !== quote.srcChainId) {
        throw new BadRequestException(
          `RPC chain mismatch for "${quote.srcChainKey}". Expected ${quote.srcChainId}, got ${String(network.chainId)}.`,
        );
      }
    }

    let approvalTxHash: string | undefined;
    if (quote.transactions.approve) {
      const approvalResponse = await signer.sendTransaction(
        this.toSignerTransactionRequest(quote.transactions.approve),
      );
      approvalTxHash = approvalResponse.hash.toLowerCase();
      await this.waitForBridgeExecutionReceipt(provider, approvalResponse.hash, 'approval');
    }

    const bridgeResponse = await signer.sendTransaction(
      this.toSignerTransactionRequest(quote.transactions.bridge),
    );

    return {
      ...(approvalTxHash ? { approvalTxHash } : {}),
      bridgeTxHash: bridgeResponse.hash.toLowerCase(),
      executorAddress: signer.address,
    };
  }

  private toSignerTransactionRequest(tx: EvmTransactionRequest): ethers.TransactionRequest {
    return {
      to: tx.to,
      data: tx.data,
      value: this.parseBigIntOrZero(tx.value),
    };
  }

  private async waitForBridgeExecutionReceipt(
    provider: ethers.Provider,
    txHash: string,
    transactionLabel: string,
  ): Promise<void> {
    const receipt = await provider.waitForTransaction(
      txHash,
      1,
      this.bridgeExecutionReceiptTimeoutMs,
    );
    if (!receipt) {
      throw new BadRequestException(
        `Timed out waiting for ${transactionLabel} transaction confirmation (${txHash})`,
      );
    }

    if (receipt.status !== 1) {
      throw new BadRequestException(
        `Bridge ${transactionLabel} transaction failed on-chain (${txHash})`,
      );
    }
  }

  private buildStargateExternalFallbackQuote(params: {
    srcChainKey: string;
    dstChainKey: string;
    srcAddress: string;
    dstAddress: string;
    srcToken: StargateToken;
    dstToken: StargateToken;
    srcAmountBaseUnits: bigint;
    slippageBps: number;
    chains: StargateChain[];
  }): NormalizedBridgeQuote | null {
    if (!this.allowExternalBridgeRoutes) {
      return null;
    }

    const dstAmountBaseUnits = this.rescaleBaseUnits(
      params.srcAmountBaseUnits,
      params.srcToken.decimals,
      params.dstToken.decimals,
    );
    const dstAmountMinBaseUnits = this.applySlippage(dstAmountBaseUnits, params.slippageBps);
    const srcAmount = this.fromBaseUnits(params.srcAmountBaseUnits, params.srcToken.decimals);
    const dstAmount = this.fromBaseUnits(dstAmountBaseUnits, params.dstToken.decimals);
    const dstAmountMin = this.fromBaseUnits(dstAmountMinBaseUnits, params.dstToken.decimals);
    const externalUrl = this.buildStargateExternalFallbackUrl({
      srcChainKey: params.srcChainKey,
      dstChainKey: params.dstChainKey,
      srcToken: params.srcToken.address,
      dstToken: params.dstToken.address,
      srcAmount,
      dstAmountMin,
      srcAddress: params.srcAddress,
      dstAddress: params.dstAddress,
    });

    if (!externalUrl) {
      return null;
    }

    return {
      route: `${this.stargateExternalFallbackProviderName}/${params.srcChainKey}-${params.dstChainKey}`,
      executionType: 'external',
      externalUrl,
      srcChainKey: params.srcChainKey,
      dstChainKey: params.dstChainKey,
      srcChainId: this.chainIdByKey(params.chains, params.srcChainKey),
      dstChainId: this.chainIdByKey(params.chains, params.dstChainKey),
      srcAmount,
      srcAmountBaseUnits: params.srcAmountBaseUnits.toString(),
      dstAmount,
      dstAmountBaseUnits: dstAmountBaseUnits.toString(),
      dstAmountMin,
      dstAmountMinBaseUnits: dstAmountMinBaseUnits.toString(),
      estimatedDurationSeconds: 900,
      fees: [],
      transactions: {
        approve: null,
        bridge: null,
      },
      error: null,
    };
  }

  private isStellarRoute(srcChainKey: string, dstChainKey: string): boolean {
    return srcChainKey === STELLAR_CHAIN_KEY || dstChainKey === STELLAR_CHAIN_KEY;
  }

  private getStellarBridgeTokens(bridgeableOnly = true): BridgeTokenInfo[] {
    const tokens: BridgeTokenInfo[] = [
      {
        chainKey: STELLAR_CHAIN_KEY,
        address: 'native',
        decimals: 7,
        symbol: 'XLM',
        name: 'Stellar Lumens',
        // Native XLM bridging currently depends on external providers.
        isBridgeable: this.allowExternalBridgeRoutes,
        priceUsd: null,
      },
      {
        chainKey: STELLAR_CHAIN_KEY,
        address: `USDC:${STELLAR_USDC_ISSUER}`,
        decimals: 7,
        symbol: 'USDC',
        name: 'USD Coin',
        isBridgeable: true,
        priceUsd: null,
      },
    ];

    return bridgeableOnly ? tokens.filter((token) => token.isBridgeable) : tokens;
  }

  private resolveStellarBridgeToken(tokenIdentifier: string): BridgeTokenInfo {
    const normalizedIdentifier = tokenIdentifier.trim().toLowerCase();
    const match = this.getStellarBridgeTokens(false).find(
      (token) =>
        token.address.toLowerCase() === normalizedIdentifier ||
        token.symbol.toLowerCase() === normalizedIdentifier,
    );

    if (!match) {
      throw new BadRequestException(
        `Unknown token "${tokenIdentifier}" for chain "${STELLAR_CHAIN_KEY}"`,
      );
    }

    return match;
  }

  private toBridgeTokenInfo(token: StargateToken): BridgeTokenInfo {
    return {
      chainKey: token.chainKey,
      address: token.address,
      decimals: token.decimals,
      symbol: token.symbol,
      name: token.name || token.symbol,
      isBridgeable: Boolean(token.isBridgeable),
      priceUsd:
        typeof token.price?.usd === 'number' && Number.isFinite(token.price.usd)
          ? token.price.usd
          : null,
    };
  }

  private async resolveBridgeToken(
    chainKey: string,
    tokenIdentifier: string,
    options?: { requireBridgeable?: boolean },
  ): Promise<BridgeTokenInfo> {
    if (chainKey === STELLAR_CHAIN_KEY) {
      return this.resolveStellarBridgeToken(tokenIdentifier);
    }

    const token = await this.resolveStargateToken(chainKey, tokenIdentifier, options);
    return this.toBridgeTokenInfo(token);
  }

  private async getStellarBridgeQuote(
    dto: BridgeQuoteDto,
    srcChainKey: string,
    dstChainKey: string,
  ) {
    const srcAddress = this.normalizeBridgeParticipantAddress(dto.srcAddress, 'srcAddress');
    const dstAddress = dto.dstAddress
      ? this.normalizeBridgeParticipantAddress(dto.dstAddress, 'dstAddress')
      : srcAddress;

    const [srcToken, dstToken, chains] = await Promise.all([
      this.resolveBridgeToken(srcChainKey, dto.srcToken, {
        requireBridgeable: srcChainKey !== STELLAR_CHAIN_KEY,
      }),
      this.resolveBridgeToken(dstChainKey, dto.dstToken, {
        requireBridgeable: false,
      }),
      this.fetchStargateChains(),
    ]);

    const srcAmountBaseUnits = this.toBaseUnits(dto.srcAmount, srcToken.decimals, 'srcAmount');
    if (srcAmountBaseUnits <= 0n) {
      throw new BadRequestException('srcAmount must be greater than zero');
    }

    const slippageBps = dto.slippageBps ?? 150;
    const normalizedSrcAmount = this.normalizeDecimal(dto.srcAmount);
    const isUsdcTransfer =
      srcToken.symbol.trim().toUpperCase() === 'USDC' &&
      dstToken.symbol.trim().toUpperCase() === 'USDC';
    const isStellarEvmPair =
      (srcChainKey === STELLAR_CHAIN_KEY && dstChainKey !== STELLAR_CHAIN_KEY) ||
      (srcChainKey !== STELLAR_CHAIN_KEY && dstChainKey === STELLAR_CHAIN_KEY);
    const supportsCustomTemplateChainPair =
      this.circleCctpV2CustomTxBuilderService.supportsChainKey(srcChainKey) &&
      this.circleCctpV2CustomTxBuilderService.supportsChainKey(dstChainKey);
    const customTemplateModeEnabled =
      this.circleCctpV2CustomTxBuilderService.supportsCustomTemplateMode();
    const supportsCustomTemplateStellarNetwork =
      this.circleCctpV2CustomTxBuilderService.supportsCustomTemplateStellarNetwork();
    const supportsCustomTemplateRoute =
      isStellarEvmPair &&
      customTemplateModeEnabled &&
      supportsCustomTemplateChainPair &&
      supportsCustomTemplateStellarNetwork;
    const supportsInAppStellarRoute =
      isStellarEvmPair && (isUsdcTransfer || supportsCustomTemplateRoute);
    const cctpRouteSupportedForCurrentPair =
      !customTemplateModeEnabled ||
      (supportsCustomTemplateChainPair && supportsCustomTemplateStellarNetwork);

    if (customTemplateModeEnabled && !supportsCustomTemplateStellarNetwork) {
      throw new BadRequestException(
        'Configured Circle CCTP v2 custom-template provider supports Stellar mainnet only. Set STELLAR_NETWORK=mainnet and use a funded mainnet Stellar account, or switch STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_BUILDER_MODE away from custom-template for testnet.',
      );
    }

    if (!this.allowExternalBridgeRoutes && !supportsInAppStellarRoute) {
      if (isStellarEvmPair && !isUsdcTransfer && !supportsCustomTemplateChainPair) {
        throw new BadRequestException(
          `In-app Stellar bridge is not available for this chain pair with the configured custom-template provider (${srcChainKey} -> ${dstChainKey}).`,
        );
      }
      throw new BadRequestException(
        'In-app Stellar bridge currently supports USDC between Stellar and EVM chains only unless custom-template mode is enabled for this chain pair.',
      );
    }

    const requestedDstMin = dto.dstAmountMin
      ? this.toBaseUnits(dto.dstAmountMin, dstToken.decimals, 'dstAmountMin')
      : null;

    const routeParams = {
      srcChainKey,
      dstChainKey,
      srcToken: srcToken.address,
      dstToken: dstToken.address,
      srcAmount: normalizedSrcAmount,
      srcAddress,
      dstAddress,
    };
    const routeCandidates: Array<{
      providerName: string;
      routeKey: string;
      feeBps: number;
      estimatedDurationSeconds: number;
      externalUrl: string | null;
      enabled: boolean;
    }> = [];

    if (supportsInAppStellarRoute && cctpRouteSupportedForCurrentPair) {
      routeCandidates.push({
        providerName: this.stellarCircleCctpV2ProviderName,
        routeKey: 'cctp-v2',
        feeBps: this.stellarCircleCctpV2FeeBps,
        estimatedDurationSeconds: 420,
        externalUrl: null,
        enabled: this.isStellarCircleCctpV2InAppConfigured(),
      });
    }
    if (this.allowExternalBridgeRoutes) {
      const legacyExternalUrl = this.buildStellarBridgeExternalUrl(routeParams);
      routeCandidates.push({
        providerName: this.stellarBridgeProviderName,
        routeKey: 'stellar',
        feeBps: this.stellarBridgeFeeBps,
        estimatedDurationSeconds: 600,
        externalUrl: legacyExternalUrl,
        enabled: Boolean(legacyExternalUrl),
      });
    }

    const quotes: NormalizedBridgeQuote[] = routeCandidates
      .filter((candidate) => candidate.enabled)
      .map((candidate) => {
        const feeBaseUnits = (srcAmountBaseUnits * BigInt(candidate.feeBps)) / 10000n;
        const netSrcAmountBaseUnitsRaw = srcAmountBaseUnits - feeBaseUnits;
        const netSrcAmountBaseUnits = netSrcAmountBaseUnitsRaw > 0n ? netSrcAmountBaseUnitsRaw : 1n;
        const dstAmountBaseUnits = this.rescaleBaseUnits(
          netSrcAmountBaseUnits,
          srcToken.decimals,
          dstToken.decimals,
        );
        const dstAmountMinBaseUnits =
          requestedDstMin || this.applySlippage(dstAmountBaseUnits, slippageBps);

        return {
          route: `${candidate.providerName}/${candidate.routeKey}`,
          executionType: 'external' as const,
          externalUrl: candidate.externalUrl,
          srcChainKey,
          dstChainKey,
          srcChainId:
            srcChainKey === STELLAR_CHAIN_KEY ? null : this.chainIdByKey(chains, srcChainKey),
          dstChainId:
            dstChainKey === STELLAR_CHAIN_KEY ? null : this.chainIdByKey(chains, dstChainKey),
          srcAmount: this.fromBaseUnits(srcAmountBaseUnits, srcToken.decimals),
          srcAmountBaseUnits: srcAmountBaseUnits.toString(),
          dstAmount: this.fromBaseUnits(dstAmountBaseUnits, dstToken.decimals),
          dstAmountBaseUnits: dstAmountBaseUnits.toString(),
          dstAmountMin: this.fromBaseUnits(dstAmountMinBaseUnits, dstToken.decimals),
          dstAmountMinBaseUnits: dstAmountMinBaseUnits.toString(),
          estimatedDurationSeconds: candidate.estimatedDurationSeconds,
          fees:
            feeBaseUnits > 0n
              ? [
                  {
                    type: 'provider',
                    chainKey: srcChainKey,
                    tokenAddress: srcToken.address,
                    tokenSymbol: srcToken.symbol,
                    amount: this.fromBaseUnits(feeBaseUnits, srcToken.decimals),
                    amountBaseUnits: feeBaseUnits.toString(),
                  },
                ]
              : [],
          transactions: {
            approve: null,
            bridge: null,
          },
          error: null,
        };
      })
      .filter((quote) => {
        if (!requestedDstMin) {
          return true;
        }
        return this.parseBigIntOrZero(quote.dstAmountBaseUnits) >= requestedDstMin;
      });

    if (quotes.length === 0) {
      if (requestedDstMin) {
        throw new BadRequestException(
          'Requested dstAmountMin is higher than estimated deliverable amount for Stellar route',
        );
      }
      throw new BadRequestException(
        this.allowExternalBridgeRoutes
          ? 'No Stellar bridge route is configured. Set STELLAR_CIRCLE_CCTP_V2_TX_API_URL (or STELLAR_CIRCLE_CCTP_V2_BASE_URL for compatibility) for in-app CCTP, or STELLAR_BRIDGE_BASE_URL for legacy external routes.'
          : 'No in-app Stellar bridge route is configured. Set STELLAR_CIRCLE_CCTP_V2_TX_API_URL (or STELLAR_CIRCLE_CCTP_V2_BASE_URL for compatibility).',
      );
    }

    const recommendedQuote =
      quotes.find((quote) =>
        quote.route
          .toLowerCase()
          .startsWith(`${this.stellarCircleCctpV2ProviderName.trim().toLowerCase()}/`),
      ) || quotes[0];
    const recommendedDstAmountMinBaseUnits = this.parseBigIntOrZero(
      recommendedQuote.dstAmountMinBaseUnits,
    );

    return {
      srcChainKey,
      dstChainKey,
      srcAddress,
      dstAddress,
      srcToken: {
        address: srcToken.address,
        symbol: srcToken.symbol,
        decimals: srcToken.decimals,
      },
      dstToken: {
        address: dstToken.address,
        symbol: dstToken.symbol,
        decimals: dstToken.decimals,
      },
      srcAmount: normalizedSrcAmount,
      srcAmountBaseUnits: srcAmountBaseUnits.toString(),
      dstAmountMin: this.fromBaseUnits(recommendedDstAmountMinBaseUnits, dstToken.decimals),
      dstAmountMinBaseUnits: recommendedDstAmountMinBaseUnits.toString(),
      slippageBps,
      routeCount: quotes.length,
      recommendedRoute: recommendedQuote.route,
      quotes,
    };
  }

  private async buildStellarBridgeTransaction(
    dto: BuildBridgeTransactionDto,
    srcChainKey: string,
    dstChainKey: string,
    userId?: string,
  ) {
    const quoteResponse = await this.getStellarBridgeQuote(dto, srcChainKey, dstChainKey);
    const selectedQuote = this.selectPreferredBridgeQuote(quoteResponse.quotes, dto.route);

    if (!selectedQuote) {
      throw new BadRequestException('No valid Stellar bridge route is available');
    }

    if (selectedQuote.srcChainKey === STELLAR_CHAIN_KEY) {
      const sourceAmountBaseUnits = this.parseBigIntOrZero(selectedQuote.srcAmountBaseUnits);
      if (sourceAmountBaseUnits <= 0n) {
        throw new BadRequestException(
          'Invalid source amount returned for Stellar bridge route',
        );
      }

      await this.assertStellarBridgeSourceCanSendAsset({
        sourcePublicKey: quoteResponse.srcAddress,
        assetIdentifier: quoteResponse.srcToken.address,
        amountBaseUnits: sourceAmountBaseUnits,
        assetDecimals: quoteResponse.srcToken.decimals,
      });
    }

    const isCctpRoute = this.isCircleCctpV2Route(selectedQuote.route);
    let cctpInAppBuild: CircleCctpV2InAppBuildPayload | null = null;
    if (isCctpRoute) {
      try {
        cctpInAppBuild = await this.buildCircleCctpV2InAppBridgePayload({
          route: selectedQuote.route,
          srcChainKey: selectedQuote.srcChainKey,
          dstChainKey: selectedQuote.dstChainKey,
          srcToken: quoteResponse.srcToken.address,
          dstToken: quoteResponse.dstToken.address,
          srcAmount: selectedQuote.srcAmount,
          srcAmountBaseUnits: selectedQuote.srcAmountBaseUnits,
          dstAmountMin: selectedQuote.dstAmountMin,
          dstAmountMinBaseUnits: selectedQuote.dstAmountMinBaseUnits,
          srcAddress: quoteResponse.srcAddress,
          dstAddress: quoteResponse.dstAddress,
          slippageBps: quoteResponse.slippageBps,
        });
      } catch (error: unknown) {
        if (!this.allowExternalBridgeRoutes) {
          throw error;
        }

        const fallbackExternalUrl = this.buildStellarBridgeExternalUrl({
          srcChainKey: selectedQuote.srcChainKey,
          dstChainKey: selectedQuote.dstChainKey,
          srcToken: quoteResponse.srcToken.address,
          dstToken: quoteResponse.dstToken.address,
          srcAmount: selectedQuote.srcAmount,
          srcAddress: quoteResponse.srcAddress,
          dstAddress: quoteResponse.dstAddress,
        });
        if (!fallbackExternalUrl) {
          throw error;
        }

        this.logger.warn(
          `Circle CCTP v2 in-app payload unavailable (${this.errorMessage(error)}). Falling back to external Stellar bridge URL.`,
        );
        cctpInAppBuild = {
          executionType: 'external',
          externalUrl: fallbackExternalUrl,
          approvalTransaction: null,
          bridgeTransaction: null,
        };
      }
    }

    if (!isCctpRoute && !this.allowExternalBridgeRoutes) {
      throw new BadRequestException(
        'External Stellar bridge routes are disabled. Configure in-app CCTP transaction payloads.',
      );
    }

    if (!isCctpRoute && !selectedQuote.externalUrl) {
      throw new BadRequestException(
        'Selected Stellar route is missing provider URL. Set STELLAR_BRIDGE_BASE_URL.',
      );
    }

    let platformFeeQuote = this.platformFeeService.buildQuote({
      action: 'bridge',
      chainKey: selectedQuote.srcChainKey,
      payerAddress: quoteResponse.srcAddress,
      asset: quoteResponse.srcToken.address,
      assetSymbol: quoteResponse.srcToken.symbol,
      amount: selectedQuote.srcAmount,
      decimals: quoteResponse.srcToken.decimals,
    });
    if (platformFeeQuote && selectedQuote.srcChainKey === STELLAR_CHAIN_KEY) {
      const feeAsset = this.parseAsset(quoteResponse.srcToken.address);
      const collectorCanReceive = await this.canStellarAccountReceiveAsset(
        platformFeeQuote.collectorAddress,
        feeAsset,
      );
      if (!collectorCanReceive) {
        this.logger.warn(
          `Skipping bridge Stellar platform fee: collector ${platformFeeQuote.collectorAddress} cannot receive ${quoteResponse.srcToken.address}.`,
        );
        platformFeeQuote = null;
      }
    }
    const platformFeeTransaction =
      platformFeeQuote && selectedQuote.srcChainKey !== STELLAR_CHAIN_KEY
        ? this.buildEvmPlatformFeeTransferTransaction(
            platformFeeQuote,
            quoteResponse.srcToken.address,
            dto.srcAddress,
          )
        : null;
    const platformFeePayment =
      platformFeeQuote && selectedQuote.srcChainKey === STELLAR_CHAIN_KEY
        ? await this.buildStellarPlatformFeePaymentPayload({
            payerAddress: quoteResponse.srcAddress,
            collectorAddress: platformFeeQuote.collectorAddress,
            asset: quoteResponse.srcToken.address,
            amount: platformFeeQuote.feeAmount,
          })
        : null;
    const platformFeeReference = platformFeeQuote
      ? this.platformFeeService.buildReference([
          'bridge',
          userId || '',
          selectedQuote.srcChainKey,
          quoteResponse.srcAddress,
          quoteResponse.dstAddress,
          selectedQuote.route,
          selectedQuote.srcAmountBaseUnits,
          platformFeePayment ? platformFeePayment.xdr : '',
          platformFeeTransaction ? JSON.stringify(platformFeeTransaction) : '',
        ])
      : undefined;
    const platformFeeRecord =
      userId && platformFeeQuote
        ? await this.platformFeeService.accrueFee({
            userId,
            quote: platformFeeQuote,
            reference: platformFeeReference,
            metadata: {
              route: selectedQuote.route,
              srcToken: quoteResponse.srcToken.address,
              dstToken: quoteResponse.dstToken.address,
              executionType: cctpInAppBuild?.executionType || selectedQuote.executionType,
            },
          })
        : null;
    const platformFeePayload = platformFeeQuote
      ? {
          action: platformFeeQuote.action,
          feeBps: platformFeeQuote.feeBps,
          amount: platformFeeQuote.feeAmount,
          asset: platformFeeQuote.asset,
          assetSymbol: platformFeeQuote.assetSymbol,
          collectorAddress: platformFeeQuote.collectorAddress,
          ...(platformFeeRecord ? { feeRecordId: platformFeeRecord.id } : {}),
        }
      : null;

    return {
      route: selectedQuote.route,
      srcChainKey: selectedQuote.srcChainKey,
      dstChainKey: selectedQuote.dstChainKey,
      srcChainId: selectedQuote.srcChainId,
      dstChainId: selectedQuote.dstChainId,
      srcAmount: selectedQuote.srcAmount,
      srcAmountBaseUnits: selectedQuote.srcAmountBaseUnits,
      dstAmount: selectedQuote.dstAmount,
      dstAmountBaseUnits: selectedQuote.dstAmountBaseUnits,
      dstAmountMin: selectedQuote.dstAmountMin,
      dstAmountMinBaseUnits: selectedQuote.dstAmountMinBaseUnits,
      estimatedDurationSeconds: selectedQuote.estimatedDurationSeconds,
      fees: selectedQuote.fees,
      approvalTransaction: cctpInAppBuild?.approvalTransaction || null,
      bridgeTransaction: cctpInAppBuild?.bridgeTransaction || null,
      executionType: cctpInAppBuild?.executionType || 'external',
      externalUrl: cctpInAppBuild?.externalUrl || selectedQuote.externalUrl,
      ...(cctpInAppBuild?.bridgeStellarTransaction
        ? { bridgeStellarTransaction: cctpInAppBuild.bridgeStellarTransaction }
        : {}),
      ...(platformFeePayload ? { platformFee: platformFeePayload } : {}),
      ...(platformFeePayment ? { platformFeePayment } : {}),
      ...(platformFeeTransaction ? { platformFeeTransaction } : {}),
    };
  }

  async buildInternalCircleCctpV2BridgeTransactionPayload(
    payload: Record<string, unknown>,
    apiKey?: string,
  ): Promise<Record<string, unknown>> {
    this.assertStellarCircleCctpV2ApiKey(apiKey);
    const { request, srcChainKey, dstChainKey, srcToken, dstToken } =
      await this.resolveCircleCctpV2TxBuilderContext(payload);

    const upstreamEndpoint = this.resolveInternalCircleCctpV2UpstreamEndpoint();
    if (upstreamEndpoint) {
      const upstreamHeaders: Record<string, string> = {};
      if (this.stellarCircleCctpV2UpstreamApiKey && this.stellarCircleCctpV2UpstreamApiKey.trim()) {
        upstreamHeaders['x-api-key'] = this.stellarCircleCctpV2UpstreamApiKey.trim();
      }

      try {
        const response = await axios.post(upstreamEndpoint, request, {
          timeout: this.stellarCircleCctpV2ApiTimeoutMs,
          ...(Object.keys(upstreamHeaders).length > 0 ? { headers: upstreamHeaders } : {}),
        });
        if (!response.data || typeof response.data !== 'object') {
          throw new BadRequestException(
            'Upstream Circle CCTP v2 tx-builder response must be an object payload',
          );
        }
        return response.data as Record<string, unknown>;
      } catch (error: unknown) {
        throw new BadRequestException(
          `Upstream Circle CCTP v2 tx-builder request failed: ${this.errorMessage(error)}`,
        );
      }
    }

    const customPayload =
      await this.circleCctpV2CustomTxBuilderService.buildTransactionPayload({
        request,
        context: {
          srcChainKey,
          dstChainKey,
          srcToken: {
            address: srcToken.address,
            symbol: srcToken.symbol,
            decimals: srcToken.decimals,
          },
          dstToken: {
            address: dstToken.address,
            symbol: dstToken.symbol,
            decimals: dstToken.decimals,
          },
        },
      });
    if (customPayload) {
      return customPayload;
    }

    if (this.allowExternalBridgeRoutes) {
      const fallbackExternalUrl = this.buildStellarBridgeExternalUrl({
        srcChainKey,
        dstChainKey,
        srcToken: srcToken.address,
        dstToken: dstToken.address,
        srcAmount: request.srcAmount,
        srcAddress: request.srcAddress,
        dstAddress: request.dstAddress,
      });
      if (fallbackExternalUrl) {
        return {
          externalUrl: fallbackExternalUrl,
        };
      }
    }

    throw new BadRequestException(
      'Local Circle CCTP v2 tx-builder endpoint is wired, but no upstream builder is configured. Set STELLAR_CIRCLE_CCTP_V2_UPSTREAM_TX_API_URL (or STELLAR_CIRCLE_CCTP_V2_BASE_URL) to a service that returns bridgeTransaction or bridgeStellarTransaction, or enable STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_BUILDER_ENABLED.',
    );
  }

  async buildInternalCircleCctpV2CustomBridgeTransactionPayload(
    payload: Record<string, unknown>,
    apiKey?: string,
  ): Promise<Record<string, unknown>> {
    this.circleCctpV2CustomTxBuilderService.assertApiKey(apiKey);

    const { request, srcChainKey, dstChainKey, srcToken, dstToken } =
      await this.resolveCircleCctpV2TxBuilderContext(payload);
    const customPayload =
      await this.circleCctpV2CustomTxBuilderService.buildTransactionPayload({
        request,
        context: {
          srcChainKey,
          dstChainKey,
          srcToken: {
            address: srcToken.address,
            symbol: srcToken.symbol,
            decimals: srcToken.decimals,
          },
          dstToken: {
            address: dstToken.address,
            symbol: dstToken.symbol,
            decimals: dstToken.decimals,
          },
        },
      });
    if (!customPayload) {
      throw new BadRequestException(
        'Custom Circle CCTP v2 tx-builder is disabled. Set STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_BUILDER_ENABLED=true and choose STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_BUILDER_MODE.',
      );
    }

    return customPayload;
  }

  private async resolveCircleCctpV2TxBuilderContext(payload: Record<string, unknown>): Promise<{
    request: CircleCctpV2TxBuilderRequest;
    srcChainKey: string;
    dstChainKey: string;
    srcToken: BridgeTokenInfo;
    dstToken: BridgeTokenInfo;
  }> {
    const request = this.parseCircleCctpV2TxBuilderRequestPayload(payload);
    if (!this.isCircleCctpV2Route(request.route)) {
      throw new BadRequestException(
        'Unsupported route for Circle CCTP v2 tx-builder endpoint',
      );
    }

    const srcChainKey =
      this.normalizeChainKeyHint(request.srcChainKey) || request.srcChainKey.trim().toLowerCase();
    const dstChainKey =
      this.normalizeChainKeyHint(request.dstChainKey) || request.dstChainKey.trim().toLowerCase();

    const [srcToken, dstToken] = await Promise.all([
      this.resolveBridgeToken(srcChainKey, request.srcToken),
      this.resolveBridgeToken(dstChainKey, request.dstToken),
    ]);

    const isUsdcTransfer =
      srcToken.symbol.trim().toUpperCase() === 'USDC' &&
      dstToken.symbol.trim().toUpperCase() === 'USDC';
    const isStellarEvmPair =
      (srcChainKey === STELLAR_CHAIN_KEY && dstChainKey !== STELLAR_CHAIN_KEY) ||
      (srcChainKey !== STELLAR_CHAIN_KEY && dstChainKey === STELLAR_CHAIN_KEY);
    const customTemplateModeEnabled =
      this.circleCctpV2CustomTxBuilderService.supportsCustomTemplateMode();
    const supportsCustomTemplateChainPair =
      this.circleCctpV2CustomTxBuilderService.supportsChainKey(srcChainKey) &&
      this.circleCctpV2CustomTxBuilderService.supportsChainKey(dstChainKey);
    const supportsCustomTemplateStellarNetwork =
      this.circleCctpV2CustomTxBuilderService.supportsCustomTemplateStellarNetwork();
    const supportsCustomTemplateRoute =
      customTemplateModeEnabled &&
      supportsCustomTemplateChainPair &&
      supportsCustomTemplateStellarNetwork;

    if (!isStellarEvmPair) {
      throw new BadRequestException(
        'In-app Stellar bridge tx-builder currently supports Stellar <-> EVM routes only.',
      );
    }

    if (customTemplateModeEnabled && !supportsCustomTemplateChainPair) {
      throw new BadRequestException(
        `In-app Stellar bridge tx-builder is not available for this chain pair with the configured custom-template provider (${srcChainKey} -> ${dstChainKey}).`,
      );
    }

    if (customTemplateModeEnabled && !supportsCustomTemplateStellarNetwork) {
      throw new BadRequestException(
        'Configured Circle CCTP v2 custom-template provider supports Stellar mainnet only. Set STELLAR_NETWORK=mainnet and use a funded mainnet Stellar account, or switch STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_BUILDER_MODE away from custom-template for testnet.',
      );
    }

    if (!isUsdcTransfer && !supportsCustomTemplateRoute) {
      throw new BadRequestException(
        'In-app Stellar bridge tx-builder currently supports USDC between Stellar and EVM chains only unless custom-template mode is enabled.',
      );
    }

    return {
      request,
      srcChainKey,
      dstChainKey,
      srcToken,
      dstToken,
    };
  }

  private async buildCircleCctpV2InAppBridgePayload(
    params: CircleCctpV2TxBuilderRequest,
  ): Promise<CircleCctpV2InAppBuildPayload> {
    const endpoint = this.resolveStellarCircleCctpV2TxApiEndpoint();
    const headers: Record<string, string> = {};
    if (this.stellarCircleCctpV2ApiKey && this.stellarCircleCctpV2ApiKey.trim()) {
      headers['x-api-key'] = this.stellarCircleCctpV2ApiKey.trim();
    }

    let responseData: unknown;
    try {
      const response = await axios.post(
        endpoint,
        {
          route: params.route,
          srcChainKey: params.srcChainKey,
          dstChainKey: params.dstChainKey,
          srcToken: params.srcToken,
          dstToken: params.dstToken,
          srcAmount: params.srcAmount,
          srcAmountBaseUnits: params.srcAmountBaseUnits,
          dstAmountMin: params.dstAmountMin,
          dstAmountMinBaseUnits: params.dstAmountMinBaseUnits,
          srcAddress: params.srcAddress,
          dstAddress: params.dstAddress,
          slippageBps: params.slippageBps,
        },
        {
          timeout: this.stellarCircleCctpV2ApiTimeoutMs,
          ...(Object.keys(headers).length > 0 ? { headers } : {}),
        },
      );
      responseData = response.data;
    } catch (error: unknown) {
      throw new BadRequestException(
        `Failed to build Circle CCTP v2 in-app transaction payload: ${this.errorMessage(error)}`,
      );
    }

    if (!responseData || typeof responseData !== 'object') {
      throw new BadRequestException(
        'Circle CCTP v2 transaction API response is invalid (expected an object payload)',
      );
    }

    const payloadResponse = responseData as Record<string, unknown>;
    const externalUrlCandidate =
      typeof payloadResponse.externalUrl === 'string' ? payloadResponse.externalUrl.trim() : '';
    if (externalUrlCandidate) {
      if (this.allowExternalBridgeRoutes) {
        return {
          executionType: 'external',
          externalUrl: externalUrlCandidate,
          approvalTransaction: null,
          bridgeTransaction: null,
        };
      }
      throw new BadRequestException(
        'Circle CCTP v2 API returned an externalUrl. In-app mode requires executable transaction payloads.',
      );
    }

    const stellarTransaction =
      this.parseCircleCctpV2StellarTransactionPayload(payloadResponse.bridgeStellarTransaction) ||
      this.parseCircleCctpV2StellarTransactionPayload(payloadResponse.stellarTransaction) ||
      this.parseCircleCctpV2StellarTransactionPayload(
        typeof payloadResponse.bridgeTransactionXdr === 'string'
          ? { xdr: payloadResponse.bridgeTransactionXdr, network: payloadResponse.network }
          : payloadResponse.bridgeTransactionXdr,
      ) ||
      this.parseCircleCctpV2StellarTransactionPayload(payloadResponse.bridgeTransaction) ||
      this.parseCircleCctpV2StellarTransactionPayload(payloadResponse);
    if (stellarTransaction) {
      if (params.srcChainKey.trim().toLowerCase() === STELLAR_CHAIN_KEY) {
        this.assertCircleCctpV2StellarTransactionSourceMatchesRequest(
          stellarTransaction,
          params.srcAddress,
        );
        await this.assertCircleCctpV2StellarPaymentDestinationsCanReceiveAssets(
          stellarTransaction,
        );
      }
      return {
        executionType: 'external',
        externalUrl: null,
        approvalTransaction: null,
        bridgeTransaction: null,
        bridgeStellarTransaction: stellarTransaction,
      };
    }

    const bridgeTransaction = this.parseCircleCctpV2EvmTransactionRequest(
      payloadResponse.bridgeTransaction,
      'bridgeTransaction',
    );
    const approvalTransaction = this.parseCircleCctpV2EvmTransactionRequest(
      payloadResponse.approvalTransaction,
      'approvalTransaction',
    );
    if (!bridgeTransaction) {
      throw new BadRequestException(
        'Circle CCTP v2 API response is missing bridgeTransaction (EVM) or bridgeStellarTransaction (XDR)',
      );
    }

    return {
      executionType: 'evm',
      externalUrl: null,
      approvalTransaction,
      bridgeTransaction,
    };
  }

  private parseCircleCctpV2TxBuilderRequestPayload(
    payload: Record<string, unknown>,
  ): CircleCctpV2TxBuilderRequest {
    const route = this.resolveCircleCctpV2TxBuilderStringField(payload, 'route');
    const srcChainKey = this.resolveCircleCctpV2TxBuilderStringField(payload, 'srcChainKey');
    const dstChainKey = this.resolveCircleCctpV2TxBuilderStringField(payload, 'dstChainKey');
    const srcToken = this.resolveCircleCctpV2TxBuilderStringField(payload, 'srcToken');
    const dstToken = this.resolveCircleCctpV2TxBuilderStringField(payload, 'dstToken');
    const srcAmount = this.normalizeDecimal(
      this.resolveCircleCctpV2TxBuilderStringField(payload, 'srcAmount'),
    );
    const srcAmountBaseUnits = this.resolveCircleCctpV2TxBuilderStringField(
      payload,
      'srcAmountBaseUnits',
    );
    const dstAmountMin = this.normalizeDecimal(
      this.resolveCircleCctpV2TxBuilderStringField(payload, 'dstAmountMin'),
    );
    const dstAmountMinBaseUnits = this.resolveCircleCctpV2TxBuilderStringField(
      payload,
      'dstAmountMinBaseUnits',
    );
    const srcAddress = this.normalizeBridgeParticipantAddress(
      this.resolveCircleCctpV2TxBuilderStringField(payload, 'srcAddress'),
      'srcAddress',
    );
    const dstAddress = this.normalizeBridgeParticipantAddress(
      this.resolveCircleCctpV2TxBuilderStringField(payload, 'dstAddress'),
      'dstAddress',
    );

    const slippageRaw = payload.slippageBps;
    let slippageBps = 150;
    if (slippageRaw !== null && slippageRaw !== undefined && slippageRaw !== '') {
      const parsed =
        typeof slippageRaw === 'number'
          ? slippageRaw
          : Number.parseInt(String(slippageRaw).trim(), 10);
      if (!Number.isFinite(parsed) || parsed < 0 || parsed > 5000) {
        throw new BadRequestException('slippageBps must be an integer between 0 and 5000');
      }
      slippageBps = Math.floor(parsed);
    }

    return {
      route,
      srcChainKey,
      dstChainKey,
      srcToken,
      dstToken,
      srcAmount,
      srcAmountBaseUnits,
      dstAmountMin,
      dstAmountMinBaseUnits,
      srcAddress,
      dstAddress,
      slippageBps,
    };
  }

  private resolveCircleCctpV2TxBuilderStringField(
    payload: Record<string, unknown>,
    fieldName: string,
  ): string {
    const value = payload[fieldName];
    if (typeof value !== 'string' || !value.trim()) {
      throw new BadRequestException(`Circle CCTP v2 tx-builder field "${fieldName}" is required`);
    }
    return value.trim();
  }

  private assertStellarCircleCctpV2ApiKey(apiKey?: string): void {
    const configuredKey = this.stellarCircleCctpV2ApiKey?.trim();
    if (!configuredKey) {
      return;
    }

    if (!apiKey || apiKey.trim() !== configuredKey) {
      throw new ForbiddenException('Invalid x-api-key for Circle CCTP v2 tx-builder endpoint');
    }
  }

  private resolveInternalCircleCctpV2UpstreamEndpoint(): string | null {
    const currentTxEndpoint = this.normalizeHttpUrlCandidate(this.stellarCircleCctpV2TxApiUrl);
    const explicitUpstream = this.normalizeHttpUrlCandidate(this.stellarCircleCctpV2UpstreamTxApiUrl);
    if (explicitUpstream && explicitUpstream !== currentTxEndpoint) {
      return explicitUpstream;
    }

    // Compatibility fallback: if BASE_URL differs from local TX API URL, treat it as upstream.
    const legacyBaseUpstream = this.normalizeHttpUrlCandidate(this.stellarCircleCctpV2BaseUrl);
    if (legacyBaseUpstream && legacyBaseUpstream !== currentTxEndpoint) {
      return legacyBaseUpstream;
    }

    return null;
  }

  private normalizeHttpUrlCandidate(value: string | undefined): string | null {
    if (!value || !value.trim()) {
      return null;
    }
    try {
      const parsed = new URL(value.trim());
      if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
        return null;
      }
      return parsed.toString();
    } catch {
      return null;
    }
  }

  private parseCircleCctpV2EvmTransactionRequest(
    payload: unknown,
    fieldName: string,
  ): EvmTransactionRequest | null {
    if (!payload) {
      return null;
    }
    if (typeof payload !== 'object') {
      throw new BadRequestException(`Circle CCTP v2 ${fieldName} payload must be an object`);
    }

    const record = payload as Record<string, unknown>;
    const to =
      typeof record.to === 'string' ? this.normalizeAddressIfPossible(record.to.trim()) : null;
    const data = typeof record.data === 'string' ? record.data.trim() : '';
    const value = this.parseCircleCctpV2TransactionValue(record.value, `${fieldName}.value`);
    const from =
      typeof record.from === 'string' ? this.normalizeAddressIfPossible(record.from.trim()) : null;

    if (!to) {
      throw new BadRequestException(
        `Circle CCTP v2 ${fieldName}.to must be a valid EVM address`,
      );
    }
    if (!/^0x([a-fA-F0-9]{2})*$/.test(data)) {
      throw new BadRequestException(
        `Circle CCTP v2 ${fieldName}.data must be a valid hex calldata string`,
      );
    }

    return {
      to,
      data,
      value,
      ...(from ? { from } : {}),
    };
  }

  private parseCircleCctpV2TransactionValue(value: unknown, fieldName: string): string {
    if (value === null || value === undefined || value === '') {
      return '0';
    }
    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (!trimmed) {
        return '0';
      }
      try {
        const parsed = trimmed.toLowerCase().startsWith('0x') ? BigInt(trimmed) : BigInt(trimmed);
        if (parsed < 0n) {
          throw new Error('negative');
        }
        return parsed.toString();
      } catch {
        throw new BadRequestException(
          `Circle CCTP v2 ${fieldName} must be a non-negative integer string`,
        );
      }
    }
    if (typeof value === 'number' && Number.isFinite(value) && Number.isInteger(value) && value >= 0) {
      return BigInt(value).toString();
    }

    throw new BadRequestException(
      `Circle CCTP v2 ${fieldName} must be a non-negative integer`,
    );
  }

  private parseCircleCctpV2StellarTransactionPayload(payload: unknown): StellarTransactionPayload | null {
    if (typeof payload === 'string') {
      const xdr = payload.trim();
      if (!xdr) {
        return null;
      }
      const network = this.resolveRequestedStellarNetwork();
      try {
        StellarSdk.TransactionBuilder.fromXDR(xdr, this.getNetworkPassphrase(network));
      } catch {
        throw new BadRequestException(
          'Circle CCTP v2 bridgeStellarTransaction.xdr is invalid for the configured Stellar network',
        );
      }
      return { xdr, network };
    }
    if (!payload || typeof payload !== 'object') {
      return null;
    }

    const record = payload as Record<string, unknown>;
    const xdr = typeof record.xdr === 'string' ? record.xdr.trim() : '';
    if (!xdr) {
      return null;
    }

    const network = this.resolveRequestedStellarNetwork(
      typeof record.network === 'string' ? record.network : undefined,
    );

    try {
      StellarSdk.TransactionBuilder.fromXDR(xdr, this.getNetworkPassphrase(network));
    } catch {
      throw new BadRequestException(
        'Circle CCTP v2 bridgeStellarTransaction.xdr is invalid for the provided network',
      );
    }

    return { xdr, network };
  }

  private assertCircleCctpV2StellarTransactionSourceMatchesRequest(
    payload: StellarTransactionPayload,
    expectedSourceAddress: string,
  ): void {
    const expected = expectedSourceAddress.trim();
    if (!this.looksLikeStellarAddress(expected)) {
      throw new BadRequestException('srcAddress must be a valid Stellar address');
    }

    let transaction: StellarSdk.Transaction | StellarSdk.FeeBumpTransaction;
    try {
      transaction = StellarSdk.TransactionBuilder.fromXDR(
        payload.xdr,
        this.getNetworkPassphrase(payload.network),
      );
    } catch {
      throw new BadRequestException(
        'Circle CCTP v2 bridgeStellarTransaction.xdr is invalid for the provided network',
      );
    }

    const txSourceCandidate =
      'source' in transaction
        ? transaction.source
        : (transaction as unknown as { feeSource?: unknown }).feeSource;
    const txSource = this.extractStellarSourceAddress(txSourceCandidate);
    if (!txSource) {
      throw new BadRequestException(
        'Circle CCTP v2 bridgeStellarTransaction is missing a valid transaction source account',
      );
    }

    if (txSource !== expected) {
      throw new BadRequestException(
        `Circle CCTP v2 bridgeStellarTransaction source (${txSource}) does not match requested srcAddress (${expected}). Reconnect the correct Stellar account and retry.`,
      );
    }

    const operations = Array.isArray((transaction as unknown as { operations?: unknown }).operations)
      ? ((transaction as unknown as { operations?: unknown[] }).operations as Array<Record<string, unknown>>)
      : [];

    for (let index = 0; index < operations.length; index += 1) {
      const operation = operations[index];
      const opSource = this.extractStellarSourceAddress(operation.source);
      if (opSource && opSource !== expected) {
        const opType =
          typeof operation.type === 'string' && operation.type.trim()
            ? operation.type.trim()
            : 'unknown';
        throw new BadRequestException(
          `Circle CCTP v2 bridgeStellarTransaction operation source mismatch at index ${index} (${opType}): ${opSource} != ${expected}. Reconnect the correct Stellar account and retry.`,
        );
      }
    }
  }

  private async assertCircleCctpV2StellarPaymentDestinationsCanReceiveAssets(
    payload: StellarTransactionPayload,
  ): Promise<void> {
    let transaction: StellarSdk.Transaction | StellarSdk.FeeBumpTransaction;
    try {
      transaction = StellarSdk.TransactionBuilder.fromXDR(
        payload.xdr,
        this.getNetworkPassphrase(payload.network),
      );
    } catch {
      throw new BadRequestException(
        'Circle CCTP v2 bridgeStellarTransaction.xdr is invalid for the provided network',
      );
    }

    const operations = Array.isArray((transaction as unknown as { operations?: unknown }).operations)
      ? ((transaction as unknown as { operations?: unknown[] }).operations as Array<Record<string, unknown>>)
      : [];

    for (let index = 0; index < operations.length; index += 1) {
      const operation = operations[index];
      const opType = typeof operation.type === 'string' ? operation.type.trim().toLowerCase() : '';
      if (opType !== 'payment') {
        continue;
      }

      const destination =
        typeof operation.destination === 'string' ? operation.destination.trim() : '';
      if (!this.looksLikeStellarAddress(destination)) {
        throw new BadRequestException(
          `Circle CCTP v2 payment operation at index ${index} is missing a valid destination account`,
        );
      }

      const asset = this.parseStellarOperationAsset(operation.asset);
      if (!asset || asset.isNative()) {
        continue;
      }

      const canReceive = await this.canStellarAccountReceiveAsset(
        destination,
        asset,
        payload.network,
      );
      if (!canReceive) {
        const assetLabel = this.toStellarAssetLabel(asset);
        throw new BadRequestException(
          `Circle CCTP v2 payment destination ${destination} has no trustline for ${assetLabel}. Provider returned a non-receivable bridge transaction payload.`,
        );
      }
    }
  }

  private parseStellarOperationAsset(assetCandidate: unknown): StellarSdk.Asset | null {
    if (!assetCandidate || typeof assetCandidate !== 'object') {
      return null;
    }

    const candidate = assetCandidate as {
      isNative?: () => boolean;
      getCode?: () => string;
      getIssuer?: () => string;
    };
    if (typeof candidate.isNative !== 'function') {
      return null;
    }

    try {
      if (candidate.isNative()) {
        return StellarSdk.Asset.native();
      }

      const code = typeof candidate.getCode === 'function' ? candidate.getCode().trim() : '';
      const issuer =
        typeof candidate.getIssuer === 'function' ? candidate.getIssuer().trim() : '';
      if (!code || !this.looksLikeStellarAddress(issuer)) {
        return null;
      }

      return new StellarSdk.Asset(code, issuer);
    } catch {
      return null;
    }
  }

  private extractStellarSourceAddress(source: unknown): string | null {
    if (typeof source === 'string') {
      const normalized = source.trim();
      return this.looksLikeStellarAddress(normalized) ? normalized : null;
    }

    if (!source || typeof source !== 'object') {
      return null;
    }

    const sourceRecord = source as {
      accountId?: () => string;
    };
    if (typeof sourceRecord.accountId === 'function') {
      try {
        const accountId = sourceRecord.accountId();
        const normalized = typeof accountId === 'string' ? accountId.trim() : '';
        return this.looksLikeStellarAddress(normalized) ? normalized : null;
      } catch {
        return null;
      }
    }

    return null;
  }

  private isCircleCctpV2Route(route: string): boolean {
    const providerSegment = route.split('/')[0]?.trim().toLowerCase();
    if (!providerSegment) {
      return false;
    }
    const configuredProvider = this.stellarCircleCctpV2ProviderName.trim().toLowerCase();
    return (
      providerSegment === configuredProvider ||
      providerSegment.includes('circle') ||
      providerSegment.includes('cctp')
    );
  }

  private isStellarCircleCctpV2InAppConfigured(): boolean {
    const candidate = this.stellarCircleCctpV2TxApiUrl?.trim();
    if (!candidate) {
      return false;
    }

    try {
      const parsed = new URL(candidate);
      if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
        return false;
      }

      // When TX API points to the local internal endpoint, require a separate upstream
      // builder URL so we don't advertise a route that can only self-loop and fail.
      const normalizedPath = parsed.pathname.replace(/\/+$/, '');
      if (
        normalizedPath.endsWith('/defi/bridge/internal/circle-cctp-v2/build-tx') &&
        !this.resolveInternalCircleCctpV2UpstreamEndpoint()
      ) {
        return false;
      }

      return true;
    } catch {
      return false;
    }
  }

  private resolveStellarCircleCctpV2TxApiEndpoint(): string {
    const candidate = this.stellarCircleCctpV2TxApiUrl?.trim();
    if (!candidate) {
      throw new BadRequestException(
        'Circle CCTP v2 in-app execution is not configured. Set STELLAR_CIRCLE_CCTP_V2_TX_API_URL (or STELLAR_CIRCLE_CCTP_V2_BASE_URL for compatibility).',
      );
    }

    try {
      const parsed = new URL(candidate);
      if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
        throw new Error('invalid protocol');
      }
      return parsed.toString();
    } catch {
      throw new BadRequestException(
        'STELLAR_CIRCLE_CCTP_V2_TX_API_URL (or STELLAR_CIRCLE_CCTP_V2_BASE_URL) must be a valid HTTP(S) URL',
      );
    }
  }

  private buildStellarBridgeExternalUrl(params: {
    srcChainKey: string;
    dstChainKey: string;
    srcToken: string;
    dstToken: string;
    srcAmount: string;
    srcAddress: string;
    dstAddress: string;
  }): string | null {
    return this.buildExternalBridgeUrl(this.stellarBridgeBaseUrl, 'STELLAR_BRIDGE_BASE_URL', {
      ...params,
      dstAmountMin: '0',
    });
  }

  private buildExternalBridgeUrl(
    baseUrl: string | undefined,
    envVarName: string,
    params: {
      srcChainKey: string;
      dstChainKey: string;
      srcToken: string;
      dstToken: string;
      srcAmount: string;
      dstAmountMin: string;
      srcAddress: string;
      dstAddress: string;
    },
  ): string | null {
    if (!baseUrl) {
      return null;
    }

    try {
      const url = new URL(baseUrl);
      url.searchParams.set('srcChainKey', params.srcChainKey);
      url.searchParams.set('dstChainKey', params.dstChainKey);
      url.searchParams.set('srcToken', params.srcToken);
      url.searchParams.set('dstToken', params.dstToken);
      url.searchParams.set('srcAmount', params.srcAmount);
      url.searchParams.set('dstAmountMin', params.dstAmountMin);
      url.searchParams.set('srcAddress', params.srcAddress);
      url.searchParams.set('dstAddress', params.dstAddress);
      return url.toString();
    } catch {
      throw new BadRequestException(`${envVarName} is not a valid URL`);
    }
  }

  private buildStargateExternalFallbackUrl(params: {
    srcChainKey: string;
    dstChainKey: string;
    srcToken: string;
    dstToken: string;
    srcAmount: string;
    dstAmountMin: string;
    srcAddress: string;
    dstAddress: string;
  }): string | null {
    if (!this.stargateExternalFallbackBaseUrl) {
      return null;
    }

    try {
      const url = new URL(this.stargateExternalFallbackBaseUrl);
      url.searchParams.set('srcChainKey', params.srcChainKey);
      url.searchParams.set('dstChainKey', params.dstChainKey);
      url.searchParams.set('srcToken', params.srcToken);
      url.searchParams.set('dstToken', params.dstToken);
      url.searchParams.set('srcAmount', params.srcAmount);
      url.searchParams.set('dstAmountMin', params.dstAmountMin);
      url.searchParams.set('srcAddress', params.srcAddress);
      url.searchParams.set('dstAddress', params.dstAddress);
      return url.toString();
    } catch {
      throw new BadRequestException('STARGATE_EXTERNAL_FALLBACK_BASE_URL is not a valid URL');
    }
  }

  async getBridgeHistory(userId: string, limit = 20) {
    const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 100) : 20;
    const records = await this.bridgeHistoryModel
      .find({ userId } as any)
      .sort({ createdAt: -1 })
      .limit(safeLimit)
      .lean()
      .exec();

    return records.map((record) => this.mapBridgeHistoryRecord(record));
  }

  async recordBridgeHistory(userId: string, dto: RecordBridgeHistoryDto) {
    const status = dto.status?.trim().toLowerCase() || 'submitted';
    const bridgeTxHash = this.normalizeBridgeReference(dto.bridgeTxHash, status, dto.srcChainKey);
    const approvalTxHash = dto.approvalTxHash
      ? this.normalizeTxHash(dto.approvalTxHash, 'approvalTxHash')
      : undefined;

    const payload: Partial<BridgeHistory> = {
      userId,
      provider: this.resolveBridgeHistoryProvider(dto.route, status, dto.metadata),
      srcChainKey: dto.srcChainKey.trim().toLowerCase(),
      dstChainKey: dto.dstChainKey.trim().toLowerCase(),
      srcAddress: this.normalizeBridgeParticipantAddress(dto.srcAddress, 'srcAddress'),
      dstAddress: this.normalizeBridgeParticipantAddress(dto.dstAddress, 'dstAddress'),
      srcTokenSymbol: dto.srcTokenSymbol.trim().toUpperCase(),
      dstTokenSymbol: dto.dstTokenSymbol.trim().toUpperCase(),
      srcAmount: this.normalizeDecimal(dto.srcAmount),
      ...(dto.dstAmount ? { dstAmount: this.normalizeDecimal(dto.dstAmount) } : {}),
      ...(dto.dstAmountMin ? { dstAmountMin: this.normalizeDecimal(dto.dstAmountMin) } : {}),
      ...(dto.route ? { route: dto.route.trim() } : {}),
      ...(approvalTxHash ? { approvalTxHash } : {}),
      bridgeTxHash,
      status,
      ...(typeof dto.estimatedDurationSeconds === 'number'
        ? { estimatedDurationSeconds: dto.estimatedDurationSeconds }
        : {}),
      ...(dto.feeAmount ? { feeAmount: this.normalizeDecimal(dto.feeAmount) } : {}),
      ...(dto.feeTokenSymbol ? { feeTokenSymbol: dto.feeTokenSymbol.trim().toUpperCase() } : {}),
      ...(dto.metadata ? { metadata: dto.metadata } : {}),
    };

    const record = await this.bridgeHistoryModel
      .findOneAndUpdate(
        { userId, bridgeTxHash } as any,
        {
          $set: payload,
        },
        { upsert: true, new: true },
      )
      .lean()
      .exec();

    if (!record) {
      throw new BadRequestException('Failed to record bridge history');
    }

    return this.mapBridgeHistoryRecord(record);
  }

  async refreshBridgeHistoryStatus(userId: string, bridgeTxHash: string) {
    const normalizedHash = this.normalizeTxHash(bridgeTxHash, 'bridgeTxHash');
    const record = await this.bridgeHistoryModel
      .findOne({ userId, bridgeTxHash: normalizedHash } as any)
      .exec();

    if (!record) {
      throw new NotFoundException('Bridge history record not found');
    }

    const rpcUrl = this.resolveRpcUrlForChain(record.srcChainKey);
    if (!rpcUrl) {
      throw new BadRequestException(
        `No RPC URL configured for chain "${record.srcChainKey}". ` +
          `Set STARGATE_EVM_RPC_URLS or STARGATE_RPC_URL_${this.chainEnvSuffix(record.srcChainKey)}.`,
      );
    }

    try {
      const provider = new ethers.JsonRpcProvider(rpcUrl);
      const receipt = await provider.getTransactionReceipt(normalizedHash);

      if (!receipt) {
        if (record.status !== 'pending' && record.status !== 'confirmed' && record.status !== 'failed') {
          record.status = 'pending';
          await record.save();
        }

        return {
          ...this.mapBridgeHistoryRecord(
            record.toObject() as unknown as Record<string, unknown>,
          ),
          onchain: {
            found: false,
          },
        };
      }

      const nextStatus = receipt.status === 1 ? 'confirmed' : 'failed';
      record.status = nextStatus;

      const metadata =
        record.metadata && typeof record.metadata === 'object'
          ? { ...(record.metadata as Record<string, unknown>) }
          : {};

      metadata.onchain = {
        ...(typeof metadata.onchain === 'object' && metadata.onchain !== null
          ? (metadata.onchain as Record<string, unknown>)
          : {}),
        blockNumber: receipt.blockNumber,
        blockHash: receipt.blockHash,
        transactionIndex: receipt.index,
        gasUsed: receipt.gasUsed ? receipt.gasUsed.toString() : undefined,
        gasPrice: receipt.gasPrice ? receipt.gasPrice.toString() : undefined,
        checkedAt: new Date().toISOString(),
      };

      record.metadata = metadata;
      await record.save();

      return {
        ...this.mapBridgeHistoryRecord(
          record.toObject() as unknown as Record<string, unknown>,
        ),
        onchain: {
          found: true,
          status: nextStatus,
        },
      };
    } catch (error: unknown) {
      throw new BadRequestException(
        `Unable to refresh bridge receipt on ${record.srcChainKey}: ${this.errorMessage(error)}`,
      );
    }
  }

  async getStrictSendQuote(dto: PathPaymentQuoteDto) {
    const sourceAsset = this.parseAsset(dto.sourceAsset);
    const destinationAsset = this.parseAsset(dto.destinationAsset);
    const limit = dto.limit ?? 25;
    const network = this.resolveRequestedStellarNetwork(dto.network);
    const server = this.getStellarServer(network);
    let response: Awaited<ReturnType<ReturnType<typeof server.strictSendPaths>['call']>>;
    try {
      response = await server
        .strictSendPaths(sourceAsset, dto.sourceAmount, [destinationAsset])
        .call();
    } catch (error: unknown) {
      const status = Number((error as { response?: { status?: unknown } })?.response?.status);
      if (status === 400 || status === 404) {
        return {
          sourceAsset: dto.sourceAsset,
          destinationAsset: dto.destinationAsset,
          sourceAmount: dto.sourceAmount,
          routes: [],
          routeCount: 0,
          network,
        };
      }
      throw error;
    }

    const routes = (response.records as StrictSendPathRecord[]).slice(0, limit).map((record) => {
      const sourceAmount = Number(record.source_amount);
      const destinationAmount = Number(record.destination_amount);

      return {
        sourceAmount: record.source_amount,
        destinationAmount: record.destination_amount,
        sourceAsset: this.assetToString(
          record.source_asset_type,
          record.source_asset_code,
          record.source_asset_issuer,
        ),
        destinationAsset: this.assetToString(
          record.destination_asset_type,
          record.destination_asset_code,
          record.destination_asset_issuer,
        ),
        effectiveRate:
          sourceAmount > 0 ? (destinationAmount / sourceAmount).toFixed(8) : '0',
        path: record.path.map((hop: StrictSendPathRecord['path'][number]) =>
          this.assetToString(hop.asset_type, hop.asset_code, hop.asset_issuer),
        ),
      };
    });

    return {
      sourceAsset: dto.sourceAsset,
      destinationAsset: dto.destinationAsset,
      sourceAmount: dto.sourceAmount,
      routes,
      routeCount: routes.length,
      network,
    };
  }

  async buildStrictSendPathPayment(dto: BuildPathPaymentDto, userId?: string) {
    const sourceAsset = this.parseAsset(dto.sourceAsset);
    const destinationAsset = this.parseAsset(dto.destinationAsset);
    const pathAssets = (dto.path ?? []).map((asset) => this.parseAsset(asset));
    const network = this.resolveRequestedStellarNetwork(dto.network);
    const server = this.getStellarServer(network);

    const account = await server.loadAccount(dto.sourcePublicKey);
    const fee = await this.feeEstimationService.estimateFee('stellar', 'path_payment_strict_send');

    const builder = new StellarSdk.TransactionBuilder(account, {
      fee,
      networkPassphrase: this.getNetworkPassphrase(network),
    });

    builder.addOperation(
      StellarSdk.Operation.pathPaymentStrictSend({
        sendAsset: sourceAsset,
        sendAmount: dto.sourceAmount,
        destination: dto.destinationPublicKey,
        destAsset: destinationAsset,
        destMin: dto.destMin,
        path: pathAssets,
      }),
    );

    const platformFeeQuote = this.platformFeeService.buildQuote({
      action: 'swap',
      chainKey: STELLAR_CHAIN_KEY,
      payerAddress: dto.sourcePublicKey,
      asset: dto.sourceAsset,
      assetSymbol: this.extractAssetSymbolFromAssetIdentifier(dto.sourceAsset),
      amount: dto.sourceAmount,
      decimals: 7,
    });

    if (platformFeeQuote) {
      builder.addOperation(
        StellarSdk.Operation.payment({
          destination: platformFeeQuote.collectorAddress,
          asset: sourceAsset,
          amount: platformFeeQuote.feeAmount,
        }),
      );
    }

    if (dto.memo) {
      builder.addMemo(StellarSdk.Memo.text(dto.memo.slice(0, 28)));
    }

    builder.setTimeout(180);
    const tx = builder.build();
    const reference = platformFeeQuote
      ? this.platformFeeService.buildReference([
          'swap',
          userId || '',
          dto.sourcePublicKey,
          dto.destinationPublicKey,
          tx.toXDR(),
        ])
      : undefined;
    const platformFeeRecord =
      userId && platformFeeQuote
        ? await this.platformFeeService.accrueFee({
            userId,
            quote: platformFeeQuote,
            reference,
            metadata: {
              destinationPublicKey: dto.destinationPublicKey,
              destinationAsset: dto.destinationAsset,
              destMin: dto.destMin,
              network,
            },
          })
        : null;

    return {
      xdr: tx.toXDR(),
      network,
      sourcePublicKey: dto.sourcePublicKey,
      destinationPublicKey: dto.destinationPublicKey,
      sourceAmount: dto.sourceAmount,
      destMin: dto.destMin,
      path: dto.path ?? [],
      fee,
      ...(platformFeeQuote
        ? {
            platformFee: {
              action: platformFeeQuote.action,
              feeBps: platformFeeQuote.feeBps,
              amount: platformFeeQuote.feeAmount,
              asset: platformFeeQuote.asset,
              assetSymbol: platformFeeQuote.assetSymbol,
              collectorAddress: platformFeeQuote.collectorAddress,
              ...(platformFeeRecord ? { feeRecordId: platformFeeRecord.id } : {}),
            },
          }
        : {}),
    };
  }

  private async fetchStargateChains(forceRefresh = false): Promise<StargateChain[]> {
    const now = Date.now();
    if (
      !forceRefresh &&
      this.stargateChainsCache &&
      now - this.stargateChainsCache.timestamp < this.stargateCacheTtlMs
    ) {
      return this.stargateChainsCache.data;
    }

    try {
      const response = await axios.get<{ chains?: StargateChain[] }>(
        `${this.stargateApiBaseUrl}/chains`,
        { timeout: 15000 },
      );
      const rawChains = Array.isArray(response.data?.chains) ? response.data.chains : [];
      const chains = rawChains
        .filter((chain) => chain && typeof chain.chainKey === 'string')
        .map((chain) => ({
          ...chain,
          chainKey: chain.chainKey.trim().toLowerCase(),
        }));

      this.stargateChainsCache = { timestamp: now, data: chains };
      return chains;
    } catch (error: unknown) {
      if (this.stargateChainsCache) {
        this.logger.warn(
          `Failed to refresh Stargate chains, using cache: ${this.errorMessage(error)}`,
        );
        return this.stargateChainsCache.data;
      }
      throw new BadRequestException(
        `Unable to fetch Stargate chains: ${this.errorMessage(error)}`,
      );
    }
  }

  private async fetchStargateTokens(forceRefresh = false): Promise<StargateToken[]> {
    const now = Date.now();
    if (
      !forceRefresh &&
      this.stargateTokensCache &&
      now - this.stargateTokensCache.timestamp < this.stargateCacheTtlMs
    ) {
      return this.stargateTokensCache.data;
    }

    try {
      const response = await axios.get<{ tokens?: StargateToken[] }>(
        `${this.stargateApiBaseUrl}/tokens`,
        { timeout: 20000 },
      );
      const rawTokens = Array.isArray(response.data?.tokens) ? response.data.tokens : [];
      const tokens = rawTokens
        .filter(
          (token) =>
            token &&
            typeof token.chainKey === 'string' &&
            typeof token.address === 'string' &&
            typeof token.symbol === 'string' &&
            Number.isFinite(Number(token.decimals)),
        )
        .map((token) => ({
          ...token,
          chainKey: token.chainKey.trim().toLowerCase(),
          address: this.normalizeAddressIfPossible(token.address) || token.address,
          decimals: Number(token.decimals),
          symbol: token.symbol.trim(),
          name: token.name?.trim() || token.symbol.trim(),
          isBridgeable: Boolean(token.isBridgeable),
        }));

      this.stargateTokensCache = { timestamp: now, data: tokens };
      return tokens;
    } catch (error: unknown) {
      if (this.stargateTokensCache) {
        this.logger.warn(
          `Failed to refresh Stargate tokens, using cache: ${this.errorMessage(error)}`,
        );
        return this.stargateTokensCache.data;
      }
      throw new BadRequestException(
        `Unable to fetch Stargate tokens: ${this.errorMessage(error)}`,
      );
    }
  }

  private async fetchStargateQuotes(
    params: Record<string, string>,
  ): Promise<StargateQuote[]> {
    try {
      const response = await axios.get<{
        quotes?: StargateQuote[];
        error?: { message?: string };
      }>(`${this.stargateApiBaseUrl}/quotes`, {
        params,
        timeout: 25000,
      });

      const quotes = Array.isArray(response.data?.quotes) ? response.data.quotes : [];
      if (quotes.length === 0 && response.data?.error?.message) {
        this.logger.debug(`Stargate quote returned no routes: ${response.data.error.message}`);
      }

      return quotes;
    } catch (error: unknown) {
      if (error instanceof BadRequestException) {
        throw error;
      }

      if (axios.isAxiosError(error)) {
        const payload = error.response?.data as
          | { error?: { message?: string }; message?: string | string[]; quotes?: unknown }
          | undefined;
        const status = error.response?.status;
        const rawQuotes = payload?.quotes;
        const hasEmptyQuotesPayload = Array.isArray(rawQuotes) && rawQuotes.length === 0;
        const message = payload?.error?.message ||
          (Array.isArray(payload?.message) ? payload.message.join('; ') : payload?.message) ||
          error.message;

        // Stargate can return 4xx with an empty quotes list for unsupported pair/amount combinations.
        if ((status === 400 || status === 404 || status === 422) && hasEmptyQuotesPayload) {
          this.logger.debug(`Stargate quote returned no routes: ${message}`);
          return [];
        }

        if (this.isNoRouteStargateError(status, message)) {
          this.logger.debug(`Stargate quote returned no routes: ${message}`);
          return [];
        }
        throw new BadRequestException(`Stargate quote failed: ${message}`);
      }

      throw new BadRequestException(`Stargate quote failed: ${this.errorMessage(error)}`);
    }
  }

  private isNoRouteStargateError(status: number | undefined, message: string | undefined): boolean {
    if (!message) {
      return false;
    }

    if (status !== undefined && status >= 500) {
      return false;
    }

    const normalized = message.trim().toLowerCase();
    return (
      normalized.includes('no supported routes') ||
      normalized.includes('no route found') ||
      normalized.includes('no routes found') ||
      normalized.includes('no route available') ||
      normalized.includes('no stargate route') ||
      normalized.includes('no stargate quote')
    );
  }

  private async resolveStargateToken(
    chainKey: string,
    tokenIdentifier: string,
    options?: { requireBridgeable?: boolean },
  ): Promise<StargateToken> {
    const normalizedChainKey = chainKey.trim().toLowerCase();
    const normalizedIdentifier = tokenIdentifier.trim();
    const requireBridgeable = options?.requireBridgeable ?? true;
    const tokens = await this.fetchStargateTokens();
    const chainTokens = tokens.filter(
      (token) => token.chainKey.toLowerCase() === normalizedChainKey,
    );

    if (chainTokens.length === 0) {
      throw new BadRequestException(`Unsupported chainKey "${chainKey}"`);
    }

    const normalizedAddress = this.normalizeAddressIfPossible(normalizedIdentifier);
    const byAddress = normalizedAddress
      ? chainTokens.find(
          (token) => token.address.toLowerCase() === normalizedAddress.toLowerCase(),
        )
      : null;
    const bySymbol = chainTokens.find(
      (token) => token.symbol.toLowerCase() === normalizedIdentifier.toLowerCase(),
    );

    const token = byAddress || bySymbol;
    if (!token) {
      throw new BadRequestException(
        `Unknown token "${tokenIdentifier}" for chain "${chainKey}"`,
      );
    }

    if (requireBridgeable && !token.isBridgeable) {
      throw new BadRequestException(
        `Token "${token.symbol}" on "${chainKey}" is not bridgeable via Stargate`,
      );
    }

    return {
      ...token,
      chainKey: token.chainKey.toLowerCase(),
      address: this.normalizeAddressIfPossible(token.address) || token.address,
    };
  }

  private normalizeBridgeQuotes(
    quotes: StargateQuote[],
    context: {
      srcToken: StargateToken;
      dstToken: StargateToken;
      chains: StargateChain[];
      tokens: StargateToken[];
    },
  ): NormalizedBridgeQuote[] {
    return quotes.map((quote) => {
      const route = quote.route || 'unknown';
      const srcChainKey = (quote.srcChainKey || context.srcToken.chainKey).toLowerCase();
      const dstChainKey = (quote.dstChainKey || context.dstToken.chainKey).toLowerCase();
      const srcAmountBaseUnits = this.parseBigIntOrZero(quote.srcAmount);
      const dstAmountBaseUnits = this.parseBigIntOrZero(quote.dstAmount);
      const dstAmountMinBaseUnits = this.parseBigIntOrZero(quote.dstAmountMin);

      const fees = (quote.fees || []).map((fee) => {
        const feeChainKey = (fee.chainKey || srcChainKey).toLowerCase();
        const tokenAddress = fee.token || '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE';
        const normalizedTokenAddress =
          this.normalizeAddressIfPossible(tokenAddress) || tokenAddress;
        const feeToken = this.findStargateToken(
          context.tokens,
          feeChainKey,
          normalizedTokenAddress,
        );
        const feeDecimals =
          feeToken?.decimals ??
          (this.isNativeTokenAddress(normalizedTokenAddress)
            ? this.nativeTokenDecimals(context.chains, feeChainKey)
            : 18);
        const feeAmountBaseUnits = this.parseBigIntOrZero(fee.amount);

        return {
          type: (fee.type || 'unknown').toLowerCase(),
          chainKey: feeChainKey,
          tokenAddress: normalizedTokenAddress,
          tokenSymbol:
            feeToken?.symbol ||
            (this.isNativeTokenAddress(normalizedTokenAddress)
              ? this.nativeTokenSymbol(context.chains, feeChainKey)
              : 'UNKNOWN'),
          amount: this.fromBaseUnits(feeAmountBaseUnits, feeDecimals),
          amountBaseUnits: feeAmountBaseUnits.toString(),
        };
      });

      return {
        route,
        executionType: 'evm',
        externalUrl: null,
        srcChainKey,
        dstChainKey,
        srcChainId: this.chainIdByKey(context.chains, srcChainKey),
        dstChainId: this.chainIdByKey(context.chains, dstChainKey),
        srcAmount: this.fromBaseUnits(srcAmountBaseUnits, context.srcToken.decimals),
        srcAmountBaseUnits: srcAmountBaseUnits.toString(),
        dstAmount: this.fromBaseUnits(dstAmountBaseUnits, context.dstToken.decimals),
        dstAmountBaseUnits: dstAmountBaseUnits.toString(),
        dstAmountMin: this.fromBaseUnits(dstAmountMinBaseUnits, context.dstToken.decimals),
        dstAmountMinBaseUnits: dstAmountMinBaseUnits.toString(),
        estimatedDurationSeconds:
          typeof quote.duration?.estimated === 'number' &&
          Number.isFinite(quote.duration.estimated)
            ? quote.duration.estimated
            : null,
        fees,
        transactions: {
          approve: this.extractStepTransaction(quote.steps, 'approve'),
          bridge: this.extractStepTransaction(quote.steps, 'bridge'),
        },
        error: quote.error?.message || null,
      };
    });
  }

  private selectPreferredRawQuote(
    quotes: StargateQuote[],
    preferredRoute?: string,
  ): StargateQuote | null {
    const candidates = quotes.filter((quote) => !quote.error?.message);
    if (candidates.length === 0) {
      return null;
    }

    const normalizedPreferredRoute = preferredRoute?.trim().toLowerCase();
    if (normalizedPreferredRoute) {
      const match = candidates.find(
        (quote) => quote.route?.toLowerCase() === normalizedPreferredRoute,
      );
      if (match) {
        return match;
      }
    }

    return candidates[0];
  }

  private selectPreferredBridgeQuote(
    quotes: NormalizedBridgeQuote[],
    preferredRoute?: string,
  ): NormalizedBridgeQuote | null {
    const executableQuotes = quotes.filter(
      (quote) =>
        !quote.error &&
        (quote.executionType === 'external' || Boolean(quote.transactions.bridge)),
    );

    if (executableQuotes.length === 0) {
      return null;
    }

    const normalizedPreferredRoute = preferredRoute?.trim().toLowerCase();
    if (normalizedPreferredRoute) {
      const match = executableQuotes.find(
        (quote) => quote.route.toLowerCase() === normalizedPreferredRoute,
      );
      if (match) {
        return match;
      }
    }

    return executableQuotes[0];
  }

  private extractStepTransaction(
    steps: StargateQuoteStep[] | undefined,
    stepType: 'approve' | 'bridge',
  ): EvmTransactionRequest | null {
    if (!Array.isArray(steps)) {
      return null;
    }

    const step = steps.find((candidate) => candidate.type?.toLowerCase() === stepType);
    if (!step?.transaction?.to || !step.transaction.data) {
      return null;
    }

    const to = this.normalizeAddressIfPossible(step.transaction.to);
    if (!to) {
      return null;
    }

    const from = step.transaction.from
      ? this.normalizeAddressIfPossible(step.transaction.from)
      : null;

    return {
      to,
      data: step.transaction.data,
      ...(from ? { from } : {}),
      value: this.isBigIntString(step.transaction.value) ? step.transaction.value : '0',
    };
  }

  private findStargateToken(
    tokens: StargateToken[],
    chainKey: string,
    tokenAddress?: string,
  ): StargateToken | null {
    if (!tokenAddress) {
      return null;
    }

    return (
      tokens.find(
        (token) =>
          token.chainKey.toLowerCase() === chainKey.toLowerCase() &&
          token.address.toLowerCase() === tokenAddress.toLowerCase(),
      ) || null
    );
  }

  private chainIdByKey(chains: StargateChain[], chainKey: string): number | null {
    const chain = chains.find(
      (candidate) => candidate.chainKey.toLowerCase() === chainKey.toLowerCase(),
    );
    if (!chain || !Number.isFinite(chain.chainId)) {
      return null;
    }
    return Number(chain.chainId);
  }

  private nativeTokenSymbol(chains: StargateChain[], chainKey: string): string {
    const chain = chains.find(
      (candidate) => candidate.chainKey.toLowerCase() === chainKey.toLowerCase(),
    );
    return chain?.nativeCurrency?.symbol || 'NATIVE';
  }

  private nativeTokenDecimals(chains: StargateChain[], chainKey: string): number {
    const chain = chains.find(
      (candidate) => candidate.chainKey.toLowerCase() === chainKey.toLowerCase(),
    );
    const decimals = Number(chain?.nativeCurrency?.decimals ?? 18);
    if (!Number.isFinite(decimals) || decimals < 0) {
      return 18;
    }
    return decimals;
  }

  private toBaseUnits(amount: string, decimals: number, fieldName: string): bigint {
    try {
      return ethers.parseUnits(this.normalizeDecimal(amount), decimals);
    } catch {
      throw new BadRequestException(
        `Invalid ${fieldName}. Could not parse amount for token decimals=${decimals}`,
      );
    }
  }

  private fromBaseUnits(amount: bigint, decimals: number): string {
    const formatted = ethers.formatUnits(amount, decimals);
    return this.normalizeDecimal(formatted);
  }

  private applySlippage(amount: bigint, slippageBps: number): bigint {
    if (!Number.isInteger(slippageBps) || slippageBps < 1 || slippageBps > 5000) {
      throw new BadRequestException('slippageBps must be an integer between 1 and 5000');
    }

    const minAmount = (amount * (10000n - BigInt(slippageBps))) / 10000n;
    return minAmount > 0n ? minAmount : 1n;
  }

  private parseRequiredBigInt(value: string | undefined, fieldName: string): bigint {
    if (!this.isBigIntString(value)) {
      throw new BadRequestException(`Invalid ${fieldName} returned by Stargate`);
    }
    return BigInt(value);
  }

  private parseBigIntOrZero(value: string | undefined): bigint {
    if (!this.isBigIntString(value)) {
      return 0n;
    }

    try {
      return BigInt(value);
    } catch {
      return 0n;
    }
  }

  private isBigIntString(value: string | undefined): value is string {
    return typeof value === 'string' && /^\d+$/.test(value);
  }

  private normalizeDecimal(value: string): string {
    const raw = value.trim();
    if (!raw.includes('.')) {
      const whole = raw.replace(/^0+(?=\d)/, '');
      return whole.length > 0 ? whole : '0';
    }

    const [wholeRaw, fractionRaw = ''] = raw.split('.');
    const whole = wholeRaw.replace(/^0+(?=\d)/, '') || '0';
    const fraction = fractionRaw.replace(/0+$/, '');

    return fraction.length > 0 ? `${whole}.${fraction}` : whole;
  }

  private rescaleBaseUnits(amount: bigint, fromDecimals: number, toDecimals: number): bigint {
    if (toDecimals === fromDecimals) {
      return amount;
    }

    if (toDecimals > fromDecimals) {
      return amount * 10n ** BigInt(toDecimals - fromDecimals);
    }

    const scaled = amount / 10n ** BigInt(fromDecimals - toDecimals);
    return scaled > 0n ? scaled : 1n;
  }

  private normalizeEvmAddress(address: string, fieldName: string): string {
    const normalized = this.normalizeAddressIfPossible(address);
    if (!normalized) {
      throw new BadRequestException(`${fieldName} must be a valid EVM address`);
    }
    return normalized;
  }

  private normalizeAddressIfPossible(address?: string): string | null {
    if (!address || typeof address !== 'string') {
      return null;
    }

    const trimmed = address.trim();
    if (!/^0x[a-fA-F0-9]{40}$/.test(trimmed)) {
      return null;
    }

    try {
      return ethers.getAddress(trimmed);
    } catch {
      return null;
    }
  }

  private normalizeBridgeParticipantAddress(address: string, fieldName: string): string {
    const trimmed = address.trim();
    if (!trimmed) {
      throw new BadRequestException(`${fieldName} is required`);
    }

    const evmAddress = this.normalizeAddressIfPossible(trimmed);
    if (evmAddress) {
      return evmAddress;
    }

    if (this.looksLikeStellarAddress(trimmed)) {
      return trimmed;
    }

    // Keep non-empty identifier for external providers that may support other formats.
    return trimmed;
  }

  private looksLikeStellarAddress(address: string): boolean {
    return /^G[A-Z2-7]{55}$/.test(address);
  }

  private isNativeTokenAddress(address: string): boolean {
    return address.toLowerCase() === '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee';
  }

  private normalizeTxHash(hash: string, fieldName: string): string {
    const normalized = hash.trim().toLowerCase();
    if (!/^0x[a-f0-9]{64}$/.test(normalized)) {
      throw new BadRequestException(`${fieldName} must be a valid EVM transaction hash`);
    }
    return normalized;
  }

  private normalizeStellarTxHash(hash: string, fieldName: string): string {
    const normalized = hash.trim().toLowerCase().replace(/^0x/, '');
    if (!/^[a-f0-9]{64}$/.test(normalized)) {
      throw new BadRequestException(`${fieldName} must be a valid Stellar transaction hash`);
    }
    return normalized;
  }

  private normalizeEvmTxHashIfPossible(hash?: string): string | null {
    if (!hash || typeof hash !== 'string') {
      return null;
    }

    const normalized = hash.trim().toLowerCase();
    if (!/^0x[a-f0-9]{64}$/.test(normalized)) {
      return null;
    }

    return normalized;
  }

  private normalizeStellarTxHashIfPossible(hash?: string): string | null {
    if (!hash || typeof hash !== 'string') {
      return null;
    }

    const normalized = hash.trim().toLowerCase().replace(/^0x/, '');
    if (!/^[a-f0-9]{64}$/.test(normalized)) {
      return null;
    }

    return normalized;
  }

  private isStellarNotFoundError(error: unknown): boolean {
    const withResponse = error as { response?: { status?: number } };
    if (withResponse.response?.status === 404) {
      return true;
    }

    const withMessage = error as { message?: string };
    if (
      typeof withMessage.message === 'string' &&
      withMessage.message.toLowerCase().includes('not found')
    ) {
      return true;
    }

    return false;
  }

  private async resolveBridgeTransactionConfirmation(
    sourceChainKey: string,
    bridgeTxHash: string,
  ): Promise<'confirmed' | 'pending' | 'failed'> {
    const normalizedChainKey = this.normalizeChainKeyHint(sourceChainKey) || sourceChainKey;
    if (normalizedChainKey === STELLAR_CHAIN_KEY) {
      const normalizedHash = this.normalizeStellarTxHashIfPossible(bridgeTxHash);
      if (!normalizedHash) {
        return 'pending';
      }

      try {
        const txRecord = (await this.server
          .transactions()
          .transaction(normalizedHash)
          .call()) as { successful?: unknown };
        if (typeof txRecord.successful === 'boolean') {
          return txRecord.successful ? 'confirmed' : 'failed';
        }
        return 'confirmed';
      } catch (error: unknown) {
        if (this.isStellarNotFoundError(error)) {
          return 'pending';
        }
        this.logger.warn(
          `Failed to resolve bridge tx confirmation for stellar/${normalizedHash}: ${this.errorMessage(error)}`,
        );
        return 'pending';
      }
    }

    const normalizedHash = this.normalizeEvmTxHashIfPossible(bridgeTxHash);
    if (!normalizedHash) {
      return 'pending';
    }

    const rpcUrl = this.resolveRpcUrlForChain(normalizedChainKey);
    if (!rpcUrl) {
      return 'pending';
    }

    try {
      const provider = new ethers.JsonRpcProvider(rpcUrl);
      const receipt = await provider.getTransactionReceipt(normalizedHash);
      if (!receipt) {
        return 'pending';
      }
      return receipt.status === 1 ? 'confirmed' : 'failed';
    } catch (error: unknown) {
      this.logger.warn(
        `Failed to resolve bridge tx confirmation for ${normalizedChainKey}/${normalizedHash}: ${this.errorMessage(error)}`,
      );
      return 'pending';
    }
  }

  private normalizeBridgeReference(
    reference: string,
    status: string,
    sourceChainKey?: string,
  ): string {
    const trimmed = reference.trim();
    if (!trimmed) {
      throw new BadRequestException('bridgeTxHash/reference is required');
    }

    const normalizedStatus = status.trim().toLowerCase();
    const requiresTypedHash = !['redirected', 'external'].includes(normalizedStatus);
    if (requiresTypedHash) {
      const normalizedSourceChainKey =
        this.normalizeChainKeyHint(sourceChainKey) || sourceChainKey?.trim().toLowerCase();
      if (normalizedSourceChainKey === STELLAR_CHAIN_KEY) {
        return this.normalizeStellarTxHash(trimmed, 'bridgeTxHash');
      }
      return this.normalizeTxHash(trimmed, 'bridgeTxHash');
    }

    if (trimmed.length > 200) {
      throw new BadRequestException('bridgeTxHash/reference is too long');
    }

    return trimmed;
  }

  private normalizeOptimizerDepositTxHash(value?: string): string | undefined {
    if (!value) {
      return undefined;
    }

    const trimmed = value.trim();
    if (!trimmed) {
      return undefined;
    }

    if (trimmed.length > 200) {
      throw new BadRequestException('depositTxHash is too long');
    }

    return trimmed;
  }

  private resolveBridgeHistoryProvider(
    route: string | undefined,
    status: string,
    metadata: Record<string, unknown> | undefined,
  ): string {
    if (metadata && typeof metadata.provider === 'string' && metadata.provider.trim()) {
      return metadata.provider.trim().toLowerCase();
    }

    if (route && route.trim()) {
      const normalizedRoute = route.trim().toLowerCase();
      const routeProvider = this.resolveProviderFromRoute(normalizedRoute);
      if (routeProvider) {
        return routeProvider;
      }
    }

    if (status === 'redirected' || status === 'external') {
      return this.stellarBridgeProviderName;
    }

    return 'stargate';
  }

  private resolveProviderFromRoute(route: string): string | null {
    const providerSegment = route.split('/')[0]?.trim();
    if (!providerSegment) {
      return null;
    }

    if (providerSegment.includes('stargate')) {
      return 'stargate';
    }

    if (providerSegment === this.stellarCircleCctpV2ProviderName.trim().toLowerCase()) {
      return this.stellarCircleCctpV2ProviderName;
    }

    if (providerSegment === this.stellarBridgeProviderName.trim().toLowerCase()) {
      return this.stellarBridgeProviderName;
    }

    if (providerSegment.includes('stellar') || providerSegment.includes('layerzero')) {
      return this.stellarBridgeProviderName;
    }

    if (providerSegment.includes('circle') || providerSegment.includes('cctp')) {
      return this.stellarCircleCctpV2ProviderName;
    }

    return providerSegment;
  }

  private mapBridgeHistoryRecord(record: Record<string, unknown>) {
    const createdAt = record.createdAt instanceof Date
      ? record.createdAt.toISOString()
      : typeof record.createdAt === 'string'
      ? record.createdAt
      : null;
    const updatedAt = record.updatedAt instanceof Date
      ? record.updatedAt.toISOString()
      : typeof record.updatedAt === 'string'
      ? record.updatedAt
      : null;

    return {
      id: String(record._id || ''),
      provider: String(record.provider || 'stargate'),
      srcChainKey: String(record.srcChainKey || ''),
      dstChainKey: String(record.dstChainKey || ''),
      srcAddress: String(record.srcAddress || ''),
      dstAddress: String(record.dstAddress || ''),
      srcTokenSymbol: String(record.srcTokenSymbol || ''),
      dstTokenSymbol: String(record.dstTokenSymbol || ''),
      srcAmount: String(record.srcAmount || '0'),
      ...(record.dstAmount ? { dstAmount: String(record.dstAmount) } : {}),
      ...(record.dstAmountMin ? { dstAmountMin: String(record.dstAmountMin) } : {}),
      ...(record.route ? { route: String(record.route) } : {}),
      ...(record.approvalTxHash ? { approvalTxHash: String(record.approvalTxHash) } : {}),
      bridgeTxHash: String(record.bridgeTxHash || ''),
      status: String(record.status || 'submitted'),
      ...(typeof record.estimatedDurationSeconds === 'number'
        ? { estimatedDurationSeconds: record.estimatedDurationSeconds }
        : {}),
      ...(record.feeAmount ? { feeAmount: String(record.feeAmount) } : {}),
      ...(record.feeTokenSymbol ? { feeTokenSymbol: String(record.feeTokenSymbol) } : {}),
      ...(record.metadata ? { metadata: record.metadata } : {}),
      createdAt,
      updatedAt,
    };
  }

  private resolveStargateRpcUrls(value: string | undefined): Record<string, string> {
    if (!value) {
      return {};
    }

    const normalizedMap: Record<string, string> = {};

    const mapEntry = (key: string, rpcUrl: string) => {
      const normalizedKey = key.trim().toLowerCase();
      const normalizedUrl = rpcUrl.trim();
      if (!normalizedKey || !normalizedUrl) {
        return;
      }
      normalizedMap[normalizedKey] = normalizedUrl;
    };

    try {
      const parsed = JSON.parse(value);
      if (parsed && typeof parsed === 'object') {
        for (const [key, rpcUrl] of Object.entries(parsed as Record<string, unknown>)) {
          if (typeof rpcUrl === 'string') {
            mapEntry(key, rpcUrl);
          }
        }
      }
    } catch {
      const pairs = value.split(',');
      for (const pair of pairs) {
        const [rawKey, ...rest] = pair.split('=');
        const rawUrl = rest.join('=');
        if (rawKey && rawUrl) {
          mapEntry(rawKey, rawUrl);
        }
      }
    }

    return normalizedMap;
  }

  private resolveRpcUrlForChain(chainKey: string): string | null {
    const normalizedKey = chainKey.trim().toLowerCase();

    const explicitEnvKey = `STARGATE_RPC_URL_${this.chainEnvSuffix(normalizedKey)}`;
    const explicit = process.env[explicitEnvKey];
    if (explicit && explicit.trim()) {
      return explicit.trim();
    }

    const mapped = this.stargateRpcUrls[normalizedKey];
    if (mapped) {
      return mapped;
    }

    const genericEnvKey = `${this.chainEnvSuffix(normalizedKey)}_RPC_URL`;
    const generic = process.env[genericEnvKey];
    if (generic && generic.trim()) {
      return generic.trim();
    }

    return null;
  }

  private createBridgeExecutorSigner(chainKey: string, rpcUrl: string): ethers.Wallet {
    const privateKey = this.resolveBridgeExecutorPrivateKey(chainKey);
    if (!privateKey) {
      throw new BadRequestException(
        `No bridge executor private key configured for chain "${chainKey}". ` +
          `Set BRIDGE_EXECUTOR_PRIVATE_KEY, BRIDGE_EXECUTOR_PRIVATE_KEY_${this.chainEnvSuffix(chainKey)}, ` +
          'or BRIDGE_EXECUTOR_PRIVATE_KEYS.',
      );
    }

    try {
      const provider = new ethers.JsonRpcProvider(rpcUrl);
      return new ethers.Wallet(privateKey, provider);
    } catch {
      throw new BadRequestException(`Invalid bridge executor private key for chain "${chainKey}"`);
    }
  }

  private resolveBridgeExecutorAddress(chainKey: string): string {
    const privateKey = this.resolveBridgeExecutorPrivateKey(chainKey);
    if (!privateKey) {
      throw new BadRequestException(
        `No bridge executor private key configured for chain "${chainKey}". ` +
          `Set BRIDGE_EXECUTOR_PRIVATE_KEY, BRIDGE_EXECUTOR_PRIVATE_KEY_${this.chainEnvSuffix(chainKey)}, ` +
          'or BRIDGE_EXECUTOR_PRIVATE_KEYS.',
      );
    }

    try {
      return new ethers.Wallet(privateKey).address;
    } catch {
      throw new BadRequestException(`Invalid bridge executor private key for chain "${chainKey}"`);
    }
  }

  private resolveBridgeExecutorPrivateKey(chainKey: string): string | null {
    const normalizedChainKey = chainKey.trim().toLowerCase();
    const explicitEnvKey = `BRIDGE_EXECUTOR_PRIVATE_KEY_${this.chainEnvSuffix(normalizedChainKey)}`;
    const explicit = this.normalizeBridgeExecutorPrivateKey(process.env[explicitEnvKey]);
    if (explicit) {
      return explicit;
    }

    const mapped = this.normalizeBridgeExecutorPrivateKey(
      this.bridgeExecutorPrivateKeys[normalizedChainKey],
    );
    if (mapped) {
      return mapped;
    }

    return this.normalizeBridgeExecutorPrivateKey(this.bridgeExecutorPrivateKey);
  }

  private chainEnvSuffix(chainKey: string): string {
    return chainKey.replace(/[^a-z0-9]/gi, '_').toUpperCase();
  }

  private normalizeBridgeExecutorPrivateKey(privateKey: string | undefined): string | null {
    const trimmed = privateKey?.trim();
    if (!trimmed) {
      return null;
    }

    const withPrefix = trimmed.startsWith('0x') ? trimmed : `0x${trimmed}`;
    if (!/^0x[a-fA-F0-9]{64}$/.test(withPrefix)) {
      throw new BadRequestException('Bridge executor private key must be a valid 32-byte hex string');
    }

    return withPrefix;
  }

  private resolveBridgeExecutorPrivateKeys(value: string | undefined): Record<string, string> {
    if (!value) {
      return {};
    }

    const mappedKeys: Record<string, string> = {};
    const mapEntry = (chainKey: string, privateKey: string) => {
      const normalizedKey = chainKey.trim().toLowerCase();
      if (!normalizedKey) {
        return;
      }
      const normalizedPrivateKey = this.normalizeBridgeExecutorPrivateKey(privateKey);
      if (!normalizedPrivateKey) {
        return;
      }
      mappedKeys[normalizedKey] = normalizedPrivateKey;
    };

    try {
      const parsed = JSON.parse(value);
      if (parsed && typeof parsed === 'object') {
        for (const [chainKey, privateKey] of Object.entries(parsed as Record<string, unknown>)) {
          if (typeof privateKey === 'string') {
            mapEntry(chainKey, privateKey);
          }
        }
      }
    } catch {
      const pairs = value.split(',');
      for (const pair of pairs) {
        const [rawChainKey, ...rest] = pair.split('=');
        const rawPrivateKey = rest.join('=');
        if (rawChainKey && rawPrivateKey) {
          mapEntry(rawChainKey, rawPrivateKey);
        }
      }
    }

    return mappedKeys;
  }

  private resolveBridgeExecutionReceiptTimeoutMs(value: string | undefined): number {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 5000) {
      return 120_000;
    }

    return Math.floor(parsed);
  }

  private resolveStellarCctpApiTimeout(value: string | undefined): number {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 3000) {
      return 15_000;
    }

    return Math.floor(parsed);
  }

  private resolveDefiLlamaApiTimeoutMs(value: string | undefined): number {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 3000) {
      return 12_000;
    }
    return Math.floor(parsed);
  }

  private resolveDefiLlamaMaxPools(value: string | undefined): number {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 10) {
      return 120;
    }
    return Math.min(Math.floor(parsed), 2000);
  }

  private resolveBooleanEnv(value: string | undefined): boolean {
    if (!value) {
      return false;
    }

    const normalized = value.trim().toLowerCase();
    return normalized === 'true' || normalized === '1' || normalized === 'yes' || normalized === 'on';
  }

  private resolveStellarBridgeFeeBps(value: string | undefined): number {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 0 || parsed > 3000) {
      return 80;
    }
    return Math.floor(parsed);
  }

  private resolveStargateCacheTtl(value: string | undefined): number {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 5000) {
      return 60_000;
    }

    return Math.floor(parsed);
  }

  private createBridgeChainLookup(
    chains: Array<{
      chainKey: string;
      name: string;
      shortName: string;
    }>,
  ): Map<string, string> {
    const lookup = new Map<string, string>();

    const add = (alias: string | null | undefined, chainKey: string) => {
      const normalized = this.normalizeLookupKey(alias);
      if (!normalized) {
        return;
      }
      lookup.set(normalized, chainKey);
    };

    for (const chain of chains) {
      const chainKey = chain.chainKey.trim().toLowerCase();
      add(chainKey, chainKey);
      add(chain.name, chainKey);
      add(chain.shortName, chainKey);

      if (chainKey === 'ethereum') {
        add('eth', chainKey);
        add('evm', chainKey);
        add('mainnet', chainKey);
      }
      if (chainKey === 'polygon') {
        add('matic', chainKey);
      }
      if (chainKey === 'arbitrum') {
        add('arb', chainKey);
        add('arbitrumone', chainKey);
      }
      if (chainKey === STELLAR_CHAIN_KEY) {
        add('xlm', chainKey);
      }
      if (chainKey === 'optimism') {
        add('op', chainKey);
      }
      if (chainKey === 'bsc' || chainKey === 'binance') {
        add('bnb', chainKey);
        add('bsc', chainKey);
      }
    }

    return lookup;
  }

  private normalizeLookupKey(value: string | null | undefined): string {
    if (!value) {
      return '';
    }
    return value.trim().toLowerCase().replace(/[^a-z0-9]/g, '');
  }

  private resolveBridgeChainKeyForYield(
    yieldChain: string,
    lookup: Map<string, string>,
  ): string | null {
    const normalized = this.normalizeLookupKey(yieldChain);
    if (!normalized) {
      return null;
    }

    const direct = lookup.get(normalized);
    if (direct) {
      return direct;
    }

    for (const [alias, chainKey] of lookup.entries()) {
      if (normalized.includes(alias) || alias.includes(normalized)) {
        return chainKey;
      }
    }

    return null;
  }

  private yieldMatchesAsset(assetValue: string, assetSymbol: string): boolean {
    const normalizedAsset = assetValue.trim().toUpperCase();
    if (!normalizedAsset) {
      return false;
    }
    if (normalizedAsset === assetSymbol) {
      return true;
    }

    const tokens = normalizedAsset.split(/[^A-Z0-9]+/).filter(Boolean);
    return tokens.includes(assetSymbol);
  }

  private resolvePreferredOptimizerOpportunity(
    dto: CreateOptimizerPlanDto,
    assetSymbol: string,
    bridgeChainLookup: Map<string, string>,
  ): { opportunity: YieldOpportunity; chainKey: string } | null {
    const preferredProtocol = this.normalizeAddressString(dto.preferredProtocol);
    const preferredChain = this.normalizeAddressString(dto.preferredChain);
    if (!preferredProtocol || !preferredChain) {
      return null;
    }

    const chainKey =
      this.resolveBridgeChainKeyForYield(preferredChain, bridgeChainLookup) ||
      this.normalizeChainKeyHint(preferredChain);
    if (!chainKey) {
      return null;
    }

    const normalizedApy = Math.min(this.toPositiveNumber(dto.preferredApy), 1000);
    const normalizedTvl = this.toPositiveNumber(dto.preferredTvlUsd);
    const riskScore =
      typeof dto.preferredRiskScore === 'number' && Number.isFinite(dto.preferredRiskScore)
        ? Math.max(0, Math.min(100, Math.round(dto.preferredRiskScore)))
        : this.computeRiskScore(normalizedApy, normalizedTvl, false);
    const category: YieldCategory =
      dto.category && this.isYieldCategory(dto.category) ? dto.category : 'lending';
    const protocolCode = this.protocolCode(preferredProtocol);
    const chainLabel = preferredChain.trim().toLowerCase();
    const protocolLabel = preferredProtocol.trim();

    return {
      chainKey,
      opportunity: {
        id: `preferred-${this.normalizeProtocolKey(protocolLabel)}-${chainKey}-${assetSymbol.toLowerCase()}`,
        name: `${protocolLabel} ${assetSymbol}`,
        protocol: protocolLabel,
        protocolLogo: protocolCode,
        logo: protocolCode,
        asset: assetSymbol,
        chain: chainLabel,
        category,
        apy: normalizedApy,
        tvl: normalizedTvl,
        riskScore,
        risk: this.riskLevel(riskScore),
      },
    };
  }

  private resolveOptimizerBalanceCandidates(
    wallets: Array<Record<string, unknown>>,
    connectedWallets: Array<Record<string, unknown>>,
    context: {
      assetSymbol: string;
      sourceChainKey?: string;
      sourceAddress?: string;
      sourceBalance?: string;
    },
  ): OptimizerBalanceCandidate[] {
    const byChain = new Map<string, OptimizerBalanceCandidate>();

    const upsert = (candidate: OptimizerBalanceCandidate) => {
      const existing = byChain.get(candidate.chainKey);
      if (!existing) {
        byChain.set(candidate.chainKey, candidate);
        return;
      }

      const existingAmount =
        typeof existing.availableAmount === 'number' ? existing.availableAmount : -1;
      const candidateAmount =
        typeof candidate.availableAmount === 'number' ? candidate.availableAmount : -1;

      if (candidateAmount > existingAmount) {
        byChain.set(candidate.chainKey, {
          ...candidate,
          address: candidate.address || existing.address,
        });
        return;
      }

      byChain.set(candidate.chainKey, {
        ...existing,
        address: existing.address || candidate.address,
        availableAmount:
          typeof existing.availableAmount === 'number'
            ? existing.availableAmount
            : candidate.availableAmount,
        source:
          existing.source === 'unknown' && candidate.source !== 'unknown'
            ? candidate.source
            : existing.source,
      });
    };

    for (const wallet of wallets) {
      const chainKey = this.normalizeWalletChainToBridgeKey(
        typeof wallet.chain === 'string' ? wallet.chain : '',
      );
      if (!chainKey) {
        continue;
      }

      const balances = Array.isArray(wallet.balances)
        ? (wallet.balances as Array<Record<string, unknown>>)
        : [];
      const matchedBalances = balances.filter((balance) => {
        const asset = typeof balance.asset === 'string' ? balance.asset : '';
        return this.extractAssetSymbol(asset) === context.assetSymbol;
      });

      if (matchedBalances.length === 0) {
        continue;
      }

      const totalAmount = matchedBalances.reduce((sum, balance) => {
        const amount = this.toPositiveNumber(balance.amount);
        return sum + amount;
      }, 0);
      if (totalAmount <= 0) {
        continue;
      }

      const address =
        this.normalizeAddressString(wallet.address) || this.normalizeAddressString(wallet.publicKey);
      upsert({
        chainKey,
        address,
        availableAmount: totalAmount,
        source: 'wallet-cache',
      });
    }

    const requestChainKey = this.normalizeChainKeyHint(context.sourceChainKey);
    const requestBalance = this.toPositiveNumber(context.sourceBalance);
    if (requestChainKey && requestBalance > 0) {
      upsert({
        chainKey: requestChainKey,
        address: this.normalizeAddressString(context.sourceAddress),
        availableAmount: requestBalance,
        source: 'request',
      });
    }

    for (const connectedWallet of connectedWallets) {
      const chainKey = this.resolveConnectedWalletChainKey(connectedWallet);
      if (!chainKey) {
        continue;
      }
      const address = this.normalizeAddressString(connectedWallet.publicKey);
      if (!address) {
        continue;
      }

      upsert({
        chainKey,
        address,
        availableAmount: null,
        source: 'connected-wallet',
      });
    }

    return Array.from(byChain.values()).sort((left, right) => {
      const leftAmount = typeof left.availableAmount === 'number' ? left.availableAmount : -1;
      const rightAmount = typeof right.availableAmount === 'number' ? right.availableAmount : -1;
      return rightAmount - leftAmount;
    });
  }

  private selectOptimizerSourceCandidate(
    candidates: OptimizerBalanceCandidate[],
    preferredChainKey: string | undefined,
    requiredAmount: number,
  ): OptimizerBalanceCandidate | null {
    if (candidates.length === 0) {
      return null;
    }

    const preferred = this.normalizeChainKeyHint(preferredChainKey);
    if (preferred) {
      const preferredMatch = candidates.find((candidate) => candidate.chainKey === preferred);
      if (preferredMatch) {
        return preferredMatch;
      }
    }

    const withEnoughBalance = candidates.find(
      (candidate) =>
        typeof candidate.availableAmount === 'number' && candidate.availableAmount >= requiredAmount,
    );
    if (withEnoughBalance) {
      return withEnoughBalance;
    }

    const withKnownBalance = candidates.find(
      (candidate) => typeof candidate.availableAmount === 'number' && candidate.availableAmount > 0,
    );
    if (withKnownBalance) {
      return withKnownBalance;
    }

    return candidates[0];
  }

  private normalizeChainKeyHint(value: string | undefined): string | null {
    if (!value) {
      return null;
    }

    const normalized = value.trim().toLowerCase();
    if (!normalized) {
      return null;
    }

    if (
      normalized === 'eth' ||
      normalized === 'ethereum' ||
      normalized === 'mainnet' ||
      normalized === 'evm' ||
      normalized === 'sepolia'
    ) {
      return 'ethereum';
    }
    if (normalized === 'matic' || normalized === 'polygon') {
      return 'polygon';
    }
    if (normalized === 'arb' || normalized === 'arbitrum') {
      return 'arbitrum';
    }
    if (normalized === 'xlm' || normalized === STELLAR_CHAIN_KEY) {
      return STELLAR_CHAIN_KEY;
    }
    if (normalized === 'op' || normalized === 'optimism') {
      return 'optimism';
    }
    if (normalized === 'bsc' || normalized === 'binance' || normalized === 'bnb') {
      return 'bsc';
    }

    return normalized;
  }

  private normalizeWalletChainToBridgeKey(chain: string): string | null {
    const normalized = chain.trim().toLowerCase();
    if (!normalized) {
      return null;
    }

    if (normalized === 'stellar') {
      return STELLAR_CHAIN_KEY;
    }

    return this.normalizeChainKeyHint(normalized);
  }

  private resolveConnectedWalletChainKey(wallet: Record<string, unknown>): string | null {
    const walletType = typeof wallet.walletType === 'string' ? wallet.walletType.toLowerCase() : '';
    const publicKey = this.normalizeAddressString(wallet.publicKey);

    if (
      walletType === 'evm' ||
      walletType === 'metamask' ||
      walletType === 'coinbase' ||
      walletType === 'trust' ||
      walletType === 'phantom' ||
      walletType === 'walletconnect'
    ) {
      return 'ethereum';
    }

    if (walletType === 'stellar' || walletType === 'freighter' || walletType === 'albedo') {
      return STELLAR_CHAIN_KEY;
    }

    if (publicKey && publicKey.startsWith('0x')) {
      return 'ethereum';
    }
    if (publicKey && this.looksLikeStellarAddress(publicKey)) {
      return STELLAR_CHAIN_KEY;
    }

    return null;
  }

  private resolveConnectedWalletAddress(
    connectedWallets: Array<Record<string, unknown>>,
    chainKey: string,
  ): string | null {
    const normalizedChainKey = this.normalizeChainKeyHint(chainKey) || chainKey;
    const matched = connectedWallets.find((wallet) => {
      const walletChain = this.resolveConnectedWalletChainKey(wallet);
      if (!walletChain) {
        return false;
      }
      return walletChain === normalizedChainKey;
    });

    return matched ? this.normalizeAddressString(matched.publicKey) : null;
  }

  private normalizeAddressString(value: unknown): string | null {
    if (typeof value !== 'string') {
      return null;
    }

    const normalized = value.trim();
    return normalized.length > 0 ? normalized : null;
  }

  private extractAssetSymbol(asset: string): string {
    const normalized = asset.trim();
    if (!normalized) {
      return '';
    }
    if (normalized.toLowerCase() === 'native') {
      return 'XLM';
    }
    if (normalized.startsWith('LP:')) {
      return '';
    }

    const [firstToken] = normalized.split(':');
    const [symbol] = firstToken.split('-');
    return symbol.trim().toUpperCase();
  }

  private async resolveBridgeTokenIdentifierForSymbol(
    chainKey: string,
    assetSymbol: string,
  ): Promise<string> {
    const tokens = await this.getBridgeTokens(chainKey, true);
    const match = tokens.find(
      (token) =>
        typeof token.symbol === 'string' &&
        token.symbol.trim().toUpperCase() === assetSymbol.toUpperCase(),
    );

    if (!match) {
      throw new BadRequestException(
        `No bridgeable ${assetSymbol} token is configured for chain "${chainKey}"`,
      );
    }

    return match.address;
  }

  private async resolveFeeTokenForSettlement(
    chainKey: string,
    assetSymbol: string,
  ): Promise<ResolvedFeeToken> {
    const normalizedChainKey = this.normalizeChainKeyHint(chainKey) || chainKey;
    const normalizedSymbol = assetSymbol.trim().toUpperCase();
    const tokens = await this.getBridgeTokens(normalizedChainKey, false);
    const matchedToken = tokens.find(
      (token) => token.symbol.trim().toUpperCase() === normalizedSymbol,
    );

    if (matchedToken) {
      const isNativeStellar = normalizedChainKey === STELLAR_CHAIN_KEY && matchedToken.address === 'native';
      const stellarAsset =
        normalizedChainKey === STELLAR_CHAIN_KEY
          ? isNativeStellar
            ? 'XLM'
            : matchedToken.address
          : 'XLM';

      return {
        chainKey: normalizedChainKey,
        symbol: matchedToken.symbol.trim().toUpperCase(),
        decimals: Number(matchedToken.decimals),
        tokenAddress: matchedToken.address,
        isNative: normalizedChainKey === STELLAR_CHAIN_KEY ? isNativeStellar : this.isNativeTokenAddress(matchedToken.address),
        stellarAsset,
      };
    }

    if (normalizedChainKey !== STELLAR_CHAIN_KEY) {
      const chains = await this.getBridgeChains();
      const chain = chains.find((candidate) => candidate.chainKey === normalizedChainKey);
      const nativeSymbol = String(chain?.nativeCurrency?.symbol || 'NATIVE').trim().toUpperCase();
      if (normalizedSymbol === nativeSymbol || normalizedSymbol === 'NATIVE') {
        return {
          chainKey: normalizedChainKey,
          symbol: nativeSymbol,
          decimals: Number(chain?.nativeCurrency?.decimals ?? 18),
          tokenAddress:
            String(chain?.nativeCurrency?.address || '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'),
          isNative: true,
          stellarAsset: 'XLM',
        };
      }
    }

    throw new BadRequestException(
      `Unable to resolve token ${assetSymbol} on chain "${normalizedChainKey}" for fee settlement`,
    );
  }

  private async resolveConnectedWalletAddressForUser(
    userId: string,
    chainKey: string,
  ): Promise<string | null> {
    const connectedWallets = (await this.connectedWalletModel.find({ userId } as any).lean().exec()) as Array<
      Record<string, unknown>
    >;
    return this.resolveConnectedWalletAddress(connectedWallets, chainKey);
  }

  private normalizeFeeSettlementAddress(
    chainKey: string,
    address: string | null | undefined,
    fieldName: string,
  ): string {
    const value = this.normalizeAddressString(address);
    if (!value) {
      throw new BadRequestException(`${fieldName} is required`);
    }

    if (chainKey === STELLAR_CHAIN_KEY) {
      if (!this.looksLikeStellarAddress(value)) {
        throw new BadRequestException(`${fieldName} must be a valid Stellar address`);
      }
      return value;
    }

    return this.normalizeEvmAddress(value, fieldName);
  }

  private resolveOptimizerFeeCollectorAddress(chainKey: string): string {
    const normalizedChainKey = this.normalizeChainKeyHint(chainKey) || chainKey;
    const explicitEnvKey = `OPTIMIZER_FEE_COLLECTOR_ADDRESS_${this.chainEnvSuffix(normalizedChainKey)}`;
    const explicit = this.normalizeAddressString(process.env[explicitEnvKey]);
    if (explicit) {
      return this.normalizeFeeSettlementAddress(normalizedChainKey, explicit, explicitEnvKey);
    }

    const mapped = this.normalizeAddressString(this.optimizerFeeCollectorAddresses[normalizedChainKey]);
    if (mapped) {
      return this.normalizeFeeSettlementAddress(
        normalizedChainKey,
        mapped,
        'OPTIMIZER_FEE_COLLECTOR_ADDRESSES',
      );
    }

    const fallback = this.normalizeAddressString(this.optimizerFeeCollectorAddress);
    if (fallback) {
      return this.normalizeFeeSettlementAddress(
        normalizedChainKey,
        fallback,
        'OPTIMIZER_FEE_COLLECTOR_ADDRESS',
      );
    }

    throw new BadRequestException(
      `Missing fee collector address for chain "${normalizedChainKey}". ` +
        `Set OPTIMIZER_FEE_COLLECTOR_ADDRESS_${this.chainEnvSuffix(normalizedChainKey)} ` +
        'or OPTIMIZER_FEE_COLLECTOR_ADDRESS.',
    );
  }

  private async buildEvmFeeSettlementPayload(params: {
    chainKey: string;
    payerAddress: string;
    collectorAddress: string;
    token: ResolvedFeeToken;
    feeBaseUnits: bigint;
    feeAmount: string;
  }): Promise<EvmFeeSettlementPayload> {
    const bridgeChains = await this.getBridgeChains();
    const chain = bridgeChains.find((candidate) => candidate.chainKey === params.chainKey);
    const chainId = chain ? chain.chainId : null;

    let transaction: EvmTransactionRequest;
    if (params.token.isNative) {
      transaction = {
        to: params.collectorAddress,
        data: '0x',
        from: params.payerAddress,
        value: params.feeBaseUnits.toString(),
      };
    } else {
      const transferInterface = new ethers.Interface(ERC20_TRANSFER_ABI);
      transaction = {
        to: this.normalizeEvmAddress(params.token.tokenAddress, 'tokenAddress'),
        data: transferInterface.encodeFunctionData('transfer', [
          params.collectorAddress,
          params.feeBaseUnits,
        ]),
        from: params.payerAddress,
        value: '0',
      };
    }

    return {
      chainKey: params.chainKey,
      chainId,
      payerAddress: params.payerAddress,
      collectorAddress: params.collectorAddress,
      assetSymbol: params.token.symbol,
      tokenAddress: params.token.tokenAddress,
      tokenDecimals: params.token.decimals,
      amount: params.feeAmount,
      amountBaseUnits: params.feeBaseUnits.toString(),
      transaction,
    };
  }

  private async tryExecuteEvmFeeSettlementOnBackend(params: {
    chainKey: string;
    payerAddress: string;
    transaction: EvmTransactionRequest;
  }): Promise<{ executed: true; txHash: string } | { executed: false; reason: string }> {
    if (!this.bridgeExecutorEnabled) {
      return {
        executed: false,
        reason:
          'Backend settlement executor is disabled. Set BRIDGE_EXECUTOR_ENABLED=true for automatic fee deductions.',
      };
    }

    const rpcUrl = this.resolveRpcUrlForChain(params.chainKey);
    if (!rpcUrl) {
      return {
        executed: false,
        reason:
          `No RPC URL configured for chain "${params.chainKey}". ` +
          `Set STARGATE_EVM_RPC_URLS or STARGATE_RPC_URL_${this.chainEnvSuffix(params.chainKey)}.`,
      };
    }

    const signer = this.createBridgeExecutorSigner(params.chainKey, rpcUrl);
    if (signer.address.toLowerCase() !== params.payerAddress.toLowerCase()) {
      return {
        executed: false,
        reason:
          `Backend signer ${signer.address} does not match payer ${params.payerAddress}. ` +
          'Wallet signature required for this settlement.',
      };
    }

    const provider = signer.provider;
    if (!provider) {
      return { executed: false, reason: 'Backend signer provider is unavailable' };
    }

    try {
      const response = await signer.sendTransaction(this.toSignerTransactionRequest(params.transaction));
      await this.waitForBridgeExecutionReceipt(provider, response.hash, 'fee settlement');
      return { executed: true, txHash: response.hash.toLowerCase() };
    } catch (error: unknown) {
      return { executed: false, reason: this.errorMessage(error) };
    }
  }

  private async buildStellarFeeSettlementPayload(params: {
    payerAddress: string;
    collectorAddress: string;
    token: ResolvedFeeToken;
    feeAmount: string;
  }): Promise<StellarFeeSettlementPayload> {
    const account = await this.server.loadAccount(params.payerAddress);
    const fee = await this.feeEstimationService.estimateFee('stellar', 'payment');
    const builder = new StellarSdk.TransactionBuilder(account, {
      fee,
      networkPassphrase: this.getNetworkPassphrase(),
    });

    builder.addOperation(
      StellarSdk.Operation.payment({
        destination: params.collectorAddress,
        asset: this.parseAsset(params.token.stellarAsset),
        amount: params.feeAmount,
      }),
    );

    builder.setTimeout(180);
    const transaction = builder.build();

    return {
      chainKey: STELLAR_CHAIN_KEY,
      payerAddress: params.payerAddress,
      collectorAddress: params.collectorAddress,
      assetSymbol: params.token.symbol,
      asset: params.token.stellarAsset,
      amount: params.feeAmount,
      fee,
      xdr: transaction.toXDR(),
      network:
        this.getNetworkPassphrase() === StellarSdk.Networks.PUBLIC ? 'mainnet' : 'testnet',
    };
  }

  private async tryExecuteStellarFeeSettlementOnBackend(params: {
    payerAddress: string;
    xdr: string;
  }): Promise<{ executed: true; txHash: string } | { executed: false; reason: string }> {
    const secret = this.normalizeAddressString(this.optimizerFeeStellarSecret);
    if (!secret) {
      return {
        executed: false,
        reason:
          'Missing OPTIMIZER_FEE_STELLAR_SECRET. Wallet signature required for Stellar settlement.',
      };
    }
    if (!StellarSdk.StrKey.isValidEd25519SecretSeed(secret)) {
      return {
        executed: false,
        reason: 'OPTIMIZER_FEE_STELLAR_SECRET is not a valid Stellar secret seed',
      };
    }

    const signer = StellarSdk.Keypair.fromSecret(secret);
    if (signer.publicKey() !== params.payerAddress) {
      return {
        executed: false,
        reason:
          `Configured Stellar backend signer ${signer.publicKey()} does not match payer ${params.payerAddress}`,
      };
    }

    try {
      const transaction = StellarSdk.TransactionBuilder.fromXDR(
        params.xdr,
        this.getNetworkPassphrase(),
      );
      transaction.sign(signer);
      const response = await this.server.submitTransaction(transaction);
      return { executed: true, txHash: String(response.hash || '').toLowerCase() };
    } catch (error: unknown) {
      return { executed: false, reason: this.errorMessage(error) };
    }
  }

  private normalizeFeeSettlementTxHash(chainKey: string, txHash: string): string {
    const normalizedChainKey = this.normalizeChainKeyHint(chainKey) || chainKey;
    const raw = txHash.trim();

    if (normalizedChainKey === STELLAR_CHAIN_KEY) {
      const normalized = raw.toLowerCase().replace(/^0x/, '');
      if (!/^[a-f0-9]{64}$/.test(normalized)) {
        throw new BadRequestException('txHash must be a valid Stellar transaction hash');
      }
      return normalized;
    }

    if (raw.startsWith('0x')) {
      return this.normalizeTxHash(raw, 'txHash');
    }
    if (/^[a-fA-F0-9]{64}$/.test(raw)) {
      return this.normalizeTxHash(`0x${raw}`, 'txHash');
    }
    throw new BadRequestException('txHash must be a valid EVM transaction hash');
  }

  private resolveAddressMap(value: string | undefined): Record<string, string> {
    if (!value) {
      return {};
    }

    const map: Record<string, string> = {};
    const upsert = (chainKey: string, address: string) => {
      const normalizedChainKey = this.normalizeChainKeyHint(chainKey) || chainKey.trim().toLowerCase();
      const normalizedAddress = this.normalizeAddressString(address);
      if (!normalizedChainKey || !normalizedAddress) {
        return;
      }
      map[normalizedChainKey] = normalizedAddress;
    };

    try {
      const parsed = JSON.parse(value);
      if (parsed && typeof parsed === 'object') {
        for (const [chainKey, address] of Object.entries(parsed as Record<string, unknown>)) {
          if (typeof address === 'string') {
            upsert(chainKey, address);
          }
        }
      }
    } catch {
      const pairs = value.split(',');
      for (const pair of pairs) {
        const [rawChainKey, ...rest] = pair.split('=');
        const rawAddress = rest.join('=');
        if (rawChainKey && rawAddress) {
          upsert(rawChainKey, rawAddress);
        }
      }
    }

    return map;
  }

  private buildStellarProtocolUrl(protocol: string): string {
    const slug = this.normalizeProtocolKey(protocol);
    const overrides: Record<string, string> = {
      aquarius: 'https://aqua.network',
      'aquarius-stellar': 'https://aqua.network',
      blend: 'https://www.blend.capital/',
      'blend-pools-v2': 'https://www.blend.capital/',
      etherfuse: 'https://app.etherfuse.com',
      fxdao: 'https://www.fxdao.io/',
      phoenix: 'https://www.phoenix-hub.io',
      'phoenix-defi-hub': 'https://www.phoenix-hub.io',
      soroswap: 'https://www.soroswap.finance/',
      spiko: 'https://www.spiko.io',
      templar: 'https://app.templarfi.org',
      'templar-protocol': 'https://app.templarfi.org',
    };
    if (overrides[slug]) {
      return overrides[slug];
    }
    return 'https://stellar.expert/explorer/public';
  }

  private roundTo4Decimals(value: number): number {
    if (!Number.isFinite(value)) {
      return 0;
    }
    return Number(value.toFixed(4));
  }

  private generateOptimizerExternalReference(): string {
    const timestampHex = Date.now().toString(16);
    const randomHex = Math.floor(Math.random() * 0xffffffff)
      .toString(16)
      .padStart(8, '0');
    return `optimizer_external_${timestampHex}_${randomHex}`;
  }

  private async resolveOrCreateOptimizerWallet(
    userId: string,
    chainKey: string,
    addressCandidate: string | null,
  ): Promise<WalletDocument> {
    const normalizedChain = this.normalizeChainKeyHint(chainKey) || chainKey;
    const address =
      this.normalizeAddressString(addressCandidate) ||
      this.resolveConnectedWalletAddress(
        (await this.connectedWalletModel.find({ userId } as any).lean().exec()) as Array<
          Record<string, unknown>
        >,
        normalizedChain,
      );

    if (!address) {
      throw new BadRequestException(
        `No wallet address available for chain "${normalizedChain}" to open optimizer position`,
      );
    }

    let wallet = await this.walletModel.findOne({
      userId,
      chain: normalizedChain,
      address,
      isArchived: false,
    } as any);

    if (wallet) {
      return wallet;
    }

    const accessProfile = await this.accessControlService.getUserAccessProfile(userId);
    const maxTrackedChains = accessProfile.limits.maxTrackedChains;
    if (maxTrackedChains !== null) {
      const trackedChains = (
        (await this.walletModel.distinct('chain', {
          userId,
          isArchived: false,
        } as any)) as string[]
      )
        .map((chain) => this.normalizeChainKeyHint(chain) || chain.trim().toLowerCase())
        .filter((chain, index, values) => values.indexOf(chain) === index);

      this.accessControlService.assertDistinctLimit({
        profile: accessProfile,
        currentValues: trackedChains,
        candidateValue: normalizedChain,
        limit: maxTrackedChains,
        resourceLabel: maxTrackedChains === 1 ? 'tracked chain' : 'tracked chains',
      });
    }

    wallet = new this.walletModel({
      userId,
      chain: normalizedChain,
      address,
      publicKey: address,
      label: `${this.titleCase(normalizedChain)} Optimizer`,
      isWatchOnly: true,
      balances: [],
    });
    await wallet.save();
    return wallet;
  }

  private async createOptimizerPosition(
    walletId: string,
    payload: {
      strategyName: string;
      protocol: string;
      category: string;
      assetSymbol: string;
      amount: string;
      sourceChainKey: string;
      destinationChainKey: string;
      sourceAddress: string | null;
      destinationAddress: string | null;
      bridgeTxHash?: string;
      depositTxHash?: string;
      netApy: number;
      estimatedAnnualNetYield: number;
    },
  ): Promise<string> {
    const position = await this.defiPositionModel.create({
      walletId,
      protocol: payload.protocol,
      type: payload.category,
      assetId: `${payload.assetSymbol}:${payload.destinationChainKey}`,
      principal: {
        amount: payload.amount,
        asset: payload.assetSymbol,
        strategyName: payload.strategyName,
        sourceChainKey: payload.sourceChainKey,
        destinationChainKey: payload.destinationChainKey,
        sourceAddress: payload.sourceAddress,
        destinationAddress: payload.destinationAddress,
        crossChain: payload.sourceChainKey !== payload.destinationChainKey,
        bridgeTxHash: payload.bridgeTxHash,
        depositTxHash: payload.depositTxHash,
      },
      currentValue: {
        amount: payload.amount,
        asset: payload.assetSymbol,
        estimatedAnnualNetYield: payload.estimatedAnnualNetYield,
      },
      apy: payload.netApy,
      unclaimedRewards: '0',
      status: 'active',
    } as any);

    return String(position._id);
  }

  private async recordOptimizerActivityTransactions(params: {
    userId: string;
    sourceChainKey: string;
    destinationChainKey: string;
    sourceAddress: string | null;
    destinationAddress: string | null;
    category: string;
    assetSymbol: string;
    amount: string;
    bridgeTxHash?: string;
    externalUrl?: string;
    status: OptimizerExecutionStatus;
    route: string | null;
    positionId?: string;
    depositTxHash?: string;
  }): Promise<void> {
    const sourceWallet = await this.resolveOrCreateOptimizerWallet(
      params.userId,
      params.sourceChainKey,
      params.sourceAddress,
    );
    const destinationWallet = await this.resolveOrCreateOptimizerWallet(
      params.userId,
      params.destinationChainKey,
      params.destinationAddress || params.sourceAddress,
    );

    if (params.bridgeTxHash) {
      await this.upsertTransactionRecord({
        walletId: String(sourceWallet._id),
        chain: params.sourceChainKey,
        hash: params.bridgeTxHash,
        type: 'bridge',
        from: params.sourceAddress || undefined,
        to: params.destinationAddress || undefined,
        amount: params.amount,
        asset: params.assetSymbol,
        status:
          params.status === 'failed'
            ? 'failed'
            : params.status === 'bridge-submitted'
            ? 'pending'
            : 'completed',
        metadata: {
          fromAsset: params.assetSymbol,
          toAsset: params.assetSymbol,
          route: params.route,
          externalUrl: params.externalUrl,
        },
      });
    }

    if (params.positionId) {
      const syntheticHash = params.depositTxHash || `optimizer_position_${params.positionId}`;
      const normalizedCategory = params.category.trim().toLowerCase();
      await this.upsertTransactionRecord({
        walletId: String(destinationWallet._id),
        chain: params.destinationChainKey,
        hash: syntheticHash,
        type: this.resolveOptimizerPositionActivityType(params.category),
        from: params.destinationAddress || params.sourceAddress || undefined,
        to: params.destinationAddress || params.sourceAddress || undefined,
        amount: params.amount,
        asset: params.assetSymbol,
        status:
          params.status === 'failed'
            ? 'failed'
            : params.depositTxHash
            ? 'pending'
            : 'completed',
        metadata: {
          fromAsset: params.assetSymbol,
          toAsset: params.assetSymbol,
          positionId: params.positionId,
          optimizer: true,
          ...(normalizedCategory ? { category: normalizedCategory } : {}),
          ...(params.depositTxHash ? { depositTxHash: params.depositTxHash } : {}),
        },
      });
    }
  }

  private resolveOptimizerPositionActivityType(category?: string): 'deposit' | 'stake' {
    const normalizedCategory = (category || '').trim().toLowerCase();
    return normalizedCategory === 'staking' ? 'stake' : 'deposit';
  }

  private async recordOptimizerFeeTransaction(params: {
    userId: string;
    chainKey: string;
    hash: string;
    payerAddress: string;
    collectorAddress: string;
    amount: string;
    assetSymbol: string;
    optimizerExecutionId: string;
    settlementId: string;
    settlementStatus: OptimizerFeeSettlementStatus;
  }): Promise<void> {
    try {
      const wallet = await this.resolveOrCreateOptimizerWallet(
        params.userId,
        params.chainKey,
        params.payerAddress,
      );

      const txStatus =
        params.settlementStatus === 'submitted'
          ? 'pending'
          : params.settlementStatus === 'failed'
          ? 'failed'
          : 'completed';

      await this.upsertTransactionRecord({
        walletId: String(wallet._id),
        chain: params.chainKey,
        hash: params.hash,
        type: 'fee',
        from: params.payerAddress,
        to: params.collectorAddress,
        amount: params.amount,
        asset: params.assetSymbol,
        status: txStatus,
        metadata: {
          optimizerExecutionId: params.optimizerExecutionId,
          optimizerFeeSettlementId: params.settlementId,
          performanceFee: true,
        },
      });
    } catch (error: unknown) {
      this.logger.warn(`Failed to persist optimizer fee transaction: ${this.errorMessage(error)}`);
    }
  }

  private async upsertTransactionRecord(payload: {
    walletId: string;
    chain: string;
    hash: string;
    type: string;
    from?: string;
    to?: string;
    amount: string;
    asset: string;
    status: string;
    metadata?: Record<string, unknown>;
  }) {
    await this.transactionModel
      .findOneAndUpdate(
        { chain: payload.chain, hash: payload.hash } as any,
        {
          $set: {
            walletId: payload.walletId,
            hash: payload.hash,
            chain: payload.chain,
            type: payload.type,
            from: payload.from,
            to: payload.to,
            amount: payload.amount,
            asset: payload.asset,
            fee: '0',
            status: payload.status,
            timestamp: new Date(),
            metadata: payload.metadata || {},
          },
        },
        { upsert: true, new: true },
      )
      .exec();
  }

  private mapOptimizerExecutionRecord(record: Record<string, unknown>) {
    const createdAt = record.createdAt instanceof Date
      ? record.createdAt.toISOString()
      : typeof record.createdAt === 'string'
      ? record.createdAt
      : null;
    const updatedAt = record.updatedAt instanceof Date
      ? record.updatedAt.toISOString()
      : typeof record.updatedAt === 'string'
      ? record.updatedAt
      : null;
    const metadata =
      record.metadata && typeof record.metadata === 'object'
        ? (record.metadata as Record<string, unknown>)
        : {};
    const protocolDepositMetadata =
      metadata.protocolDeposit && typeof metadata.protocolDeposit === 'object'
        ? (metadata.protocolDeposit as Record<string, unknown>)
        : null;
    const protocolDepositStatus =
      protocolDepositMetadata && typeof protocolDepositMetadata.status === 'string'
        ? protocolDepositMetadata.status
        : null;
    const rawStatus = String(record.status || 'planned');
    const normalizedStatus =
      rawStatus === 'deposit-pending' && protocolDepositStatus === 'wallet-action-required'
        ? 'wallet-action-required'
        : rawStatus;

    return {
      id: String(record._id || ''),
      strategyName: String(record.strategyName || ''),
      assetSymbol: String(record.assetSymbol || ''),
      amount: String(record.amount || '0'),
      sourceChainKey: String(record.sourceChainKey || ''),
      destinationChainKey: String(record.destinationChainKey || ''),
      sourceAddress: record.sourceAddress ? String(record.sourceAddress) : null,
      destinationAddress: record.destinationAddress ? String(record.destinationAddress) : null,
      protocol: String(record.protocol || ''),
      category: String(record.category || ''),
      baselineApy: this.toPositiveNumber(record.baselineApy),
      optimizedApy: this.toPositiveNumber(record.optimizedApy),
      netApy: this.toPositiveNumber(record.netApy),
      performanceFeeBps: this.toPositiveNumber(record.performanceFeeBps),
      estimatedAnnualFee: this.toPositiveNumber(record.estimatedAnnualFee),
      estimatedAnnualNetYield: this.toPositiveNumber(record.estimatedAnnualNetYield),
      route: record.route ? String(record.route) : null,
      bridgeExecutionType: record.bridgeExecutionType ? String(record.bridgeExecutionType) : null,
      bridgeTxHash: record.bridgeTxHash ? String(record.bridgeTxHash) : null,
      approvalTxHash: record.approvalTxHash ? String(record.approvalTxHash) : null,
      externalUrl: record.externalUrl ? String(record.externalUrl) : null,
      positionId: record.positionId ? String(record.positionId) : null,
      status: normalizedStatus,
      lastError: record.lastError ? String(record.lastError) : null,
      flow: Array.isArray(record.flow)
        ? (record.flow as Array<Record<string, unknown>>).map((step) => ({
            index: this.toPositiveNumber(step.index),
            title: String(step.title || ''),
            detail: String(step.detail || ''),
          }))
        : [],
      metadata,
      createdAt,
      updatedAt,
    };
  }

  private mapOptimizerFeeSettlementRecord(record: Record<string, unknown>) {
    const createdAt =
      record.createdAt instanceof Date
        ? record.createdAt.toISOString()
        : typeof record.createdAt === 'string'
        ? record.createdAt
        : null;
    const updatedAt =
      record.updatedAt instanceof Date
        ? record.updatedAt.toISOString()
        : typeof record.updatedAt === 'string'
        ? record.updatedAt
        : null;

    return {
      id: String(record._id || ''),
      userId: String(record.userId || ''),
      optimizerExecutionId: String(record.optimizerExecutionId || ''),
      chainKey: String(record.chainKey || ''),
      assetSymbol: String(record.assetSymbol || ''),
      payerAddress: String(record.payerAddress || ''),
      collectorAddress: String(record.collectorAddress || ''),
      realizedProfitAmount: String(record.realizedProfitAmount || '0'),
      performanceFeeBps: this.toPositiveNumber(record.performanceFeeBps),
      feeAmount: String(record.feeAmount || '0'),
      settlementMode: String(record.settlementMode || 'wallet'),
      status: String(record.status || 'wallet-action-required'),
      txHash: record.txHash ? String(record.txHash) : null,
      txPayload:
        record.txPayload && typeof record.txPayload === 'object' ? record.txPayload : null,
      error: record.error ? String(record.error) : null,
      metadata: record.metadata && typeof record.metadata === 'object' ? record.metadata : {},
      createdAt,
      updatedAt,
    };
  }

  private parseStellarAmountBaseUnits(value: unknown): bigint {
    if (typeof value !== 'string') {
      return 0n;
    }

    const normalized = value.trim();
    if (!normalized) {
      return 0n;
    }

    try {
      return this.toBaseUnits(normalized, 7, 'stellar amount');
    } catch {
      return 0n;
    }
  }

  private async assertStellarBridgeSourceCanSendAsset(params: {
    sourcePublicKey: string;
    assetIdentifier: string;
    amountBaseUnits: bigint;
    assetDecimals: number;
  }): Promise<void> {
    if (!this.looksLikeStellarAddress(params.sourcePublicKey)) {
      throw new BadRequestException('srcAddress must be a valid Stellar address');
    }

    const asset = this.parseAsset(params.assetIdentifier);
    const assetLabel = this.toStellarAssetLabel(asset);
    const server = this.getStellarServer();

    let account: StellarSdk.Horizon.AccountResponse;
    try {
      account = await server.loadAccount(params.sourcePublicKey);
    } catch (error: unknown) {
      throw new BadRequestException(
        `Could not load Stellar source account ${params.sourcePublicKey}: ${this.errorMessage(
          error,
        )}`,
      );
    }

    const accountBalances = Array.isArray((account as any).balances)
      ? ((account as any).balances as Array<Record<string, unknown>>)
      : [];
    const hasAssetBalanceLine = accountBalances.some((balance) =>
      this.matchesStellarBalanceAsset(balance, asset),
    );

    if (!hasAssetBalanceLine) {
      if (!asset.isNative()) {
        throw new BadRequestException(
          `No trustline for ${assetLabel} found on ${params.sourcePublicKey}. Add trustline before bridging.`,
        );
      }

      throw new BadRequestException(
        `No spendable ${assetLabel} balance found on ${params.sourcePublicKey}.`,
      );
    }

    const spendableAmountBaseUnits = this.resolveStellarSpendableBalanceBaseUnits(
      accountBalances,
      asset,
    );
    if (spendableAmountBaseUnits < params.amountBaseUnits) {
      throw new BadRequestException(
        `Insufficient spendable ${assetLabel} balance on ${params.sourcePublicKey}. Required ${this.fromBaseUnits(
          params.amountBaseUnits,
          params.assetDecimals,
        )}, available ${this.fromBaseUnits(spendableAmountBaseUnits, params.assetDecimals)}.`,
      );
    }
  }

  private toStellarAssetLabel(asset: StellarSdk.Asset): string {
    if (asset.isNative()) {
      return 'XLM';
    }
    return `${asset.getCode().trim().toUpperCase()}:${asset.getIssuer().trim()}`;
  }

  private resolveStellarSpendableBalanceBaseUnits(
    accountBalances: Array<Record<string, unknown>>,
    asset: StellarSdk.Asset,
  ): bigint {
    const matchingBalance = accountBalances.find((balance) =>
      this.matchesStellarBalanceAsset(balance, asset),
    );
    if (!matchingBalance) {
      return 0n;
    }

    const balanceAmount = this.parseStellarAmountBaseUnits(matchingBalance.balance);
    const sellingLiabilities = this.parseStellarAmountBaseUnits(
      matchingBalance.selling_liabilities,
    );
    const spendable = balanceAmount - sellingLiabilities;

    return spendable > 0n ? spendable : 0n;
  }

  private matchesStellarBalanceAsset(
    balance: Record<string, unknown>,
    asset: StellarSdk.Asset,
  ): boolean {
    const assetType =
      typeof balance.asset_type === 'string' ? balance.asset_type.trim().toLowerCase() : '';

    if (asset.isNative()) {
      return assetType === 'native';
    }

    const balanceCode =
      typeof balance.asset_code === 'string' ? balance.asset_code.trim().toUpperCase() : '';
    const balanceIssuer =
      typeof balance.asset_issuer === 'string' ? balance.asset_issuer.trim() : '';

    return (
      (assetType === 'credit_alphanum4' || assetType === 'credit_alphanum12') &&
      balanceCode === asset.getCode().trim().toUpperCase() &&
      balanceIssuer === asset.getIssuer().trim()
    );
  }

  private async canStellarAccountReceiveAsset(
    accountId: string,
    asset: StellarSdk.Asset,
    network?: StellarNetwork,
  ): Promise<boolean> {
    if (!asset || asset.isNative()) {
      return true;
    }
    if (!this.looksLikeStellarAddress(accountId)) {
      return false;
    }

    try {
      const server = this.getStellarServer(network);
      const account = await server.loadAccount(accountId);
      const accountBalances = Array.isArray((account as any).balances)
        ? ((account as any).balances as Array<Record<string, unknown>>)
        : [];
      return accountBalances.some((balance) => this.matchesStellarBalanceAsset(balance, asset));
    } catch (error: unknown) {
      this.logger.warn(
        `Could not verify collector trustline for ${accountId}: ${this.errorMessage(error)}`,
      );
      return false;
    }
  }

  private parseAsset(asset: string): StellarSdk.Asset {
    const normalized = asset.trim();

    if (!normalized || normalized.toUpperCase() === 'XLM' || normalized.toLowerCase() === 'native') {
      return StellarSdk.Asset.native();
    }

    const [code, issuer] = normalized.split(':');
    if (!code || !issuer) {
      throw new BadRequestException(
        `Invalid asset format "${asset}". Use "XLM" or "CODE:ISSUER".`,
      );
    }

    if (!StellarSdk.StrKey.isValidEd25519PublicKey(issuer)) {
      throw new BadRequestException(`Invalid issuer in asset "${asset}"`);
    }

    return new StellarSdk.Asset(code, issuer);
  }

  private extractAssetSymbolFromAssetIdentifier(asset: string): string {
    const normalized = asset.trim();
    if (!normalized || normalized.toLowerCase() === 'native') {
      return 'XLM';
    }

    const [code] = normalized.split(':');
    const symbol = code?.trim().toUpperCase();
    return symbol || 'XLM';
  }

  private assetToString(assetType: string, code?: string, issuer?: string): string {
    if (assetType === 'native') return 'XLM';
    if (!code || !issuer) return 'UNKNOWN';
    return `${code}:${issuer}`;
  }

  private resolveStellarNetwork(network?: string): string {
    return (network || 'testnet').trim().toLowerCase();
  }

  private resolveRequestedStellarNetwork(network?: string): StellarNetwork {
    const normalized = this.resolveStellarNetwork(network);
    if (normalized === 'mainnet' || normalized === 'public') {
      return 'mainnet';
    }
    return 'testnet';
  }

  private getStellarServer(network?: string): StellarSdk.Horizon.Server {
    const resolvedNetwork = this.resolveRequestedStellarNetwork(network);
    return resolvedNetwork === 'mainnet' ? this.mainnetServer : this.testnetServer;
  }

  private getNetworkPassphrase(network?: string): string {
    const resolvedNetwork = this.resolveRequestedStellarNetwork(network || process.env.STELLAR_NETWORK);
    return resolvedNetwork === 'mainnet'
      ? StellarSdk.Networks.PUBLIC
      : StellarSdk.Networks.TESTNET;
  }

  private errorMessage(error: unknown): string {
    if (axios.isAxiosError(error)) {
      const responseData = error.response?.data;
      if (typeof responseData === 'string' && responseData.trim()) {
        return responseData.trim();
      }

      if (responseData && typeof responseData === 'object') {
        const payload = responseData as {
          message?: string | string[];
          error?: string | { message?: string | string[] };
        };
        const directMessage = Array.isArray(payload.message)
          ? payload.message.join('; ')
          : payload.message;
        if (typeof directMessage === 'string' && directMessage.trim()) {
          return directMessage.trim();
        }

        if (typeof payload.error === 'string' && payload.error.trim()) {
          return payload.error.trim();
        }

        if (
          payload.error &&
          typeof payload.error === 'object' &&
          'message' in payload.error
        ) {
          const nestedMessage = Array.isArray(payload.error.message)
            ? payload.error.message.join('; ')
            : payload.error.message;
          if (typeof nestedMessage === 'string' && nestedMessage.trim()) {
            return nestedMessage.trim();
          }
        }
      }

      if (typeof error.message === 'string' && error.message.trim()) {
        return error.message.trim();
      }
    }

    if (error instanceof Error && error.message) {
      return error.message;
    }

    if (typeof error === 'string' && error.trim()) {
      return error.trim();
    }

    return 'unknown error';
  }

  private async getStellarYieldOpportunities(): Promise<YieldOpportunity[]> {
    const protocols = await this.marketService.getSorobanProtocols();
    return protocols
      .map((protocol) => this.mapProtocolToYield(protocol))
      .filter((opportunity) => Number.isFinite(opportunity.apy) && Number.isFinite(opportunity.tvl))
      .sort((left, right) => right.tvl - left.tvl || right.apy - left.apy);
  }

  private async getDefiLlamaYieldOpportunities(): Promise<YieldOpportunity[]> {
    if (!this.defiLlamaYieldsEnabled || !this.defiLlamaYieldsUrl) {
      return [];
    }

    const [response, protocolLogoLookup] = await Promise.all([
      axios.get<DefiLlamaYieldResponse>(this.defiLlamaYieldsUrl, {
        timeout: this.defiLlamaApiTimeoutMs,
        headers: {
          Accept: 'application/json',
          'User-Agent': 'yielder-backend/defi-service',
        },
      }),
      this.getDefiLlamaProtocolLogoLookup(),
    ]);
    const pools = Array.isArray(response.data?.data) ? response.data.data : [];
    if (pools.length === 0) {
      return [];
    }

    return pools
      .map((pool) => this.mapDefiLlamaPoolToYield(pool, protocolLogoLookup))
      .filter((opportunity): opportunity is YieldOpportunity => Boolean(opportunity))
      .sort((left, right) => right.tvl - left.tvl || right.apy - left.apy)
      .slice(0, this.defiLlamaMaxPools);
  }

  private mapDefiLlamaPoolToYield(
    pool: DefiLlamaYieldPool,
    protocolLogoLookup: DefiLlamaProtocolLogoLookup,
  ): YieldOpportunity | null {
    const chainRaw = this.normalizeAddressString(pool.chain);
    const projectRaw = this.normalizeAddressString(pool.project);
    const symbolRaw = this.normalizeAddressString(pool.symbol);
    if (!chainRaw || !projectRaw || !symbolRaw) {
      return null;
    }

    const chain = this.normalizeYieldChainLabel(chainRaw);
    const protocol = this.titleCase(projectRaw);
    const asset = this.normalizeYieldAssetLabel(symbolRaw);
    const apy = this.resolveDefiLlamaApy(pool);
    const tvl = this.toPositiveNumber(pool.tvlUsd);
    if (apy <= 0 || tvl <= 0) {
      return null;
    }

    const category = this.resolveDefiLlamaYieldCategory(pool, apy);
    const stablecoin = this.resolveDefiLlamaStablecoin(pool, asset);
    const ilRisk = this.resolveDefiLlamaIlRisk(pool, category);
    const riskScore = this.computeRiskScore(apy, tvl, stablecoin, ilRisk);
    const risk = this.riskLevel(riskScore);
    const protocolKey = this.normalizeProtocolKey(projectRaw);
    const poolId = this.normalizeAddressString(pool.pool);
    const id = poolId || `${chain.toLowerCase()}-${protocolKey}-${asset.toLowerCase()}`;
    const protocolLogo = this.resolveDefiLlamaProtocolLogo(
      protocolKey,
      protocol,
      protocolLogoLookup,
    );

    return {
      id,
      name: `${protocol} ${asset}`,
      protocol,
      protocolLogo,
      logo: protocolLogo,
      asset,
      chain,
      category,
      apy: this.roundTo4Decimals(apy),
      tvl: this.roundTo4Decimals(tvl),
      riskScore,
      risk,
    };
  }

  private async getDefiLlamaProtocolLogoLookup(): Promise<DefiLlamaProtocolLogoLookup> {
    const emptyLookup: DefiLlamaProtocolLogoLookup = {
      bySlug: new Map<string, string>(),
      byName: new Map<string, string>(),
    };

    if (!this.defiLlamaYieldsEnabled || !this.defiLlamaProtocolsUrl) {
      return emptyLookup;
    }

    const now = Date.now();
    if (
      this.defiLlamaProtocolLogoCache &&
      now - this.defiLlamaProtocolLogoCache.timestamp < this.defiLlamaProtocolsCacheTtlMs
    ) {
      return this.defiLlamaProtocolLogoCache.data;
    }

    if (this.defiLlamaProtocolLogoInFlight) {
      return this.defiLlamaProtocolLogoInFlight;
    }

    const request = (async (): Promise<DefiLlamaProtocolLogoLookup> => {
      try {
        const response = await axios.get<DefiLlamaProtocolMeta[]>(this.defiLlamaProtocolsUrl, {
          timeout: this.defiLlamaApiTimeoutMs,
          headers: {
            Accept: 'application/json',
            'User-Agent': 'yielder-backend/defi-service',
          },
        });
        const protocols = Array.isArray(response.data) ? response.data : [];
        if (protocols.length === 0) {
          return emptyLookup;
        }

        const bySlug = new Map<string, string>();
        const byName = new Map<string, string>();

        for (const protocol of protocols) {
          const slugRaw = this.normalizeAddressString(protocol.slug);
          const nameRaw = this.normalizeAddressString(protocol.name);
          const logoRaw = this.normalizeAddressString(protocol.logo);
          const slugKey = this.normalizeProtocolKey(slugRaw || nameRaw || '');
          const nameKey = this.normalizeProtocolKey(nameRaw || slugRaw || '');
          const logo = this.resolveDefiLlamaLogoUrl(slugKey, logoRaw);

          if (!logo) {
            continue;
          }

          if (slugKey) {
            bySlug.set(slugKey, logo);
          }
          if (nameKey) {
            byName.set(nameKey, logo);
          }
        }

        const lookup: DefiLlamaProtocolLogoLookup = { bySlug, byName };
        this.defiLlamaProtocolLogoCache = {
          timestamp: Date.now(),
          data: lookup,
        };
        return lookup;
      } catch (error) {
        this.logger.warn(
          `Failed to fetch DeFiLlama protocol logos: ${this.errorMessage(error)}`,
        );
        return emptyLookup;
      }
    })();

    this.defiLlamaProtocolLogoInFlight = request;
    try {
      return await request;
    } finally {
      if (this.defiLlamaProtocolLogoInFlight === request) {
        this.defiLlamaProtocolLogoInFlight = null;
      }
    }
  }

  private resolveDefiLlamaProtocolLogo(
    protocolKey: string,
    protocolName: string,
    lookup: DefiLlamaProtocolLogoLookup,
  ): string {
    const bySlug = lookup.bySlug.get(protocolKey);
    if (bySlug) {
      return bySlug;
    }

    const normalizedName = this.normalizeProtocolKey(protocolName);
    if (normalizedName) {
      const byName = lookup.byName.get(normalizedName);
      if (byName) {
        return byName;
      }
    }

    return this.resolveDefiLlamaLogoUrl(protocolKey, null);
  }

  private resolveDefiLlamaLogoUrl(slugKey: string, explicitLogo: string | null): string {
    const logoCandidate = explicitLogo?.trim() || '';
    if (/^https?:\/\//i.test(logoCandidate)) {
      return logoCandidate;
    }

    const normalizedSlug = this.normalizeProtocolKey(slugKey);
    if (!normalizedSlug) {
      return `https://api.dicebear.com/9.x/shapes/svg?seed=defi`;
    }

    return `${this.defiLlamaIconsBaseUrl.replace(/\/+$/, '')}/${encodeURIComponent(
      normalizedSlug,
    )}.png`;
  }

  private mergeYieldOpportunities(opportunities: YieldOpportunity[]): YieldOpportunity[] {
    const deduped = new Map<string, YieldOpportunity>();

    for (const opportunity of opportunities) {
      if (
        !Number.isFinite(opportunity.apy) ||
        !Number.isFinite(opportunity.tvl) ||
        !opportunity.protocol ||
        !opportunity.asset ||
        !opportunity.chain
      ) {
        continue;
      }

      const chainKey = opportunity.chain.trim().toLowerCase();
      const protocolKey = this.normalizeProtocolKey(opportunity.protocol);
      const assetKey = opportunity.asset.trim().toUpperCase();
      const dedupeKey = `${chainKey}:${protocolKey}:${assetKey}:${opportunity.category}`;

      const existing = deduped.get(dedupeKey);
      if (!existing) {
        deduped.set(dedupeKey, opportunity);
        continue;
      }

      const preferred = opportunity.tvl > existing.tvl ? opportunity : existing;
      const mergedApy = this.roundTo4Decimals(Math.max(existing.apy, opportunity.apy));
      const mergedTvl = this.roundTo4Decimals(Math.max(existing.tvl, opportunity.tvl));
      const stablecoin = this.isLikelyStableAsset(preferred.asset);
      const ilRisk = preferred.category === 'liquidity' ? 'yes' : undefined;
      const riskScore = this.computeRiskScore(mergedApy, mergedTvl, stablecoin, ilRisk);

      deduped.set(dedupeKey, {
        ...preferred,
        apy: mergedApy,
        tvl: mergedTvl,
        riskScore,
        risk: this.riskLevel(riskScore),
      });
    }

    return Array.from(deduped.values()).sort(
      (left, right) => right.tvl - left.tvl || right.apy - left.apy || left.name.localeCompare(right.name),
    );
  }

  private resolveDefiLlamaApy(pool: DefiLlamaYieldPool): number {
    const directApy = this.toPositiveNumber(pool.apy);
    if (directApy > 0) {
      return directApy;
    }

    const apyBase = this.toPositiveNumber(pool.apyBase);
    const apyReward = this.toPositiveNumber(pool.apyReward);
    return apyBase + apyReward;
  }

  private resolveDefiLlamaYieldCategory(pool: DefiLlamaYieldPool, apy: number): YieldCategory {
    const categoryRaw = this.normalizeAddressString(pool.category) || '';
    const poolMetaRaw = this.normalizeAddressString(pool.poolMeta) || '';
    const exposureRaw = this.normalizeAddressString(pool.exposure) || '';
    const symbolRaw = this.normalizeAddressString(pool.symbol) || '';
    const descriptor = `${categoryRaw} ${poolMetaRaw} ${exposureRaw}`.toLowerCase();

    if (
      descriptor.includes('dex') ||
      descriptor.includes('amm') ||
      descriptor.includes('liquidity') ||
      descriptor.includes('lp') ||
      descriptor.includes('swap')
    ) {
      return 'liquidity';
    }

    if (
      descriptor.includes('stake') ||
      descriptor.includes('restake') ||
      descriptor.includes('farm') ||
      descriptor.includes('reward')
    ) {
      return 'staking';
    }

    if (
      descriptor.includes('lend') ||
      descriptor.includes('borrow') ||
      descriptor.includes('credit') ||
      descriptor.includes('money market') ||
      descriptor.includes('cdp')
    ) {
      return 'lending';
    }

    if (/[-/]/.test(symbolRaw)) {
      return 'liquidity';
    }

    if (apy >= 10) {
      return 'staking';
    }

    return 'lending';
  }

  private resolveDefiLlamaStablecoin(pool: DefiLlamaYieldPool, asset: string): boolean {
    const stablecoinRaw = pool.stablecoin;
    if (typeof stablecoinRaw === 'boolean') {
      return stablecoinRaw;
    }
    if (typeof stablecoinRaw === 'number') {
      return Number.isFinite(stablecoinRaw) && stablecoinRaw > 0;
    }
    if (typeof stablecoinRaw === 'string') {
      const normalized = stablecoinRaw.trim().toLowerCase();
      if (['true', '1', 'yes', 'stable'].includes(normalized)) {
        return true;
      }
      if (['false', '0', 'no', 'volatile'].includes(normalized)) {
        return false;
      }
    }

    return this.isLikelyStableAsset(asset);
  }

  private resolveDefiLlamaIlRisk(
    pool: DefiLlamaYieldPool,
    category: YieldCategory,
  ): string | undefined {
    const ilRiskRaw = pool.ilRisk;
    if (typeof ilRiskRaw === 'boolean') {
      return ilRiskRaw ? 'yes' : 'no';
    }
    if (typeof ilRiskRaw === 'string') {
      const normalized = ilRiskRaw.trim().toLowerCase();
      if (['yes', 'true', '1'].includes(normalized)) {
        return 'yes';
      }
      if (['no', 'false', '0'].includes(normalized)) {
        return 'no';
      }
    }

    return category === 'liquidity' ? 'yes' : undefined;
  }

  private normalizeYieldAssetLabel(symbol: string): string {
    const normalized = symbol.trim().toUpperCase().replace(/\s+/g, '');
    if (!normalized) {
      return 'N/A';
    }
    return normalized.slice(0, 40);
  }

  private normalizeYieldChainLabel(chain: string): string {
    const normalized = chain.trim().replace(/[_-]+/g, ' ').replace(/\s+/g, ' ');
    if (!normalized) {
      return 'Unknown';
    }

    const compact = normalized.toLowerCase();
    const aliases: Record<string, string> = {
      arbitrumone: 'Arbitrum',
      arbitrum: 'Arbitrum',
      ethereum: 'Ethereum',
      base: 'Base',
      optimism: 'Optimism',
      polygon: 'Polygon',
      bsc: 'BSC',
      binance: 'BSC',
      avalanche: 'Avalanche',
      stellar: 'Stellar',
    };
    const canonical = aliases[compact.replace(/[^a-z0-9]/g, '')];
    if (canonical) {
      return canonical;
    }

    return this.titleCase(normalized);
  }

  private filterYields(
    yields: YieldOpportunity[],
    chain?: string,
    category?: string,
    limit = 120,
  ): YieldOpportunity[] {
    const normalizedChain = chain?.trim().toLowerCase();
    const normalizedCategory = category?.trim().toLowerCase();
    const categoryFilter: YieldCategory | null =
      normalizedCategory && normalizedCategory !== 'all' && this.isYieldCategory(normalizedCategory)
        ? normalizedCategory
        : null;

    const filtered = yields.filter((item) => {
      const chainMatches = !normalizedChain || item.chain.toLowerCase() === normalizedChain;
      const categoryMatches = !categoryFilter || item.category === categoryFilter;
      return chainMatches && categoryMatches;
    });

    if (limit <= 0) {
      return filtered;
    }

    return filtered.slice(0, limit);
  }

  private isYieldCategory(value: string): value is YieldCategory {
    return value === 'lending' || value === 'liquidity' || value === 'staking';
  }

  private mapProtocolToYield(protocol: StellarProtocol): YieldOpportunity {
    const protocolNameRaw =
      typeof protocol.name === 'string' && protocol.name.trim() ? protocol.name : 'Unknown';
    const protocolName = this.titleCase(protocolNameRaw);
    const protocolSlugRaw =
      typeof protocol.slug === 'string' && protocol.slug.trim() ? protocol.slug : protocolName;
    const protocolSlug = this.normalizeProtocolKey(protocolSlugRaw);
    const symbol = this.resolveProtocolAssetSymbol(protocol);
    const apy = this.toPositiveNumber(protocol.apy);
    const tvl = this.toPositiveNumber(protocol.tvl);
    const categoryValue =
      typeof protocol.category === 'string' ? protocol.category : '';
    const category = this.resolveProtocolYieldCategory(categoryValue, apy);
    const stablecoin = this.isLikelyStableAsset(symbol);
    const ilRisk = category === 'liquidity' ? 'yes' : undefined;
    const riskScore = this.computeRiskScore(apy, tvl, stablecoin, ilRisk);
    const risk = this.riskLevel(riskScore);
    const logo = typeof protocol.logo === 'string' ? protocol.logo.trim() : '';
    const logoOrCode = logo || protocolSlug || this.protocolCode(protocolName);
    const protocolId = typeof protocol.id === 'string' && protocol.id.trim()
      ? protocol.id
      : `stellar-${protocolSlug || symbol}`;

    return {
      id: protocolId,
      name: `${protocolName} ${symbol}`,
      protocol: protocolName,
      protocolLogo: logoOrCode,
      logo: logoOrCode,
      asset: symbol,
      chain: 'Stellar',
      category,
      apy,
      tvl,
      riskScore,
      risk,
    };
  }

  private resolveProtocolYieldCategory(categoryValue: string, apy: number): YieldCategory {
    const normalized = categoryValue.trim().toLowerCase();
    if (
      normalized.includes('dex') ||
      normalized.includes('swap') ||
      normalized.includes('amm') ||
      normalized.includes('liquidity')
    ) {
      return 'liquidity';
    }

    if (
      normalized.includes('lend') ||
      normalized.includes('borrow') ||
      normalized.includes('credit')
    ) {
      return 'lending';
    }

    if (apy >= 10) {
      return 'staking';
    }

    return 'lending';
  }

  private resolveProtocolAssetSymbol(protocol: StellarProtocol): string {
    const tokenRaw = typeof protocol.token === 'string' ? protocol.token : '';
    const token = tokenRaw.trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
    if (token) {
      return token.slice(0, 12);
    }
    const slugOrNameRaw =
      typeof protocol.slug === 'string' && protocol.slug.trim()
        ? protocol.slug
        : typeof protocol.name === 'string'
          ? protocol.name
          : '';
    const slugOrName = slugOrNameRaw.trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
    return slugOrName ? slugOrName.slice(0, 12) : 'XLM';
  }

  private isLikelyStableAsset(symbol: string): boolean {
    const normalized = symbol.trim().toUpperCase();
    if (!normalized) {
      return false;
    }
    const knownStableSymbols = new Set([
      'USDC',
      'USDT',
      'DAI',
      'EURC',
      'EUTBL',
      'USTBL',
      'UKTBL',
      'EUR',
      'USD',
    ]);
    if (knownStableSymbols.has(normalized)) {
      return true;
    }
    return normalized.includes('USD');
  }

  private computeRiskScore(
    apy: number,
    tvl: number,
    stablecoin: boolean,
    ilRisk?: string,
  ): number {
    let score = 35;

    if (apy >= 30) score += 30;
    else if (apy >= 20) score += 20;
    else if (apy >= 12) score += 12;
    else if (apy >= 7) score += 6;

    if (tvl >= 500_000_000) score -= 14;
    else if (tvl >= 100_000_000) score -= 10;
    else if (tvl >= 20_000_000) score -= 6;
    else if (tvl < 2_000_000) score += 12;

    if (stablecoin) score -= 6;
    if ((ilRisk || '').toLowerCase() === 'yes') score += 10;

    return Math.max(5, Math.min(95, Math.round(score)));
  }

  private riskLevel(score: number): YieldRisk {
    if (score < 35) return 'Low';
    if (score < 65) return 'Medium';
    return 'High';
  }

  private protocolCode(project: string): string {
    const letters = project
      .replace(/[^a-z0-9 ]/gi, ' ')
      .split(' ')
      .filter(Boolean)
      .map((part) => part[0]?.toUpperCase() || '')
      .join('');
    return letters.slice(0, 4) || 'DEFI';
  }

  private titleCase(value: string): string {
    return value
      .split(/[\s_-]+/)
      .filter(Boolean)
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
  }

  private toPositiveNumber(value: unknown): number {
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }
}
