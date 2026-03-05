import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import axios, { AxiosRequestConfig } from 'axios';
import { Model } from 'mongoose';
import { Wallet, WalletDocument } from '../wallet/schemas/wallet.schema';
import { DeFiPosition, DeFiPositionDocument } from '../defi/schemas/defi-position.schema';
import { PriceFeedService, PriceQuote } from '../portfolio/price-feed.service';

export interface NetworkStats {
  volume24h: number;
  activeContracts: number;
  uniqueWallets: number;
  txCount24h: number;
  fees24h: number;
  contractCalls24h: number;
}

export interface Protocol {
  id: string;
  name: string;
  slug?: string;
  url?: string;
  category: string;
  tvl: number;
  apy: number;
  token: string;
  audited: boolean;
  users: number;
  logo: string;
  change24h: number;
}

export interface Asset {
  id: string;
  symbol: string;
  name: string;
  price: number;
  change24h: number;
  volume24h: number;
  marketCap: number;
  holders?: number;
  issuer?: string;
}

export interface LiquidityPool {
  id: string;
  pair: string;
  protocol: string;
  protocolSlug?: string;
  protocolLogo?: string;
  tvlUsd: number;
  volume24hUsd: number;
  apy: number;
  feeBps: number;
  totalShares: number;
  users: number;
  reserveCount: number;
  source: 'aquarius' | 'phoenix' | 'horizon';
}

export interface StellarAccountBalance {
  assetType: string;
  assetCode: string;
  assetIssuer?: string;
  balance: string;
  limit?: string;
}

export interface StellarAccountSummary {
  accountId: string;
  exists: boolean;
  sequence?: string;
  subentryCount?: number;
  lastModifiedLedger?: number;
  balances: StellarAccountBalance[];
  horizon: string;
  sorobanRpc: string;
  sorobanLatestLedger?: number;
}

type DeFiQueryAddressType = 'account' | 'contract' | 'invalid';

type DeFiSourceStatus = 'ok' | 'degraded' | 'unavailable';

export interface DeFiPositionValuePoint {
  timestamp: string;
  value: number;
}

export interface DeFiPositionTotals {
  totalCurrentValue: number;
  totalDepositValue: number;
  totalBorrowedValue: number | null;
  totalCurrentReturn: number | null;
  healthFactor: number | null;
}

export interface DeFiPositionDetailMetric {
  key: string;
  label: string;
  value: number;
  format: 'number' | 'percent' | 'ratio';
}

export interface DeFiPositionRecord {
  id: string;
  protocol: string;
  positionType: string;
  status: string;
  dataSource?: 'persisted' | 'live_fallback';
  assetId: string;
  assetLabel?: string;
  assetPair?: string;
  walletAddress?: string;
  semantics: string;
  totals: DeFiPositionTotals;
  apy?: number | null;
  unclaimedRewards?: number | null;
  detailMetrics?: DeFiPositionDetailMetric[];
  positionValueHistory?: DeFiPositionValuePoint[];
  updatedAt?: string;
  stale: boolean;
}

export interface DeFiSourceHealth {
  status: DeFiSourceStatus;
  latencyMs: number;
  detail: string;
}

export interface DeFiDataDiscrepancy {
  code: string;
  severity: 'info' | 'warning' | 'critical';
  message: string;
}

export interface DeFiAvailability {
  status: 'healthy' | 'degraded' | 'unavailable';
  sources: {
    mongo: DeFiSourceHealth;
    horizon: DeFiSourceHealth;
    sorobanRpc: DeFiSourceHealth;
  };
  fallbackPolicy: {
    onPositionDataUnavailable: string;
    onSourceDisagreement: string;
  };
  discrepancies: DeFiDataDiscrepancy[];
}

export interface DeFiPositionsResponse {
  query: {
    input: string;
    normalizedAddress: string;
    addressType: DeFiQueryAddressType;
  };
  generatedAt: string;
  latencyMs: number;
  availability: DeFiAvailability;
  summary: DeFiPositionTotals & {
    positionCount: number;
    protocolCount: number;
    totalAssetBalance: number;
    totalAssetBalanceUsd: number;
  };
  positions: DeFiPositionRecord[];
}

type TickerAsset = {
  assetCode: string;
  issuer?: string;
  displayName: string;
  priceUsd: number;
  change24hPercent: number;
  volume24hUsd: number;
  trustlines: number;
  supplyTokens: number;
};

type AquaPool = {
  index?: string;
  address?: string;
  total_apy?: number | string;
  fee?: number | string;
  total_share?: number | string;
  volume_usd?: number | string;
  volume_24h_usd?: number | string;
  liquidity_usd?: number | string;
  tokens_str?: string[];
};

type PhoenixPoolTokenInfo = {
  symbol?: string;
};

type PhoenixPoolAsset = {
  address?: string;
  token_info?: PhoenixPoolTokenInfo;
};

type PhoenixStatsPool = {
  pool_address?: string;
  asset_a_address?: string;
  asset_b_address?: string;
  asset_a?: PhoenixPoolAsset;
  asset_b?: PhoenixPoolAsset;
  total_fee_bps?: number | string;
  tvl_usd?: number | string;
};

type PhoenixWeekBucket = {
  year?: number | string;
  week?: number | string;
};

type PhoenixWeeklyVolumePoint = {
  week?: PhoenixWeekBucket;
  usdVolume?: number | string;
};

type TemplarMarketMetadata = {
  symbol?: string;
};

type TemplarMarket = {
  deployment?: string;
  borrowMetadata?: TemplarMarketMetadata;
  collateralMetadata?: TemplarMarketMetadata;
};

type TemplarTimeChunkConfiguration = {
  duration_ms?: number | string;
};

type TemplarMarketConfig = {
  time_chunk_configuration?: TemplarTimeChunkConfiguration;
};

type TemplarMarketDataEntry = {
  deployment?: string;
  config?: TemplarMarketConfig;
};

type TemplarSnapshotInner = {
  interest_rate?: number | string;
};

type TemplarSnapshot = {
  deployment?: string;
  yield?: number | string;
  availableBalance?: number | string;
  snapshot?: TemplarSnapshotInner;
};

type EtherfuseBondMint = {
  symbol?: string;
  supply?: number | string;
  currentTokenAmount?: number | string;
};

type EtherfuseCurrentIssuance = {
  interestRateBps?: number | string;
};

type EtherfuseBond = {
  currency?: string;
  mint?: EtherfuseBondMint;
  currentIssuance?: EtherfuseCurrentIssuance;
  interestRate?: number | string;
};

type EtherfuseStablebondsResponse = {
  bonds?: EtherfuseBond[];
};

type EtherfuseBridgeStellar = {
  totalSupply?: number | string;
};

type EtherfuseBridgeOptionEntry = {
  stellar?: EtherfuseBridgeStellar;
};

type EtherfuseBridgingOptionsResponse = Record<string, EtherfuseBridgeOptionEntry>;

type SpikoYieldResponse = {
  dailyYield?: number | string;
  weeklyYield?: number | string;
  monthlyYield?: number | string;
};

type SpikoTotalsResponse = {
  totalAssets?: {
    value?: number | string;
    currency?: string;
  };
};

type HorizonLedgerRecord = {
  closed_at?: string;
  successful_transaction_count?: number | string;
  operation_count?: number | string;
  base_fee_in_stroops?: number | string;
};

type HorizonBalanceRecord = {
  asset_type?: string;
  asset_code?: string;
  asset_issuer?: string;
  balance?: string;
  limit?: string;
  liquidity_pool_id?: string;
};

type HorizonAccountResponse = {
  account_id?: string;
  sequence?: string;
  subentry_count?: number | string;
  last_modified_ledger?: number | string;
  balances?: HorizonBalanceRecord[];
};

type HorizonLiquidityPoolReserve = {
  asset?: string;
  amount?: string;
};

type HorizonLiquidityPoolResponse = {
  id?: string;
  total_shares?: string;
  fee_bp?: number | string;
  last_modified_ledger?: number | string;
  reserves?: HorizonLiquidityPoolReserve[];
};

type RawWalletForDeFi = {
  _id: unknown;
  address?: string;
  publicKey?: string;
  chain?: string;
};

type RawDeFiPositionForQuery = {
  _id: unknown;
  walletId?: unknown;
  protocol?: string;
  type?: string;
  status?: string;
  assetId?: string;
  principal?: unknown;
  currentValue?: unknown;
  apy?: unknown;
  unclaimedRewards?: unknown;
  updatedAt?: Date | string;
  createdAt?: Date | string;
};

type CachedValue<T> = {
  timestamp: number;
  data: T;
};

type LedgerEstimate = {
  txCount24h: number;
  operationCount24h: number;
  baseFeeStroops: number;
};

type LiveAssetMarketData = {
  code: string;
  issuer?: string;
  priceUsd: number;
  change24hPercent: number;
};

type DefiLlamaYieldPool = {
  chain?: unknown;
  project?: unknown;
  symbol?: unknown;
  apy?: unknown;
  apyBase?: unknown;
  apyReward?: unknown;
  tvlUsd?: unknown;
  category?: unknown;
};

type DefiLlamaYieldResponse = {
  data?: DefiLlamaYieldPool[];
};

type DefiLlamaProtocolRecord = {
  name?: unknown;
  slug?: unknown;
  tvl?: unknown;
  chains?: unknown;
  chainTvls?: unknown;
  audits?: unknown;
  audit_links?: unknown;
};

type DefiLlamaStellarProtocolMetadata = {
  tvlByProtocol: Map<string, number>;
  auditedByProtocol: Map<string, boolean>;
};

type FallbackSorobanProtocol = {
  id: string;
  name: string;
  slug: string;
  category: string;
  token: string;
  audited: boolean;
  url: string;
};

const FALLBACK_SOROBAN_PROTOCOLS: FallbackSorobanProtocol[] = [
  {
    id: 'blend',
    name: 'Blend',
    slug: 'blend',
    category: 'lending',
    token: 'BLND',
    audited: true,
    url: 'https://www.blend.capital/',
  },
  {
    id: 'fxdao',
    name: 'FxDAO',
    slug: 'fxdao',
    category: 'stablecoin',
    token: 'FXD',
    audited: false,
    url: 'https://www.fxdao.io/',
  },
  {
    id: 'soroswap',
    name: 'Soroswap',
    slug: 'soroswap',
    category: 'dex',
    token: 'SORO',
    audited: false,
    url: 'https://www.soroswap.finance/',
  },
  {
    id: 'ondo-yield-assets',
    name: 'Ondo Yield Assets',
    slug: 'ondo-yield-assets',
    category: 'rwa',
    token: 'USDY',
    audited: true,
    url: 'https://ondo.finance/',
  },
  {
    id: 'aquarius-stellar',
    name: 'Aquarius',
    slug: 'aquarius-stellar',
    category: 'dex',
    token: 'AQUA',
    audited: true,
    url: 'https://aqua.network/',
  },
  {
    id: 'phoenix-defi-hub',
    name: 'Phoenix',
    slug: 'phoenix-defi-hub',
    category: 'dex',
    token: 'PHO',
    audited: true,
    url: 'https://www.phoenix-hub.io/',
  },
  {
    id: 'templar-protocol',
    name: 'Templar',
    slug: 'templar-protocol',
    category: 'lending',
    token: 'TMPL',
    audited: false,
    url: 'https://www.templarfi.org/',
  },
  {
    id: 'etherfuse',
    name: 'Etherfuse',
    slug: 'etherfuse',
    category: 'rwa',
    token: 'ETHF',
    audited: false,
    url: 'https://www.etherfuse.com/',
  },
  {
    id: 'spiko',
    name: 'Spiko',
    slug: 'spiko',
    category: 'rwa',
    token: 'EUTBL',
    audited: true,
    url: 'https://www.spiko.io/',
  },
  {
    id: 'allbridge-core',
    name: 'Allbridge Core',
    slug: 'allbridge-core',
    category: 'bridge',
    token: 'ABR',
    audited: false,
    url: 'https://core.allbridge.io/',
  },
  {
    id: 'stellar-dex',
    name: 'Stellar DEX',
    slug: 'stellar-dex',
    category: 'dex',
    token: 'XLM',
    audited: true,
    url: 'https://developers.stellar.org/docs/learn/encyclopedia/sdex/liquidity-on-stellar-sdex-liquidity-pools#sdex',
  },
  {
    id: 'lumenswap',
    name: 'LumenSwap',
    slug: 'lumenswap',
    category: 'dex',
    token: 'LSP',
    audited: false,
    url: 'https://lumenswap.io/',
  },
  {
    id: 'defindex',
    name: 'DeFindex',
    slug: 'defindex',
    category: 'yield',
    token: 'DFX',
    audited: false,
    url: 'https://www.defindex.io/',
  },
  {
    id: 'scopuly',
    name: 'Scopuly',
    slug: 'scopuly',
    category: 'dex',
    token: 'SCP',
    audited: false,
    url: 'https://scopuly.com/',
  },
  {
    id: 'excellar',
    name: 'Excellar',
    slug: 'excellar',
    category: 'trading',
    token: 'EXL',
    audited: false,
    url: 'https://excellar.finance/',
  },
  {
    id: 'vnx',
    name: 'VNX',
    slug: 'vnx',
    category: 'rwa',
    token: 'VNX',
    audited: false,
    url: 'https://vnx.li/',
  },
  {
    id: 'balanced-exchange',
    name: 'Balanced Exchange',
    slug: 'balanced-exchange',
    category: 'dex',
    token: 'BALN',
    audited: false,
    url: 'https://app.balanced.network/trade',
  },
  {
    id: 'near-intents',
    name: 'NEAR Intents',
    slug: 'near-intents',
    category: 'bridge',
    token: 'INT',
    audited: false,
    url: 'https://app.near-intents.org/',
  },
  {
    id: 'blend-backstop',
    name: 'Blend Backstop',
    slug: 'blend-backstop',
    category: 'insurance',
    token: 'BLND',
    audited: false,
    url: 'https://www.blend.capital/',
  },
  {
    id: 'blend-backstop-v2',
    name: 'Blend Backstop V2',
    slug: 'blend-backstop-v2',
    category: 'insurance',
    token: 'BLND',
    audited: false,
    url: 'https://www.blend.capital/',
  },
];

@Injectable()
export class MarketService {
  private readonly logger = new Logger(MarketService.name);
  private readonly stellarNetwork = (process.env.STELLAR_NETWORK || 'testnet').toLowerCase();
  private readonly isConfiguredMainnet =
    this.stellarNetwork === 'mainnet' || this.stellarNetwork === 'public';
  private readonly stellarExpertApi = 'https://api.stellar.expert/explorer/public';
  private readonly aquaPoolsApiUrl = process.env.MARKET_AQUA_POOLS_URL || 'https://amm-api.aqua.network/pools/?size=500';
  private readonly phoenixPoolsApiUrl = process.env.MARKET_PHOENIX_POOLS_URL || 'https://stats.phoenix-hub.io/api/pools';
  private readonly phoenixVolumeApiBaseUrl =
    (process.env.MARKET_PHOENIX_VOLUME_API_URL || 'https://api-phoenix-v2.decentrio.ventures').replace(/\/+$/, '');
  private readonly templarMarketsApiUrl =
    process.env.MARKET_TEMPLAR_MARKETS_URL || 'https://app.templarfi.org/api/markets?domain=app';
  private readonly templarSnapshotsApiUrl =
    process.env.MARKET_TEMPLAR_SNAPSHOTS_URL || 'https://app.templarfi.org/api/snapshots?domain=app';
  private readonly etherfuseStablebondsApiUrl =
    process.env.MARKET_ETHERFUSE_STABLEBONDS_URL || 'https://app.etherfuse.com/api/catalog/stablebonds';
  private readonly etherfuseBridgingOptionsApiUrl =
    process.env.MARKET_ETHERFUSE_BRIDGING_OPTIONS_URL || 'https://app.etherfuse.com/api/catalog/bridging-options';
  private readonly spikoPublicApiBaseUrl = (
    process.env.MARKET_SPIKO_PUBLIC_API_URL || 'https://public-api.spiko.io'
  ).replace(/\/+$/, '');
  private readonly defiLlamaYieldsUrl =
    process.env.DEFI_LLAMA_YIELDS_URL || 'https://yields.llama.fi/pools';
  private readonly defiLlamaProtocolsUrl =
    process.env.DEFI_LLAMA_PROTOCOLS_URL || 'https://api.llama.fi/protocols';
  private readonly defiLlamaIconsBaseUrl = (
    process.env.DEFI_LLAMA_ICONS_BASE_URL || 'https://icons.llama.fi'
  ).replace(/\/+$/, '');
  private readonly spikoShareClasses = (
    process.env.MARKET_SPIKO_SHARE_CLASSES || 'EUTBL,USTBL,UKTBL'
  )
    .split(',')
    .map((value) => value.trim().toUpperCase())
    .filter(Boolean);
  private readonly horizonApiMainnet =
    process.env.MARKET_HORIZON_URL_MAINNET ||
    process.env.STELLAR_HORIZON_URL_MAINNET ||
    (!process.env.MARKET_HORIZON_URL && this.isConfiguredMainnet
      ? process.env.STELLAR_HORIZON_URL
      : undefined) ||
    'https://horizon.stellar.org';
  private readonly horizonApiTestnet =
    process.env.MARKET_HORIZON_URL_TESTNET ||
    process.env.STELLAR_HORIZON_URL_TESTNET ||
    (!process.env.MARKET_HORIZON_URL && !this.isConfiguredMainnet
      ? process.env.STELLAR_HORIZON_URL
      : undefined) ||
    'https://horizon-testnet.stellar.org';
  private readonly horizonApi =
    process.env.MARKET_HORIZON_URL ||
    (this.isConfiguredMainnet ? this.horizonApiMainnet : this.horizonApiTestnet);
  private readonly fallbackHorizonApi =
    this.horizonApi === this.horizonApiMainnet
      ? this.horizonApiTestnet
      : this.horizonApiMainnet;
  private readonly sorobanRpcApiMainnet =
    process.env.MARKET_SOROBAN_RPC_URL_MAINNET || 'https://mainnet.sorobanrpc.com';
  private readonly sorobanRpcApiTestnet =
    process.env.MARKET_SOROBAN_RPC_URL_TESTNET || 'https://soroban-testnet.stellar.org';
  private readonly sorobanRpcApi =
    process.env.MARKET_SOROBAN_RPC_URL ||
    (this.isConfiguredMainnet ? this.sorobanRpcApiMainnet : this.sorobanRpcApiTestnet);
  private readonly fallbackSorobanRpcApi =
    this.sorobanRpcApi === this.sorobanRpcApiMainnet
      ? this.sorobanRpcApiTestnet
      : this.sorobanRpcApiMainnet;
  private readonly protocolCacheTtlMs = this.resolvePositiveIntEnv(
    process.env.MARKET_PROTOCOL_CACHE_TTL_MS,
    5 * 1000,
    2 * 1000,
    5 * 60 * 1000,
  );
  private readonly defiLlamaProtocolsCacheTtlMs = 5 * 60 * 1000;
  private readonly networkStatsCacheTtlMs = this.resolvePositiveIntEnv(
    process.env.MARKET_NETWORK_STATS_CACHE_TTL_MS,
    2 * 1000,
    1000,
    2 * 60 * 1000,
  );
  private readonly trendingAssetsCacheTtlMs = 15 * 1000;
  private readonly liquidityPoolsCacheTtlMs = this.resolvePositiveIntEnv(
    process.env.MARKET_LIQUIDITY_POOLS_CACHE_TTL_MS,
    10 * 1000,
    2 * 1000,
    5 * 60 * 1000,
  );
  private readonly defiPositionsCacheTtlMs = 8 * 1000;
  private readonly livePriceMaxAgeSeconds = 45;
  private readonly requestTimeoutMs = this.resolvePositiveIntEnv(
    process.env.MARKET_REQUEST_TIMEOUT_MS,
    12000,
    3000,
    120000,
  );
  private readonly warningCooldownMs = this.resolvePositiveIntEnv(
    process.env.MARKET_WARNING_COOLDOWN_MS,
    60 * 1000,
    5000,
    3_600_000,
  );
  private protocolsCache: CachedValue<Protocol[]> | null = null;
  private defiLlamaStellarProtocolMetadataCache: CachedValue<DefiLlamaStellarProtocolMetadata> | null = null;
  private networkStatsCache: CachedValue<NetworkStats> | null = null;
  private trendingAssetsCache: CachedValue<Asset[]> | null = null;
  private liquidityPoolsCache: CachedValue<LiquidityPool[]> | null = null;
  private defiPositionsCache = new Map<string, CachedValue<DeFiPositionsResponse>>();
  private protocolsInFlight: Promise<Protocol[]> | null = null;
  private defiLlamaStellarProtocolMetadataInFlight: Promise<DefiLlamaStellarProtocolMetadata> | null = null;
  private networkStatsInFlight: Promise<NetworkStats> | null = null;
  private trendingAssetsInFlight: Promise<Asset[]> | null = null;
  private liquidityPoolsInFlight: Promise<LiquidityPool[]> | null = null;
  private defiPositionsInFlight = new Map<string, Promise<DeFiPositionsResponse>>();
  private warningTimestamps = new Map<string, number>();

  constructor(
    @InjectModel(Wallet.name) private walletModel: Model<WalletDocument>,
    @InjectModel(DeFiPosition.name)
    private defiPositionModel: Model<DeFiPositionDocument>,
    private readonly priceFeedService: PriceFeedService,
  ) {}

  async getNetworkStats(): Promise<NetworkStats> {
    if (this.networkStatsCache && Date.now() - this.networkStatsCache.timestamp < this.networkStatsCacheTtlMs) {
      return this.networkStatsCache.data;
    }

    if (this.networkStatsInFlight) {
      return this.networkStatsInFlight;
    }

    const request = (async (): Promise<NetworkStats> => {
      try {
        const [xlmAssetRes, ledgersRes, operationsRes, protocolsRes] = await Promise.allSettled([
          this.getJsonWithRetry(`${this.stellarExpertApi}/asset/XLM`, {
            retries: 2,
          }),
          this.getJsonWithRetry(`${this.horizonApi}/ledgers`, {
            retries: 2,
            params: { order: 'desc', limit: 200 },
          }),
          this.getJsonWithRetry(`${this.horizonApi}/operations`, {
            retries: 2,
            params: { order: 'desc', limit: 200 },
          }),
          this.getSorobanProtocols(),
        ]);

        const xlmAsset =
          xlmAssetRes.status === 'fulfilled' ? this.parseExpertAsset(xlmAssetRes.value) : null;
        const volume24h = xlmAsset?.volume24hUsd ?? 0;
        const uniqueWallets = xlmAsset?.trustlines ?? 0;

        const ledgerEstimate =
          ledgersRes.status === 'fulfilled'
            ? this.estimateLedgerStatsFromLedgers(ledgersRes.value)
            : { txCount24h: 0, operationCount24h: 0, baseFeeStroops: 100 };
        const contractCallRatio =
          operationsRes.status === 'fulfilled' ? this.resolveContractCallRatio(operationsRes.value) : 0;
        const protocols = protocolsRes.status === 'fulfilled' ? protocolsRes.value : [];

        let txCount24h = ledgerEstimate.txCount24h;
        let operationCount24h = ledgerEstimate.operationCount24h;
        if (operationCount24h <= 0 && txCount24h > 0) {
          operationCount24h = Math.max(txCount24h, Math.round(txCount24h * 2));
        }

        if (txCount24h <= 0 && operationCount24h > 0) {
          txCount24h = Math.max(1, Math.round(operationCount24h / 2));
        }

        const fees24hXlm = this.roundTo4(
          (operationCount24h * Math.max(1, ledgerEstimate.baseFeeStroops)) / 10_000_000,
        );
        const contractCalls24h = Math.max(0, Math.round(operationCount24h * contractCallRatio));
        const networkStats: NetworkStats = {
          volume24h,
          activeContracts: protocols.length,
          uniqueWallets,
          txCount24h,
          fees24h: fees24hXlm,
          contractCalls24h,
        };

        this.networkStatsCache = {
          timestamp: Date.now(),
          data: networkStats,
        };

        return networkStats;
      } catch (error: unknown) {
        this.warnWithCooldown(
          'network-stats-upstream',
          `Failed to fetch upstream network stats. ${this.errorMessage(error)}`,
        );

        if (this.networkStatsCache) {
          return this.networkStatsCache.data;
        }

        return {
          volume24h: 0,
          activeContracts: 0,
          uniqueWallets: 0,
          txCount24h: 0,
          fees24h: 0,
          contractCalls24h: 0,
        };
      }
    })();

    this.networkStatsInFlight = request;
    try {
      return await request;
    } finally {
      if (this.networkStatsInFlight === request) {
        this.networkStatsInFlight = null;
      }
    }
  }

  async getSorobanProtocols(): Promise<Protocol[]> {
    if (this.protocolsCache && Date.now() - this.protocolsCache.timestamp < this.protocolCacheTtlMs) {
      return this.protocolsCache.data;
    }

    if (this.protocolsInFlight) {
      return this.protocolsInFlight;
    }

    const request = (async (): Promise<Protocol[]> => {
      try {
        const [
          aquariusResult,
          phoenixResult,
          templarResult,
          etherfuseResult,
          spikoResult,
          defiLlamaStellarProtocolsResult,
          defiLlamaStellarProtocolMetadataResult,
          assetMarketMapResult,
        ] = await Promise.allSettled([
          this.fetchAquariusMetrics(),
          this.fetchPhoenixMetrics(),
          this.fetchTemplarMetrics(),
          this.fetchEtherfuseMetrics(),
          this.fetchSpikoMetrics(),
          this.fetchDefiLlamaStellarProtocols(),
          this.fetchDefiLlamaStellarProtocolMetadata(),
          this.fetchAssetMarketMap(260),
        ]);

        const trustlineMap =
          assetMarketMapResult.status === 'fulfilled'
            ? assetMarketMapResult.value.trustlines
            : new Map<string, number>();
        const tokenChangeMap =
          assetMarketMapResult.status === 'fulfilled'
            ? assetMarketMapResult.value.change24hPercent
            : new Map<string, number>();
        const tokenMarketCapMap =
          assetMarketMapResult.status === 'fulfilled'
            ? assetMarketMapResult.value.marketCapUsd
            : new Map<string, number>();
        const tokenVolumeMap =
          assetMarketMapResult.status === 'fulfilled'
            ? assetMarketMapResult.value.volume24hUsd
            : new Map<string, number>();
        const protocolTvlMap =
          defiLlamaStellarProtocolMetadataResult.status === 'fulfilled'
            ? defiLlamaStellarProtocolMetadataResult.value.tvlByProtocol
            : new Map<string, number>();
        const protocolAuditedMap =
          defiLlamaStellarProtocolMetadataResult.status === 'fulfilled'
            ? defiLlamaStellarProtocolMetadataResult.value.auditedByProtocol
            : new Map<string, boolean>();
        if (defiLlamaStellarProtocolMetadataResult.status !== 'fulfilled') {
          this.warnWithCooldown(
            'protocol-defillama-stellar-metadata',
            `Failed to fetch DefiLlama Stellar protocol metadata map. ${this.errorMessage(defiLlamaStellarProtocolMetadataResult.reason)}`,
          );
        }
        if (assetMarketMapResult.status !== 'fulfilled') {
          this.warnWithCooldown(
            'protocol-asset-market-map',
            `Failed to fetch token trustline/change map. ${this.errorMessage(assetMarketMapResult.reason)}`,
          );
        }

        const protocols: Protocol[] = [];
        const addCuratedProtocol = (params: {
          id: string;
          name: string;
          slug: string;
          url: string;
          category: string;
          token: string;
          audited: boolean;
          tvl: number;
          apy: number;
        }): void => {
          const token = this.normalizeTokenSymbol(params.token);
          const tvl = this.toPositiveNumber(params.tvl);
          const apy = this.parseApyPercent(params.apy);
          protocols.push(
            this.buildProtocol({
              id: params.id,
              name: params.name,
              slug: params.slug,
              url: params.url,
              category: this.normalizeProtocolCategory(params.category),
              token,
              logo: this.resolveProtocolLogoUrl(params.slug),
              audited: this.resolveProtocolAuditedStatus(params.slug, params.audited, protocolAuditedMap),
              tvl,
              apy,
              users: this.resolveProtocolUsers(token, tvl, trustlineMap),
              change24h: this.resolveProtocolChange24h(token, tokenChangeMap),
            }),
          );
        };

        if (aquariusResult.status === 'fulfilled') {
          addCuratedProtocol({
            id: 'aquarius-stellar',
            name: 'Aquarius',
            slug: 'aquarius-stellar',
            url: 'https://aqua.network',
            category: 'dex',
            token: 'AQUA',
            audited: true,
            tvl: aquariusResult.value.tvlUsd,
            apy: aquariusResult.value.apyPercent,
          });
        } else {
          this.warnWithCooldown(
            'protocol-aquarius',
            `Failed to fetch Aquarius protocol metrics. ${this.errorMessage(aquariusResult.reason)}`,
          );
        }

        if (phoenixResult.status === 'fulfilled') {
          addCuratedProtocol({
            id: 'phoenix-defi-hub',
            name: 'Phoenix',
            slug: 'phoenix-defi-hub',
            url: 'https://www.phoenix-hub.io',
            category: 'dex',
            token: 'PHO',
            audited: true,
            tvl: phoenixResult.value.tvlUsd,
            apy: phoenixResult.value.apyPercent,
          });
        } else {
          this.warnWithCooldown(
            'protocol-phoenix',
            `Failed to fetch Phoenix protocol metrics. ${this.errorMessage(phoenixResult.reason)}`,
          );
        }

        if (templarResult.status === 'fulfilled') {
          addCuratedProtocol({
            id: 'templar-protocol',
            name: 'Templar',
            slug: 'templar-protocol',
            url: 'https://app.templarfi.org',
            category: 'lending',
            token: 'TMPL',
            audited: false,
            tvl: templarResult.value.tvlUsd,
            apy: templarResult.value.apyPercent,
          });
        } else {
          this.warnWithCooldown(
            'protocol-templar',
            `Failed to fetch Templar protocol metrics. ${this.errorMessage(templarResult.reason)}`,
          );
        }

        if (etherfuseResult.status === 'fulfilled') {
          addCuratedProtocol({
            id: 'etherfuse',
            name: 'Etherfuse',
            slug: 'etherfuse',
            url: 'https://app.etherfuse.com',
            category: 'rwa',
            token: 'ETHF',
            audited: false,
            tvl: etherfuseResult.value.tvlUsd,
            apy: etherfuseResult.value.apyPercent,
          });
        } else {
          this.warnWithCooldown(
            'protocol-etherfuse',
            `Failed to fetch Etherfuse protocol metrics. ${this.errorMessage(etherfuseResult.reason)}`,
          );
        }

        if (spikoResult.status === 'fulfilled') {
          addCuratedProtocol({
            id: 'spiko',
            name: 'Spiko',
            slug: 'spiko',
            url: 'https://www.spiko.io',
            category: 'rwa',
            token: 'EUTBL',
            audited: true,
            tvl: 0,
            apy: spikoResult.value.apyPercent,
          });
        } else {
          this.warnWithCooldown(
            'protocol-spiko',
            `Failed to fetch Spiko protocol metrics. ${this.errorMessage(spikoResult.reason)}`,
          );
        }

        if (defiLlamaStellarProtocolsResult.status === 'fulfilled') {
          protocols.push(...defiLlamaStellarProtocolsResult.value);
        } else {
          this.warnWithCooldown(
            'protocol-defillama-stellar',
            `Failed to fetch DefiLlama Stellar protocol metrics. ${this.errorMessage(defiLlamaStellarProtocolsResult.reason)}`,
          );
        }

        const deduped = new Map<string, Protocol>();
        for (const protocol of protocols) {
          const key = this.normalizeProtocolKey(protocol.slug || protocol.id || protocol.name);
          if (!key) {
            continue;
          }
          const existing = deduped.get(key);
          if (!existing) {
            deduped.set(key, protocol);
            continue;
          }

          deduped.set(key, {
            ...existing,
            id: protocol.id || existing.id,
            name: protocol.name || existing.name,
            slug: protocol.slug || existing.slug,
            url: protocol.url || existing.url,
            category: protocol.category || existing.category,
            tvl: this.roundTo4(Math.max(existing.tvl, protocol.tvl)),
            apy: this.roundTo4(Math.max(protocol.apy, existing.apy)),
            token: this.chooseProtocolToken(key, existing.token, protocol.token),
            audited: existing.audited || protocol.audited,
            users: Math.max(existing.users, protocol.users),
            logo: protocol.logo || existing.logo,
            change24h: this.roundTo4(protocol.change24h || existing.change24h || 0),
          });
        }

        const existingProtocols = Array.from(deduped.values());
        const tokenApyBaselines = this.buildTokenApyBaselineMap(existingProtocols);
        const categoryApyBaselines = this.buildCategoryApyBaselineMap(existingProtocols);
        const globalApyBaseline = this.resolveGlobalApyBaseline(existingProtocols);

        for (const fallback of FALLBACK_SOROBAN_PROTOCOLS) {
          const key = this.normalizeProtocolKey(fallback.slug);
          if (!key || deduped.has(key)) {
            continue;
          }

          const inferredApy = this.resolveFallbackProtocolApy({
            fallback,
            tokenBaselines: tokenApyBaselines,
            categoryBaselines: categoryApyBaselines,
            globalBaseline: globalApyBaseline,
          });
          const fallbackTvlFromProtocols = this.toPositiveNumber(protocolTvlMap.get(key));
          const fallbackTvlFromTokenActivity = this.estimateProtocolTvlFromTokenActivity(
            fallback.token,
            tokenMarketCapMap,
            tokenVolumeMap,
          );
          const fallbackTvl = Math.max(fallbackTvlFromProtocols, fallbackTvlFromTokenActivity);
          const fallbackUsers = this.resolveProtocolUsers(fallback.token, fallbackTvl, trustlineMap);
          const fallbackChange24h = this.resolveProtocolChange24hWithEstimation({
            tokenSymbol: fallback.token,
            tokenChange24hMap: tokenChangeMap,
            apy: inferredApy,
            tvlUsd: fallbackTvl,
          });
          if (
            fallbackUsers <= 0 &&
            fallbackTvl <= 0 &&
            Math.abs(fallbackChange24h) < 0.0001 &&
            inferredApy <= 0
          ) {
            continue;
          }

          deduped.set(
            key,
            this.buildProtocol({
              id: fallback.id,
              name: fallback.name,
              slug: fallback.slug,
              url: fallback.url,
              category: fallback.category,
              token: fallback.token,
              logo: this.resolveProtocolLogoUrl(fallback.slug),
              audited: this.resolveProtocolAuditedStatus(
                fallback.slug,
                fallback.audited,
                protocolAuditedMap,
              ),
              tvl: fallbackTvl,
              apy: inferredApy,
              users: fallbackUsers,
              change24h: fallbackChange24h,
            }),
          );
        }

        const fallbackKeys = new Set(
          FALLBACK_SOROBAN_PROTOCOLS.map((item) => this.normalizeProtocolKey(item.slug)),
        );

        const mapped = Array.from(deduped.values())
          .map((protocol) => {
            const protocolKey = this.normalizeProtocolKey(protocol.slug || protocol.id || protocol.name);
            const fallbackTvl = this.toPositiveNumber(protocolTvlMap.get(protocolKey));
            const directProtocolTvl = this.toPositiveNumber(protocol.tvl);
            const tokenActivityTvl =
              directProtocolTvl > 0 || fallbackTvl > 0
                ? 0
                : this.estimateProtocolTvlFromTokenActivity(
                    protocol.token,
                    tokenMarketCapMap,
                    tokenVolumeMap,
                  );
            const resolvedTvl = Math.max(directProtocolTvl, fallbackTvl, tokenActivityTvl);
            return {
              ...protocol,
              audited: this.resolveProtocolAuditedStatus(
                protocolKey,
                Boolean(protocol.audited),
                protocolAuditedMap,
              ),
              tvl: this.roundTo4(resolvedTvl),
              users: this.resolveProtocolUsers(protocol.token, resolvedTvl, trustlineMap),
              change24h: this.resolveProtocolChange24hWithEstimation({
                tokenSymbol: protocol.token,
                tokenChange24hMap: tokenChangeMap,
                apy: this.numberOr(protocol.apy, 0),
                tvlUsd: resolvedTvl,
                explicitChange24h: this.numberOr(protocol.change24h, 0),
              }),
            };
          })
          .filter((protocol) => {
            const key = this.normalizeProtocolKey(protocol.slug || protocol.id || protocol.name);
            if (fallbackKeys.has(key)) {
              return (
                protocol.tvl > 0 ||
                protocol.users > 0 ||
                Math.abs(this.numberOr(protocol.change24h, 0)) >= 0.0001 ||
                protocol.apy > 0
              );
            }
            return protocol.tvl > 0 || protocol.apy > 0 || protocol.users > 0;
          })
          .sort(
            (left, right) =>
              right.tvl - left.tvl || right.apy - left.apy || right.users - left.users || left.name.localeCompare(right.name),
          )
          .slice(0, 50);

        this.protocolsCache = {
          timestamp: Date.now(),
          data: mapped,
        };

        return mapped;
      } catch (error: unknown) {
        this.warnWithCooldown(
          'soroban-protocol-data',
          `Failed to fetch Soroban protocol data. ${this.errorMessage(error)}`,
        );

        if (this.protocolsCache) {
          return this.protocolsCache.data;
        }

        return [];
      }
    })();

    this.protocolsInFlight = request;
    try {
      return await request;
    } finally {
      if (this.protocolsInFlight === request) {
        this.protocolsInFlight = null;
      }
    }
  }

  async getTrendingAssets(): Promise<Asset[]> {
    if (this.trendingAssetsCache && Date.now() - this.trendingAssetsCache.timestamp < this.trendingAssetsCacheTtlMs) {
      const liveAdjustedCache = await this.applyLivePriceOverlay(this.trendingAssetsCache.data);
      this.trendingAssetsCache.data = liveAdjustedCache;
      return liveAdjustedCache;
    }

    if (this.trendingAssetsInFlight) {
      return this.trendingAssetsInFlight;
    }

    const request = (async (): Promise<Asset[]> => {
      try {
        const payload = await this.getJsonWithRetry(`${this.stellarExpertApi}/asset`, {
          retries: 2,
          params: {
            order: 'desc',
            limit: 160,
            cursor: 0,
          },
        });

        const records = this.extractAssetRecords(payload);
        const assets = records
          .map((record) => this.parseExpertAsset(record))
          .filter((record): record is TickerAsset => Boolean(record));

        const normalized = this.selectRepresentativeAssetsBySymbol(assets)
          .filter((asset) => asset.volume24hUsd > 500)
          .sort((a, b) => b.volume24hUsd - a.volume24hUsd)
          .slice(0, 40)
          .map((asset) => ({
            id: `${asset.assetCode}-${asset.issuer ?? 'native'}`,
            symbol: asset.assetCode,
            name: asset.displayName,
            price: asset.priceUsd,
            change24h: asset.change24hPercent,
            volume24h: asset.volume24hUsd,
            marketCap: asset.priceUsd > 0 ? this.roundTo4(asset.supplyTokens * asset.priceUsd) : 0,
            holders: asset.trustlines,
            issuer: asset.issuer,
          }));
        const liveAdjusted = await this.applyLivePriceOverlay(normalized);

        this.trendingAssetsCache = {
          timestamp: Date.now(),
          data: liveAdjusted,
        };

        return liveAdjusted;
      } catch (error: unknown) {
        if (this.trendingAssetsCache) {
          this.logger.debug(
            `Trending assets upstream unavailable; serving cached dataset. ${this.errorMessage(error)}`,
            'MarketService',
          );
          const liveAdjustedCache = await this.applyLivePriceOverlay(this.trendingAssetsCache.data);
          this.trendingAssetsCache.data = liveAdjustedCache;
          return liveAdjustedCache;
        }

        const fallback = await this.buildFallbackTrendingAssets();
        if (fallback.length > 0) {
          this.logger.debug(
            `Trending assets upstream unavailable; serving fallback protocol-derived dataset (${fallback.length} assets). ${this.errorMessage(error)}`,
            'MarketService',
          );
        } else {
          this.warnWithCooldown(
            'trending-assets',
            `Failed to fetch trending assets and fallback dataset is empty. ${this.errorMessage(error)}`,
          );
        }

        const liveAdjustedFallback = await this.applyLivePriceOverlay(fallback);
        this.trendingAssetsCache = {
          timestamp: Date.now(),
          data: liveAdjustedFallback,
        };
        return liveAdjustedFallback;
      }
    })();

    this.trendingAssetsInFlight = request;
    try {
      return await request;
    } finally {
      if (this.trendingAssetsInFlight === request) {
        this.trendingAssetsInFlight = null;
      }
    }
  }

  private async applyLivePriceOverlay(assets: Asset[]): Promise<Asset[]> {
    if (assets.length === 0) {
      return assets;
    }

    const symbols = [...new Set(assets.map((asset) => asset.symbol.trim().toUpperCase()).filter(Boolean))];
    if (symbols.length === 0) {
      return assets;
    }

    let quoteMap: Map<string, PriceQuote>;
    try {
      quoteMap = await this.priceFeedService.getQuotes(symbols);
    } catch (error: unknown) {
      this.logger.debug(`Live price overlay skipped: ${this.errorMessage(error)}`, 'MarketService');
      return assets;
    }

    const nowUnix = Math.floor(Date.now() / 1000);
    let updatedAny = false;
    const updatedAssets = assets.map((asset) => {
      const symbol = asset.symbol.trim().toUpperCase();
      const quote = quoteMap.get(symbol);
      if (!quote || !this.isLiveQuoteFresh(quote, nowUnix)) {
        return asset;
      }

      if (!Number.isFinite(quote.price) || quote.price <= 0 || quote.price === asset.price) {
        return asset;
      }

      const supplyEstimate = asset.price > 0 && asset.marketCap > 0 ? asset.marketCap / asset.price : 0;
      const marketCap = supplyEstimate > 0 ? this.roundTo4(supplyEstimate * quote.price) : asset.marketCap;
      updatedAny = true;
      return {
        ...asset,
        price: quote.price,
        marketCap,
      };
    });

    return updatedAny ? updatedAssets : assets;
  }

  private isLiveQuoteFresh(quote: PriceQuote, nowUnix: number): boolean {
    if (!Number.isFinite(quote.price) || quote.price <= 0) {
      return false;
    }

    if (!Number.isFinite(quote.publishTime) || quote.publishTime <= 0) {
      return true;
    }

    return nowUnix - quote.publishTime <= this.livePriceMaxAgeSeconds;
  }

  async getLiquidityPools(): Promise<LiquidityPool[]> {
    if (
      this.liquidityPoolsCache &&
      Date.now() - this.liquidityPoolsCache.timestamp < this.liquidityPoolsCacheTtlMs
    ) {
      return this.liquidityPoolsCache.data;
    }

    if (this.liquidityPoolsInFlight) {
      return this.liquidityPoolsInFlight;
    }

    const request = (async (): Promise<LiquidityPool[]> => {
      try {
        const [aquariusResult, phoenixResult, horizonResult] = await Promise.allSettled([
          this.fetchAquariusLiquidityPools(),
          this.fetchPhoenixLiquidityPools(),
          this.fetchHorizonLiquidityPoolsCatalog(120),
        ]);

        const merged = this.mergeLiquidityPools([
          ...(aquariusResult.status === 'fulfilled' ? aquariusResult.value : []),
          ...(phoenixResult.status === 'fulfilled' ? phoenixResult.value : []),
          ...(horizonResult.status === 'fulfilled' ? horizonResult.value : []),
        ]);

        if (aquariusResult.status !== 'fulfilled') {
          this.warnWithCooldown(
            'liquidity-pools-aquarius',
            `Failed to fetch Aquarius pools. ${this.errorMessage(aquariusResult.reason)}`,
          );
        }
        if (phoenixResult.status !== 'fulfilled') {
          this.warnWithCooldown(
            'liquidity-pools-phoenix',
            `Failed to fetch Phoenix pools. ${this.errorMessage(phoenixResult.reason)}`,
          );
        }
        if (horizonResult.status !== 'fulfilled') {
          this.warnWithCooldown(
            'liquidity-pools-horizon',
            `Failed to fetch Horizon pools. ${this.errorMessage(horizonResult.reason)}`,
          );
        }

        const normalized = merged
          .filter((pool) => pool.pair)
          .filter((pool) => {
            // Horizon catalog pools are a fallback source and often lack TVL/volume/APY pricing context.
            // Keep only pools with at least one tracked market metric so the table avoids zero-only noise.
            if (pool.source === 'horizon') {
              return pool.tvlUsd > 0 || pool.volume24hUsd > 0 || pool.apy > 0;
            }

            return pool.tvlUsd > 0 || pool.totalShares > 0;
          })
          .sort(
            (left, right) =>
              right.tvlUsd - left.tvlUsd ||
              right.volume24hUsd - left.volume24hUsd ||
              right.totalShares - left.totalShares,
          )
          .slice(0, 250);

        this.liquidityPoolsCache = {
          timestamp: Date.now(),
          data: normalized,
        };
        return normalized;
      } catch (error: unknown) {
        this.warnWithCooldown(
          'liquidity-pools',
          `Failed to build liquidity pools dataset. ${this.errorMessage(error)}`,
        );

        if (this.liquidityPoolsCache) {
          return this.liquidityPoolsCache.data;
        }
        return [];
      }
    })();

    this.liquidityPoolsInFlight = request;
    try {
      return await request;
    } finally {
      if (this.liquidityPoolsInFlight === request) {
        this.liquidityPoolsInFlight = null;
      }
    }
  }

  async getAccountSummary(address: string): Promise<StellarAccountSummary> {
    const normalizedAddress = this.stringOr(address, '').trim().toUpperCase();
    const invalidResponse = (
      horizonApi: string,
      sorobanRpcApi: string,
      sorobanLatestLedger?: number,
    ): StellarAccountSummary => ({
      accountId: normalizedAddress,
      exists: false,
      balances: [],
      horizon: horizonApi,
      sorobanRpc: sorobanRpcApi,
      ...(typeof sorobanLatestLedger === 'number' ? { sorobanLatestLedger } : {}),
    });

    if (!/^G[A-Z2-7]{55}$/.test(normalizedAddress)) {
      return invalidResponse(this.horizonApi, this.sorobanRpcApi);
    }

    const primaryLedgerPromise = this.getSorobanLatestLedgerSequence(this.sorobanRpcApi, {
      suppressWarning: true,
    });

    let primaryAccount: HorizonAccountResponse | null = null;
    let primaryError: unknown = null;
    try {
      primaryAccount = await this.getHorizonAccountByUrl(this.horizonApi, normalizedAddress);
    } catch (error: unknown) {
      primaryError = error;
    }

    if (primaryAccount) {
      const sorobanLatestLedger = await primaryLedgerPromise;
      return this.mapAccountSummary({
        account: primaryAccount,
        fallbackAccountId: normalizedAddress,
        horizonApi: this.horizonApi,
        sorobanRpcApi: this.sorobanRpcApi,
        sorobanLatestLedger,
      });
    }

    const tryFallback = this.fallbackHorizonApi !== this.horizonApi;
    let fallbackError: unknown = null;
    if (tryFallback) {
      const fallbackLedgerPromise = this.getSorobanLatestLedgerSequence(this.fallbackSorobanRpcApi, {
        suppressWarning: true,
      });
      try {
        const fallbackAccount = await this.getHorizonAccountByUrl(
          this.fallbackHorizonApi,
          normalizedAddress,
        );
        if (fallbackAccount) {
          this.warnWithCooldown(
            `account-summary-network-mismatch:${normalizedAddress}`,
            `Account ${normalizedAddress} not found on configured Horizon ${this.horizonApi}, but found on fallback ${this.fallbackHorizonApi}.`,
          );
          const fallbackLedger = await fallbackLedgerPromise;
          return this.mapAccountSummary({
            account: fallbackAccount,
            fallbackAccountId: normalizedAddress,
            horizonApi: this.fallbackHorizonApi,
            sorobanRpcApi: this.fallbackSorobanRpcApi,
            sorobanLatestLedger: fallbackLedger,
          });
        }
      } catch (error: unknown) {
        fallbackError = error;
      }
    }

    if (primaryError || fallbackError) {
      const errorMessages = [
        primaryError ? `primary=${this.errorMessage(primaryError)}` : null,
        fallbackError ? `fallback=${this.errorMessage(fallbackError)}` : null,
      ]
        .filter((message): message is string => Boolean(message))
        .join('; ');

      this.warnWithCooldown(
        `account-summary-upstream-error:${normalizedAddress}`,
        `Account summary lookup degraded for ${normalizedAddress}. Returning exists=false fallback. ${errorMessages}`,
      );
    }

    const sorobanLatestLedger = await primaryLedgerPromise;
    return invalidResponse(this.horizonApi, this.sorobanRpcApi, sorobanLatestLedger);
  }

  async getDeFiPositions(address: string): Promise<DeFiPositionsResponse> {
    const requestStartedAt = Date.now();
    const normalizedAddress = this.stringOr(address, '').trim().toUpperCase();
    const addressType = this.resolveDeFiQueryAddressType(normalizedAddress);
    const cacheKey = normalizedAddress || '__empty__';

    const cached = this.defiPositionsCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < this.defiPositionsCacheTtlMs) {
      return cached.data;
    }

    const inFlight = this.defiPositionsInFlight.get(cacheKey);
    if (inFlight) {
      return inFlight;
    }

    const request = (async (): Promise<DeFiPositionsResponse> => {
      const mongoSource: DeFiSourceHealth = {
        status: 'unavailable',
        latencyMs: 0,
        detail: 'Query not executed',
      };
      const horizonSource: DeFiSourceHealth = {
        status: 'unavailable',
        latencyMs: 0,
        detail: 'Query not executed',
      };
      const sorobanSource: DeFiSourceHealth = {
        status: 'unavailable',
        latencyMs: 0,
        detail: 'Query not executed',
      };

      const discrepancies: DeFiDataDiscrepancy[] = [];
      let rawPositions: RawDeFiPositionForQuery[] = [];
      const walletById = new Map<string, RawWalletForDeFi>();
      let horizonAccount: HorizonAccountResponse | null = null;
      let horizonAccountExists = false;

      if (addressType === 'invalid') {
        mongoSource.detail = 'Invalid address format. Expected Stellar G... or C... address.';
        horizonSource.detail = 'Skipped due to invalid address format.';
        sorobanSource.detail = 'Skipped due to invalid address format.';
      } else {
        const mongoStart = Date.now();
        try {
          const mongoResult = await this.fetchRawPositionsForAddress(normalizedAddress, addressType);
          rawPositions = mongoResult.positions;
          mongoResult.walletById.forEach((wallet, key) => walletById.set(key, wallet));
          mongoSource.status = 'ok';
          mongoSource.latencyMs = Date.now() - mongoStart;
          mongoSource.detail = `Loaded ${rawPositions.length} persisted positions from MongoDB.`;
        } catch (error: unknown) {
          mongoSource.status = 'unavailable';
          mongoSource.latencyMs = Date.now() - mongoStart;
          mongoSource.detail = this.errorMessage(error);
          discrepancies.push({
            code: 'MONGO_UNAVAILABLE',
            severity: 'critical',
            message: 'Position source database is unavailable. Returning empty result.',
          });
        }

        if (addressType === 'account') {
          const horizonStart = Date.now();
          try {
            horizonAccount = await this.getJsonWithRetry<HorizonAccountResponse>(
              `${this.horizonApi}/accounts/${normalizedAddress}`,
              {
                retries: 1,
              },
            );
            horizonAccountExists = true;
            horizonSource.status = 'ok';
            horizonSource.latencyMs = Date.now() - horizonStart;
            horizonSource.detail = 'Account exists on Horizon.';
          } catch (error: unknown) {
            if (axios.isAxiosError(error) && error.response?.status === 404) {
              horizonAccountExists = false;
              horizonSource.status = 'ok';
              horizonSource.latencyMs = Date.now() - horizonStart;
              horizonSource.detail = 'Account not found on Horizon.';
              if (rawPositions.length > 0) {
                discrepancies.push({
                  code: 'ACCOUNT_NOT_ON_HORIZON',
                  severity: 'warning',
                  message:
                    'Persisted positions exist, but the account is not currently found on Horizon.',
                });
              }
            } else {
              horizonAccountExists = false;
              horizonSource.status = 'unavailable';
              horizonSource.latencyMs = Date.now() - horizonStart;
              horizonSource.detail = this.errorMessage(error);
              discrepancies.push({
                code: 'HORIZON_UNAVAILABLE',
                severity: 'warning',
                message: 'Horizon account verification failed. Using persisted position records.',
              });
            }
          }
        } else {
          horizonSource.status = 'ok';
          horizonSource.detail = 'Not applicable for contract-address query.';
        }

        const sorobanStart = Date.now();
        try {
          const latestLedger = await this.getSorobanLatestLedgerSequence(this.sorobanRpcApi, {
            suppressWarning: true,
          });
          sorobanSource.latencyMs = Date.now() - sorobanStart;
          if (typeof latestLedger === 'number' && latestLedger > 0) {
            sorobanSource.status = 'ok';
            sorobanSource.detail = `Latest ledger ${latestLedger}.`;
          } else {
            sorobanSource.status = 'degraded';
            sorobanSource.detail =
              'Soroban RPC responded without latest-ledger sequence. Position query returned persisted data.';
            discrepancies.push({
              code: 'SOROBAN_DEGRADED',
              severity: 'warning',
              message: 'Soroban source did not provide latest-ledger confirmation.',
            });
          }
        } catch (error: unknown) {
          sorobanSource.status = 'unavailable';
          sorobanSource.latencyMs = Date.now() - sorobanStart;
          sorobanSource.detail = this.errorMessage(error);
          discrepancies.push({
            code: 'SOROBAN_UNAVAILABLE',
            severity: 'warning',
            message: 'Soroban source unavailable. Using persisted position records.',
          });
        }
      }

      let positions = rawPositions
        .map((position) => this.normalizeDeFiPosition(position, walletById))
        .filter((position): position is DeFiPositionRecord => Boolean(position));

      if (
        addressType === 'account' &&
        horizonSource.status === 'ok' &&
        horizonAccountExists
      ) {
        const liveFallbackStart = Date.now();
        try {
          const liveFallbackPositions = await this.buildLiveFallbackPositionsFromHorizon(
            normalizedAddress,
            horizonAccount || undefined,
          );
          if (liveFallbackPositions.length > 0) {
            if (positions.length === 0) {
              positions = liveFallbackPositions;
              discrepancies.push({
                code: 'LIVE_FALLBACK_USED',
                severity: 'info',
                message:
                  `Mongo has no persisted positions for this account. Returned ${liveFallbackPositions.length} live fallback positions derived from Horizon balances and liquidity pools.`,
              });
              horizonSource.detail = `${horizonSource.detail} Live fallback generated in ${Date.now() - liveFallbackStart}ms.`;
            } else {
              const mergedPositions = this.mergePersistedAndLiveFallbackPositions(
                positions,
                liveFallbackPositions,
              );
              const addedCount = Math.max(0, mergedPositions.length - positions.length);
              if (addedCount > 0) {
                positions = mergedPositions;
                discrepancies.push({
                  code: 'LIVE_FALLBACK_AUGMENTED',
                  severity: 'info',
                  message:
                    `Added ${addedCount} live fallback positions from Horizon balances to complement ${positions.length - addedCount} persisted Mongo positions.`,
                });
                horizonSource.detail = `${horizonSource.detail} Live fallback augmentation added ${addedCount} positions in ${Date.now() - liveFallbackStart}ms.`;
              }
            }
          }
        } catch (error: unknown) {
          discrepancies.push({
            code: 'LIVE_FALLBACK_FAILED',
            severity: 'warning',
            message: `Live fallback failed and returned no positions. ${this.errorMessage(error)}`,
          });
        }
      }

      const horizonTotalAssetBalanceUsd = await this.computeHorizonTotalBalanceUsd(
        horizonAccount?.balances,
      );
      const summary = this.computeDeFiSummary(
        positions,
        horizonAccount?.balances,
        horizonTotalAssetBalanceUsd,
      );

      if (mongoSource.status === 'ok' && positions.length === 0) {
        discrepancies.push({
          code: 'NO_POSITIONS_FOUND',
          severity: 'info',
          message:
            'No active persisted DeFi positions were found for the query. This can occur for fresh wallets/contracts or unsupported protocols.',
        });
      }

      const status = this.resolveAvailabilityStatus({
        mongo: mongoSource.status,
        horizon: horizonSource.status,
        soroban: sorobanSource.status,
      });

      const response: DeFiPositionsResponse = {
        query: {
          input: this.stringOr(address, ''),
          normalizedAddress,
          addressType,
        },
        generatedAt: new Date().toISOString(),
        latencyMs: Date.now() - requestStartedAt,
        availability: {
          status,
          sources: {
            mongo: mongoSource,
            horizon: horizonSource,
            sorobanRpc: sorobanSource,
          },
          fallbackPolicy: {
            onPositionDataUnavailable:
              'If Mongo has no positions but Horizon is available, API derives live fallback positions from balances and liquidity pools; otherwise returns persisted MongoDB positions (if any) and marks degraded/unavailable.',
            onSourceDisagreement:
              'When source states differ, persisted position math remains authoritative for totals and discrepancies are attached for clients.',
          },
          discrepancies,
        },
        summary,
        positions,
      };

      this.defiPositionsCache.set(cacheKey, {
        timestamp: Date.now(),
        data: response,
      });

      return response;
    })();

    this.defiPositionsInFlight.set(cacheKey, request);
    try {
      return await request;
    } finally {
      if (this.defiPositionsInFlight.get(cacheKey) === request) {
        this.defiPositionsInFlight.delete(cacheKey);
      }
    }
  }

  private async buildFallbackTrendingAssets(): Promise<Asset[]> {
    try {
      const protocols = await this.getSorobanProtocols();
      return protocols.slice(0, 10).map((protocol, index) => ({
        id: protocol.slug || protocol.id || `protocol-${index}`,
        symbol: this.stringOr(protocol.token, 'DEFI').toUpperCase(),
        name: protocol.name || `Protocol ${index + 1}`,
        price: 0,
        change24h: protocol.change24h,
        volume24h: protocol.tvl,
        marketCap: protocol.tvl,
      }));
    } catch {
      return [];
    }
  }

  private async getSorobanLatestLedgerSequence(
    sorobanRpcApi = this.sorobanRpcApi,
    options: { suppressWarning?: boolean } = {},
  ): Promise<number | undefined> {
    const payload = {
      jsonrpc: '2.0',
      id: 'market-service',
      method: 'getLatestLedger',
    };

    let lastError: unknown = null;

    for (let attempt = 0; attempt <= 1; attempt += 1) {
      try {
        const response = await axios.post<{ result?: { sequence?: number | string } }>(
          sorobanRpcApi,
          payload,
          {
            timeout: this.requestTimeoutMs,
            headers: {
              Accept: 'application/json',
              'Content-Type': 'application/json',
              'User-Agent': 'yielder-backend/market-service',
            },
          },
        );

        const sequence = this.numberOr(response.data?.result?.sequence, 0);
        return sequence > 0 ? sequence : undefined;
      } catch (error: unknown) {
        lastError = error;
        if (attempt >= 1 || !this.isRetryableRequestError(error)) {
          break;
        }
        const delayMs = 250 * (attempt + 1);
        await this.sleep(delayMs);
      }
    }

    if (!options.suppressWarning) {
      this.warnWithCooldown(
        `soroban-latest-ledger:${sorobanRpcApi}`,
        `Failed to fetch Soroban latest ledger from ${sorobanRpcApi}. ${this.errorMessage(lastError)}`,
      );
    }
    return undefined;
  }

  private async getHorizonAccountByUrl(
    horizonApi: string,
    normalizedAddress: string,
  ): Promise<HorizonAccountResponse | null> {
    try {
      return await this.getJsonWithRetry<HorizonAccountResponse>(
        `${horizonApi}/accounts/${normalizedAddress}`,
        { retries: 1 },
      );
    } catch (error: unknown) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        return null;
      }
      throw error;
    }
  }

  private mapAccountSummary(params: {
    account: HorizonAccountResponse;
    fallbackAccountId: string;
    horizonApi: string;
    sorobanRpcApi: string;
    sorobanLatestLedger?: number;
  }): StellarAccountSummary {
    const balances = this.mapHorizonBalances(params.account.balances);

    return {
      accountId: this.stringOr(params.account.account_id, params.fallbackAccountId),
      exists: true,
      sequence: this.stringOr(params.account.sequence, '0'),
      subentryCount: this.numberOr(params.account.subentry_count, 0),
      lastModifiedLedger: this.numberOr(params.account.last_modified_ledger, 0),
      balances,
      horizon: params.horizonApi,
      sorobanRpc: params.sorobanRpcApi,
      ...(typeof params.sorobanLatestLedger === 'number'
        ? { sorobanLatestLedger: params.sorobanLatestLedger }
        : {}),
    };
  }

  private mapHorizonBalances(records: unknown): StellarAccountBalance[] {
    if (!Array.isArray(records)) {
      return [];
    }

    const mapped = (records as HorizonBalanceRecord[])
      .map((record) => {
        const assetType = this.stringOr(record.asset_type, 'unknown');
        const balance = this.stringOr(record.balance, '0');
        const limit = this.optionalString(record.limit);
        const assetCode =
          assetType === 'native'
            ? 'XLM'
            : assetType === 'liquidity_pool_shares'
              ? this.stringOr(record.liquidity_pool_id, 'LP-SHARES')
              : this.stringOr(record.asset_code, assetType.toUpperCase());

        return {
          assetType,
          assetCode,
          ...(this.optionalString(record.asset_issuer) ? { assetIssuer: this.optionalString(record.asset_issuer) } : {}),
          balance,
          ...(limit ? { limit } : {}),
        } satisfies StellarAccountBalance;
      })
      .sort((left, right) => {
        if (left.assetCode === 'XLM') return -1;
        if (right.assetCode === 'XLM') return 1;

        const leftBalance = this.numberOr(left.balance, 0);
        const rightBalance = this.numberOr(right.balance, 0);
        return rightBalance - leftBalance;
      });

    return mapped;
  }

  private resolveDeFiQueryAddressType(value: string): DeFiQueryAddressType {
    if (/^G[A-Z2-7]{55}$/.test(value)) {
      return 'account';
    }
    if (/^C[A-Z2-7]{55}$/.test(value)) {
      return 'contract';
    }
    return 'invalid';
  }

  private async fetchRawPositionsForAddress(
    normalizedAddress: string,
    addressType: DeFiQueryAddressType,
  ): Promise<{
    positions: RawDeFiPositionForQuery[];
    walletById: Map<string, RawWalletForDeFi>;
  }> {
    if (addressType === 'invalid') {
      return { positions: [], walletById: new Map<string, RawWalletForDeFi>() };
    }

    const walletById = new Map<string, RawWalletForDeFi>();

    if (addressType === 'account') {
      const wallets = (await this.walletModel
        .find({
          isArchived: false,
          $or: [{ address: normalizedAddress }, { publicKey: normalizedAddress }],
        } as any)
        .select('_id address publicKey chain')
        .lean()
        .exec()) as RawWalletForDeFi[];

      if (wallets.length === 0) {
        return { positions: [], walletById };
      }

      const walletIds = wallets.map((wallet) => wallet._id);
      for (const wallet of wallets) {
        walletById.set(String(wallet._id), wallet);
      }

      const positions = (await this.defiPositionModel
        .find({
          walletId: { $in: walletIds },
          status: { $ne: 'closed' },
        } as any)
        .lean()
        .exec()) as RawDeFiPositionForQuery[];

      return {
        positions,
        walletById,
      };
    }

    const contractFilters = [
      { assetId: { $regex: normalizedAddress, $options: 'i' } },
      { 'principal.contractId': normalizedAddress },
      { 'principal.poolContractId': normalizedAddress },
      { 'principal.backstopContractId': normalizedAddress },
      { 'currentValue.contractId': normalizedAddress },
      { 'currentValue.poolContractId': normalizedAddress },
      { 'currentValue.backstopContractId': normalizedAddress },
    ];

    const positions = (await this.defiPositionModel
      .find({
        status: { $ne: 'closed' },
        $or: contractFilters,
      } as any)
      .limit(400)
      .lean()
      .exec()) as RawDeFiPositionForQuery[];

    const walletIds = positions
      .map((position) => this.toIdentifierString(position.walletId))
      .filter((walletId): walletId is string => Boolean(walletId));

    if (walletIds.length > 0) {
      const wallets = (await this.walletModel
        .find({ _id: { $in: walletIds } } as any)
        .select('_id address publicKey chain')
        .lean()
        .exec()) as RawWalletForDeFi[];
      for (const wallet of wallets) {
        walletById.set(String(wallet._id), wallet);
      }
    }

    return {
      positions,
      walletById,
    };
  }

  private normalizeDeFiPosition(
    position: RawDeFiPositionForQuery,
    walletById: Map<string, RawWalletForDeFi>,
  ): DeFiPositionRecord | null {
    const protocol = this.stringOr(position.protocol, '').trim().toLowerCase() || 'unknown';
    const positionType = this.stringOr(position.type, '').trim().toLowerCase() || 'unknown';
    const status = this.stringOr(position.status, '').trim().toLowerCase() || 'active';
    const assetId = this.stringOr(position.assetId, '').trim();
    const walletId = this.toIdentifierString(position.walletId);
    const wallet = walletId ? walletById.get(walletId) : undefined;
    const walletAddress = this.stringOr(wallet?.address || wallet?.publicKey, '');

    const totalDepositValue = this.extractNumericValue(position.principal);
    const currentFromPayload = this.extractNumericValue(position.currentValue);
    const totalCurrentValue = currentFromPayload > 0 ? currentFromPayload : totalDepositValue;

    const totalBorrowedValueRaw = this.extractBorrowedValue(position);
    const totalBorrowedValue =
      totalBorrowedValueRaw > 0 ? this.roundTo4(totalBorrowedValueRaw) : null;
    const totalCurrentReturnRaw = totalCurrentValue - totalDepositValue;
    const totalCurrentReturn =
      totalDepositValue > 0 || totalCurrentValue > 0 ? this.roundTo4(totalCurrentReturnRaw) : null;
    const healthFactorRaw = this.extractHealthFactor(position);
    const healthFactor = healthFactorRaw > 0 ? this.roundTo4(healthFactorRaw) : null;
    const apyRaw = this.extractFiniteNumber(position.apy);
    const apy = typeof apyRaw === 'number' ? this.roundTo4(apyRaw) : null;
    const unclaimedRewardsRaw = this.extractFiniteNumber(position.unclaimedRewards);
    const unclaimedRewards =
      typeof unclaimedRewardsRaw === 'number' ? this.roundTo4(unclaimedRewardsRaw) : null;

    const interestRateRaw = this.extractInterestRate(position);
    const interestRate = interestRateRaw > 0 ? this.roundTo4(interestRateRaw) : null;
    const collateralRatioRaw = this.extractCollateralRatio(position);
    const collateralRatio =
      collateralRatioRaw > 0 ? this.roundTo4(collateralRatioRaw) : null;

    const semantics = this.resolveProtocolSemantics(protocol, positionType, assetId);
    const assetDescriptor = this.resolveAssetDescriptor(assetId);
    const updatedAt = this.resolveTimestampIso(position.updatedAt || position.createdAt);
    const stale = this.isStaleTimestamp(updatedAt);
    const positionValueHistory = this.extractPositionValueHistory(position, totalCurrentValue, updatedAt);
    const totals: DeFiPositionTotals = {
      totalCurrentValue: this.roundTo4(totalCurrentValue),
      totalDepositValue: this.roundTo4(totalDepositValue),
      totalBorrowedValue,
      totalCurrentReturn,
      healthFactor,
    };
    const detailMetrics = this.buildPositionDetailMetrics({
      totals,
      apy,
      unclaimedRewards,
      interestRate,
      collateralRatio,
    });

    return {
      id: this.toIdentifierString(position._id) || `${protocol}:${assetId}:${updatedAt || 'unknown'}`,
      protocol,
      positionType,
      status,
      dataSource: 'persisted',
      assetId,
      ...(assetDescriptor.label ? { assetLabel: assetDescriptor.label } : {}),
      ...(assetDescriptor.pair ? { assetPair: assetDescriptor.pair } : {}),
      ...(walletAddress ? { walletAddress } : {}),
      semantics,
      totals,
      ...(typeof apy === 'number' ? { apy } : {}),
      ...(typeof unclaimedRewards === 'number' ? { unclaimedRewards } : {}),
      ...(detailMetrics.length > 0 ? { detailMetrics } : {}),
      ...(positionValueHistory.length > 0 ? { positionValueHistory } : {}),
      ...(updatedAt ? { updatedAt } : {}),
      stale,
    };
  }

  private mergePersistedAndLiveFallbackPositions(
    persisted: DeFiPositionRecord[],
    liveFallback: DeFiPositionRecord[],
  ): DeFiPositionRecord[] {
    if (liveFallback.length === 0) {
      return persisted;
    }

    const merged = [...persisted];
    const existingKeys = new Set<string>(
      persisted.map((position) => this.buildDeFiPositionUniquenessKey(position)),
    );

    for (const position of liveFallback) {
      const key = this.buildDeFiPositionUniquenessKey(position);
      if (existingKeys.has(key)) {
        continue;
      }
      existingKeys.add(key);
      merged.push(position);
    }

    return merged.sort(
      (left, right) =>
        right.totals.totalCurrentValue - left.totals.totalCurrentValue ||
        left.protocol.localeCompare(right.protocol),
    );
  }

  private buildDeFiPositionUniquenessKey(position: DeFiPositionRecord): string {
    const protocol = this.normalizeProtocolKey(position.protocol || 'unknown');
    const positionType = this.normalizeProtocolKey(position.positionType || 'unknown');
    const assetId = this.normalizeProtocolKey(
      position.assetId || position.assetPair || position.assetLabel || position.id || 'unknown',
    );
    return `${protocol}:${positionType}:${assetId}`;
  }

  private async buildLiveFallbackPositionsFromHorizon(
    normalizedAddress: string,
    accountHint?: HorizonAccountResponse,
  ): Promise<DeFiPositionRecord[]> {
    const account =
      accountHint ||
      (await this.getJsonWithRetry<HorizonAccountResponse>(
        `${this.horizonApi}/accounts/${normalizedAddress}`,
        { retries: 1 },
      ));
    const balances = Array.isArray(account.balances) ? (account.balances as HorizonBalanceRecord[]) : [];
    if (balances.length === 0) {
      return [];
    }

    const marketCache = new Map<string, Promise<LiveAssetMarketData | null>>();
    const getMarketData = async (assetCode: string, assetIssuer?: string): Promise<LiveAssetMarketData | null> => {
      const key = `${assetCode.toUpperCase()}:${assetIssuer || 'native'}`;
      const existing = marketCache.get(key);
      if (existing) {
        return existing;
      }

      const request = this.fetchExpertAssetMarketData(assetCode, assetIssuer);
      marketCache.set(key, request);
      return request;
    };

    const xlmMarket = await getMarketData('XLM');
    const xlmPriceUsd = this.toPositiveNumber(xlmMarket?.priceUsd);
    const nowIso = new Date().toISOString();
    const positions: DeFiPositionRecord[] = [];

    for (const balance of balances) {
      const assetType = this.stringOr(balance.asset_type, 'unknown').trim().toLowerCase();
      const amount = this.toPositiveNumber(balance.balance);
      if (amount <= 0) {
        continue;
      }

      if (assetType === 'liquidity_pool_shares') {
        const poolId = this.stringOr(balance.liquidity_pool_id, '').trim();
        if (!poolId) {
          continue;
        }

        const pool = await this.fetchHorizonLiquidityPool(poolId);
        if (!pool) {
          continue;
        }

        const lpPosition = await this.buildLiveLiquidityPoolPosition({
          normalizedAddress,
          nowIso,
          pool,
          lpShareAmount: amount,
          xlmPriceUsd,
          getMarketData,
        });

        if (lpPosition) {
          positions.push(lpPosition);
        }
        continue;
      }

      const assetCode =
        assetType === 'native'
          ? 'XLM'
          : this.stringOr(balance.asset_code, '').trim().toUpperCase();
      if (!assetCode) {
        continue;
      }

      const assetIssuer = this.optionalString(balance.asset_issuer);
      const assetId = assetIssuer ? `${assetCode}:${assetIssuer}` : assetCode;
      const market = await getMarketData(assetCode, assetIssuer);
      const priceUsd = this.toPositiveNumber(market?.priceUsd);
      const change24hPercent = this.numberOr(market?.change24hPercent, 0);
      const priceInXlm =
        assetCode === 'XLM' ? 1 : this.convertUsdToXlm(priceUsd, xlmPriceUsd);
      let totalCurrentValue =
        assetCode === 'XLM' ? amount : this.convertUsdToXlm(amount * priceUsd, xlmPriceUsd);
      if (totalCurrentValue <= 0) {
        totalCurrentValue = amount;
      }

      const { depositValue, currentReturn } = this.deriveLiveReturnComponents(
        totalCurrentValue,
        change24hPercent,
      );
      const protocolMeta = this.resolveLiveFallbackProtocol(assetCode, assetType);
      const semantics = this.resolveProtocolSemantics(protocolMeta.protocol, protocolMeta.positionType, assetId);
      const assetDescriptor = this.resolveAssetDescriptor(assetId);
      const totals: DeFiPositionTotals = {
        totalCurrentValue: this.roundTo4(totalCurrentValue),
        totalDepositValue: this.roundTo4(depositValue),
        totalBorrowedValue: null,
        totalCurrentReturn: typeof currentReturn === 'number' ? this.roundTo4(currentReturn) : null,
        healthFactor: null,
      };
      const detailMetrics = this.buildPositionDetailMetrics({
        totals,
        apy: null,
        unclaimedRewards: null,
        interestRate: null,
        collateralRatio: null,
      });
      detailMetrics.push({
        key: 'tokenAmount',
        label: 'Token Amount',
        value: this.roundTo4(amount),
        format: 'number',
      });
      if (priceInXlm > 0) {
        detailMetrics.push({
          key: 'priceXlm',
          label: 'Price (XLM)',
          value: this.roundTo4(priceInXlm),
          format: 'number',
        });
      }
      detailMetrics.push({
        key: 'change24h',
        label: '24h Change',
        value: this.roundTo4(change24hPercent),
        format: 'percent',
      });
      const positionValueHistory = this.buildSyntheticHistoryFromChange(totalCurrentValue, change24hPercent, nowIso);

      positions.push({
        id: `live:${assetId}`,
        protocol: protocolMeta.protocol,
        positionType: protocolMeta.positionType,
        status: 'active',
        dataSource: 'live_fallback',
        assetId,
        ...(assetDescriptor.label ? { assetLabel: assetDescriptor.label } : {}),
        ...(assetDescriptor.pair ? { assetPair: assetDescriptor.pair } : {}),
        walletAddress: normalizedAddress,
        semantics,
        totals,
        detailMetrics,
        ...(positionValueHistory.length > 0 ? { positionValueHistory } : {}),
        updatedAt: nowIso,
        stale: false,
      });
    }

    return positions
      .sort((left, right) => right.totals.totalCurrentValue - left.totals.totalCurrentValue)
      .slice(0, 80);
  }

  private async buildLiveLiquidityPoolPosition(params: {
    normalizedAddress: string;
    nowIso: string;
    pool: HorizonLiquidityPoolResponse;
    lpShareAmount: number;
    xlmPriceUsd: number;
    getMarketData: (assetCode: string, assetIssuer?: string) => Promise<LiveAssetMarketData | null>;
  }): Promise<DeFiPositionRecord | null> {
    const totalShares = this.toPositiveNumber(params.pool.total_shares);
    if (totalShares <= 0 || params.lpShareAmount <= 0) {
      return null;
    }

    const shareRatio = params.lpShareAmount / totalShares;
    if (!Number.isFinite(shareRatio) || shareRatio <= 0) {
      return null;
    }

    const reserves = Array.isArray(params.pool.reserves)
      ? (params.pool.reserves as HorizonLiquidityPoolReserve[])
      : [];
    if (reserves.length === 0) {
      return null;
    }

    let totalPoolUsd = 0;
    let weightedChangeSum = 0;
    let weightedChangeDenominator = 0;
    const reserveCodes: string[] = [];
    const reserveMetrics: DeFiPositionDetailMetric[] = [];

    for (const reserve of reserves.slice(0, 4)) {
      const reserveAsset = this.parseHorizonAssetReference(this.stringOr(reserve.asset, ''));
      if (!reserveAsset) {
        continue;
      }

      const reserveAmount = this.toPositiveNumber(reserve.amount);
      if (reserveAmount <= 0) {
        continue;
      }

      reserveCodes.push(reserveAsset.assetCode);
      const market = await params.getMarketData(reserveAsset.assetCode, reserveAsset.assetIssuer);
      const reservePriceUsd =
        reserveAsset.assetCode === 'XLM'
          ? params.xlmPriceUsd
          : this.toPositiveNumber(market?.priceUsd);
      const reserveValueUsd = reservePriceUsd > 0 ? reserveAmount * reservePriceUsd : 0;
      if (reserveValueUsd > 0) {
        totalPoolUsd += reserveValueUsd;
        weightedChangeSum += this.numberOr(market?.change24hPercent, 0) * reserveValueUsd;
        weightedChangeDenominator += reserveValueUsd;
      }

      const userReserveAmount = reserveAmount * shareRatio;
      if (userReserveAmount > 0) {
        reserveMetrics.push({
          key: `reserve_${reserveAsset.assetCode.toLowerCase()}`,
          label: `${reserveAsset.assetCode} Amount`,
          value: this.roundTo4(userReserveAmount),
          format: 'number',
        });
      }
    }

    const pair = reserveCodes.slice(0, 2).join('/');
    const protocolMeta = this.resolveLiveFallbackProtocol(pair || 'LP', 'liquidity_pool_shares');
    const change24hPercent =
      weightedChangeDenominator > 0 ? weightedChangeSum / weightedChangeDenominator : 0;
    let totalCurrentValue = this.convertUsdToXlm(totalPoolUsd * shareRatio, params.xlmPriceUsd);
    if (totalCurrentValue <= 0) {
      totalCurrentValue = params.lpShareAmount;
    }

    const { depositValue, currentReturn } = this.deriveLiveReturnComponents(
      totalCurrentValue,
      change24hPercent,
    );
    const totals: DeFiPositionTotals = {
      totalCurrentValue: this.roundTo4(totalCurrentValue),
      totalDepositValue: this.roundTo4(depositValue),
      totalBorrowedValue: null,
      totalCurrentReturn: typeof currentReturn === 'number' ? this.roundTo4(currentReturn) : null,
      healthFactor: null,
    };
    const detailMetrics = this.buildPositionDetailMetrics({
      totals,
      apy: null,
      unclaimedRewards: null,
      interestRate: null,
      collateralRatio: null,
    });
    detailMetrics.push({
      key: 'lpShares',
      label: 'LP Shares',
      value: this.roundTo4(params.lpShareAmount),
      format: 'number',
    });
    detailMetrics.push({
      key: 'poolShare',
      label: 'Pool Share',
      value: this.roundTo4(shareRatio * 100),
      format: 'percent',
    });
    detailMetrics.push({
      key: 'feeBps',
      label: 'Pool Fee (bps)',
      value: this.roundTo4(this.toPositiveNumber(params.pool.fee_bp)),
      format: 'number',
    });
    detailMetrics.push({
      key: 'change24h',
      label: '24h Change',
      value: this.roundTo4(change24hPercent),
      format: 'percent',
    });
    detailMetrics.push(...reserveMetrics);
    const positionValueHistory = this.buildSyntheticHistoryFromChange(
      totalCurrentValue,
      change24hPercent,
      params.nowIso,
    );
    const assetId = pair ? `${pair}:LP:${this.stringOr(params.pool.id, 'unknown')}` : `LP:${this.stringOr(params.pool.id, 'unknown')}`;
    const semantics = this.resolveProtocolSemantics(
      protocolMeta.protocol,
      protocolMeta.positionType,
      assetId,
    );
    const assetDescriptor = this.resolveAssetDescriptor(assetId);

    return {
      id: `live:lp:${this.stringOr(params.pool.id, 'unknown')}`,
      protocol: protocolMeta.protocol,
      positionType: protocolMeta.positionType,
      status: 'active',
      dataSource: 'live_fallback',
      assetId,
      ...(assetDescriptor.label ? { assetLabel: assetDescriptor.label } : {}),
      ...(assetDescriptor.pair ? { assetPair: assetDescriptor.pair } : {}),
      walletAddress: params.normalizedAddress,
      semantics,
      totals,
      detailMetrics,
      ...(positionValueHistory.length > 0 ? { positionValueHistory } : {}),
      updatedAt: params.nowIso,
      stale: false,
    };
  }

  private resolveLiveFallbackProtocol(assetCodeOrPair: string, assetType: string): {
    protocol: string;
    positionType: string;
  } {
    const normalized = assetCodeOrPair.trim().toUpperCase();

    if (assetType === 'liquidity_pool_shares') {
      if (normalized.includes('AQUA')) {
        return { protocol: 'aquarius', positionType: 'liquidity_pool' };
      }
      if (normalized.includes('BLND')) {
        return { protocol: 'blend', positionType: 'liquidity_pool' };
      }
      return { protocol: 'soroswap', positionType: 'liquidity_pool' };
    }

    if (normalized === 'BLND') {
      return { protocol: 'blend', positionType: 'lending' };
    }
    if (normalized === 'AQUA') {
      return { protocol: 'aquarius', positionType: 'rewards' };
    }
    if (normalized === 'YBX' || normalized === 'SORO') {
      return { protocol: 'soroswap', positionType: 'liquidity' };
    }
    if (normalized === 'USDC' || normalized === 'EURC' || normalized === 'FUSD') {
      return { protocol: 'fxdao', positionType: 'stablecoin' };
    }
    if (normalized === 'XLM') {
      return { protocol: 'stellar', positionType: 'wallet_balance' };
    }
    return { protocol: 'stellar', positionType: 'token_holding' };
  }

  private async fetchExpertAssetMarketData(
    assetCode: string,
    assetIssuer?: string,
  ): Promise<LiveAssetMarketData | null> {
    const code = assetCode.trim().toUpperCase();
    if (!code) {
      return null;
    }

    if (code !== 'XLM' && !assetIssuer) {
      return null;
    }

    const identifier = code === 'XLM' ? 'XLM' : `${code}-${assetIssuer}`;
    try {
      const payload = await this.getJsonWithRetry(`${this.stellarExpertApi}/asset/${encodeURIComponent(identifier)}`, {
        retries: 1,
      });
      const parsed = this.parseExpertAsset(payload);
      if (!parsed) {
        return null;
      }

      return {
        code: parsed.assetCode.toUpperCase(),
        ...(parsed.issuer ? { issuer: parsed.issuer } : {}),
        priceUsd: this.toPositiveNumber(parsed.priceUsd),
        change24hPercent: this.roundTo4(parsed.change24hPercent),
      };
    } catch {
      return null;
    }
  }

  private async fetchHorizonLiquidityPool(poolId: string): Promise<HorizonLiquidityPoolResponse | null> {
    if (!poolId.trim()) {
      return null;
    }
    try {
      return await this.getJsonWithRetry<HorizonLiquidityPoolResponse>(
        `${this.horizonApi}/liquidity_pools/${encodeURIComponent(poolId)}`,
        { retries: 1 },
      );
    } catch {
      return null;
    }
  }

  private async computeHorizonTotalBalanceUsd(
    balances?: HorizonBalanceRecord[],
  ): Promise<number | null> {
    if (!Array.isArray(balances) || balances.length === 0) {
      return null;
    }

    const positiveBalances = balances.filter((balance) => this.toPositiveNumber(balance.balance) > 0);
    if (positiveBalances.length === 0) {
      return 0;
    }

    const marketCache = new Map<string, Promise<LiveAssetMarketData | null>>();
    const getMarketData = async (assetCode: string, assetIssuer?: string): Promise<LiveAssetMarketData | null> => {
      const key = `${assetCode.toUpperCase()}:${assetIssuer || 'native'}`;
      const existing = marketCache.get(key);
      if (existing) {
        return existing;
      }

      const request = this.fetchExpertAssetMarketData(assetCode, assetIssuer);
      marketCache.set(key, request);
      return request;
    };

    const xlmMarket = await getMarketData('XLM');
    const xlmPriceUsd = this.toPositiveNumber(xlmMarket?.priceUsd);
    let totalBalanceUsd = 0;

    for (const balance of positiveBalances) {
      const amount = this.toPositiveNumber(balance.balance);
      if (amount <= 0) {
        continue;
      }

      const assetType = this.stringOr(balance.asset_type, 'unknown').trim().toLowerCase();

      if (assetType === 'liquidity_pool_shares') {
        const poolId = this.stringOr(balance.liquidity_pool_id, '').trim();
        if (!poolId) {
          continue;
        }
        const pool = await this.fetchHorizonLiquidityPool(poolId);
        if (!pool) {
          continue;
        }
        const lpShareValueUsd = await this.computeLiquidityPoolShareUsd({
          pool,
          lpShareAmount: amount,
          xlmPriceUsd,
          getMarketData,
        });
        if (lpShareValueUsd > 0) {
          totalBalanceUsd += lpShareValueUsd;
        }
        continue;
      }

      const assetCode =
        assetType === 'native'
          ? 'XLM'
          : this.stringOr(balance.asset_code, '').trim().toUpperCase();
      if (!assetCode) {
        continue;
      }

      const assetIssuer = this.optionalString(balance.asset_issuer);
      const market = assetCode === 'XLM' ? xlmMarket : await getMarketData(assetCode, assetIssuer);
      const priceUsd =
        assetCode === 'XLM'
          ? xlmPriceUsd
          : this.toPositiveNumber(market?.priceUsd);

      if (priceUsd > 0) {
        totalBalanceUsd += amount * priceUsd;
      }
    }

    if (!Number.isFinite(totalBalanceUsd) || totalBalanceUsd <= 0) {
      return 0;
    }

    return this.roundTo4(totalBalanceUsd);
  }

  private async computeLiquidityPoolShareUsd(params: {
    pool: HorizonLiquidityPoolResponse;
    lpShareAmount: number;
    xlmPriceUsd: number;
    getMarketData: (assetCode: string, assetIssuer?: string) => Promise<LiveAssetMarketData | null>;
  }): Promise<number> {
    const totalShares = this.toPositiveNumber(params.pool.total_shares);
    if (totalShares <= 0 || params.lpShareAmount <= 0) {
      return 0;
    }

    const shareRatio = params.lpShareAmount / totalShares;
    if (!Number.isFinite(shareRatio) || shareRatio <= 0) {
      return 0;
    }

    const reserves = Array.isArray(params.pool.reserves)
      ? (params.pool.reserves as HorizonLiquidityPoolReserve[])
      : [];
    if (reserves.length === 0) {
      return 0;
    }

    let totalPoolUsd = 0;
    for (const reserve of reserves.slice(0, 4)) {
      const reserveAsset = this.parseHorizonAssetReference(this.stringOr(reserve.asset, ''));
      if (!reserveAsset) {
        continue;
      }

      const reserveAmount = this.toPositiveNumber(reserve.amount);
      if (reserveAmount <= 0) {
        continue;
      }

      const market = await params.getMarketData(reserveAsset.assetCode, reserveAsset.assetIssuer);
      const reservePriceUsd =
        reserveAsset.assetCode === 'XLM'
          ? params.xlmPriceUsd
          : this.toPositiveNumber(market?.priceUsd);

      if (reservePriceUsd > 0) {
        totalPoolUsd += reserveAmount * reservePriceUsd;
      }
    }

    const userShareUsd = totalPoolUsd * shareRatio;
    if (!Number.isFinite(userShareUsd) || userShareUsd <= 0) {
      return 0;
    }

    return userShareUsd;
  }

  private parseHorizonAssetReference(value: string): { assetCode: string; assetIssuer?: string } | null {
    const normalized = value.trim();
    if (!normalized) {
      return null;
    }

    if (normalized.toLowerCase() === 'native') {
      return { assetCode: 'XLM' };
    }

    const [assetCode, assetIssuer] = normalized.split(':');
    if (!assetCode) {
      return null;
    }

    const code = assetCode.trim().toUpperCase();
    const issuer = this.optionalString(assetIssuer);
    return issuer ? { assetCode: code, assetIssuer: issuer } : { assetCode: code };
  }

  private convertUsdToXlm(valueUsd: number, xlmPriceUsd: number): number {
    if (!Number.isFinite(valueUsd) || valueUsd <= 0) {
      return 0;
    }
    if (xlmPriceUsd > 0) {
      return valueUsd / xlmPriceUsd;
    }
    return valueUsd;
  }

  private deriveLiveReturnComponents(currentValue: number, change24hPercent: number): {
    depositValue: number;
    currentReturn: number | null;
  } {
    if (!Number.isFinite(currentValue) || currentValue <= 0) {
      return { depositValue: 0, currentReturn: null };
    }

    if (!Number.isFinite(change24hPercent) || change24hPercent === 0) {
      return { depositValue: currentValue, currentReturn: 0 };
    }

    const ratio = 1 + change24hPercent / 100;
    if (!Number.isFinite(ratio) || ratio <= 0.05 || ratio > 20) {
      return { depositValue: currentValue, currentReturn: 0 };
    }

    const depositValue = currentValue / ratio;
    const currentReturn = currentValue - depositValue;
    if (!Number.isFinite(depositValue) || !Number.isFinite(currentReturn)) {
      return { depositValue: currentValue, currentReturn: 0 };
    }

    return {
      depositValue,
      currentReturn,
    };
  }

  private buildSyntheticHistoryFromChange(
    currentValue: number,
    change24hPercent: number,
    timestampIso: string,
  ): DeFiPositionValuePoint[] {
    if (!Number.isFinite(currentValue) || currentValue <= 0) {
      return [];
    }

    const { depositValue } = this.deriveLiveReturnComponents(currentValue, change24hPercent);
    const now = new Date(timestampIso);
    if (!Number.isFinite(now.getTime())) {
      return [];
    }

    const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    return [
      {
        timestamp: oneDayAgo.toISOString(),
        value: this.roundTo4(depositValue > 0 ? depositValue : currentValue),
      },
      {
        timestamp: now.toISOString(),
        value: this.roundTo4(currentValue),
      },
    ];
  }

  private resolveProtocolSemantics(protocol: string, positionType: string, assetId: string): string {
    if (protocol.includes('blend')) {
      if (
        positionType.includes('backstop') ||
        assetId.toLowerCase().includes('backstop')
      ) {
        return 'blend.backstop';
      }
      if (positionType.includes('lend')) {
        return 'blend.lending';
      }
      if (positionType.includes('borrow')) {
        return 'blend.borrow';
      }
      return 'blend.generic';
    }

    if (protocol.includes('aquarius') || positionType.includes('liquidity')) {
      return 'amm.liquidity_pool';
    }

    if (positionType.includes('staking')) {
      return 'staking';
    }

    return `${protocol || 'unknown'}.${positionType || 'position'}`;
  }

  private resolveAssetDescriptor(assetId: string): { label?: string; pair?: string } {
    const normalized = this.stringOr(assetId, '').trim();
    if (!normalized) {
      return {};
    }

    const segment = normalized.split(':')[0].trim();
    if (!segment) {
      return {};
    }

    const pair = this.extractAssetPair(segment);
    if (pair) {
      return {
        label: pair,
        pair,
      };
    }

    return {
      label: segment.toUpperCase(),
    };
  }

  private extractAssetPair(value: string): string | null {
    const normalized = value.trim().replace(/_/g, '/');
    if (!normalized) {
      return null;
    }

    const slashParts = normalized
      .split('/')
      .map((part) => part.trim())
      .filter(Boolean);

    if (slashParts.length >= 2) {
      return `${slashParts[0].toUpperCase()}/${slashParts[1].toUpperCase()}`;
    }

    const dashParts = normalized
      .split('-')
      .map((part) => part.trim())
      .filter(Boolean)
      .filter((part) => !/^G[A-Z2-7]{55}$/.test(part));

    if (dashParts.length >= 2 && dashParts[0].length <= 12 && dashParts[1].length <= 12) {
      return `${dashParts[0].toUpperCase()}/${dashParts[1].toUpperCase()}`;
    }

    return null;
  }

  private extractInterestRate(position: RawDeFiPositionForQuery): number {
    const fromCurrent = this.extractByCandidateKeys(position.currentValue, [
      'interestRate',
      'borrowRate',
      'supplyRate',
      'apr',
      'apy',
      'yieldRate',
      'rate',
    ]);
    if (fromCurrent > 0) {
      return fromCurrent;
    }

    const fromPrincipal = this.extractByCandidateKeys(position.principal, [
      'interestRate',
      'borrowRate',
      'supplyRate',
      'apr',
      'apy',
      'yieldRate',
      'rate',
    ]);
    if (fromPrincipal > 0) {
      return fromPrincipal;
    }

    return this.toPositiveNumber(position.apy);
  }

  private extractCollateralRatio(position: RawDeFiPositionForQuery): number {
    const fromCurrent = this.extractByCandidateKeys(position.currentValue, [
      'collateralRatio',
      'collateralizationRatio',
      'loanToValue',
      'ltv',
    ]);
    if (fromCurrent > 0) {
      return fromCurrent;
    }

    return this.extractByCandidateKeys(position.principal, [
      'collateralRatio',
      'collateralizationRatio',
      'loanToValue',
      'ltv',
    ]);
  }

  private buildPositionDetailMetrics(params: {
    totals: DeFiPositionTotals;
    apy: number | null;
    unclaimedRewards: number | null;
    interestRate: number | null;
    collateralRatio: number | null;
  }): DeFiPositionDetailMetric[] {
    const metrics: DeFiPositionDetailMetric[] = [
      {
        key: 'totalCurrentValue',
        label: 'Current Value',
        value: params.totals.totalCurrentValue,
        format: 'number',
      },
      {
        key: 'totalDepositValue',
        label: 'Deposit Value',
        value: params.totals.totalDepositValue,
        format: 'number',
      },
    ];

    if (typeof params.totals.totalBorrowedValue === 'number') {
      metrics.push({
        key: 'totalBorrowedValue',
        label: 'Borrowed Value',
        value: params.totals.totalBorrowedValue,
        format: 'number',
      });
    }

    if (typeof params.totals.totalCurrentReturn === 'number') {
      metrics.push({
        key: 'totalCurrentReturn',
        label: 'Current Return',
        value: params.totals.totalCurrentReturn,
        format: 'number',
      });
    }

    if (typeof params.totals.healthFactor === 'number') {
      metrics.push({
        key: 'healthFactor',
        label: 'Health Factor',
        value: params.totals.healthFactor,
        format: 'ratio',
      });
    }

    if (typeof params.collateralRatio === 'number') {
      metrics.push({
        key: 'collateralRatio',
        label: 'Collateral Ratio',
        value: params.collateralRatio,
        format: 'ratio',
      });
    }

    if (typeof params.interestRate === 'number') {
      metrics.push({
        key: 'interestRate',
        label: 'Interest Rate',
        value: params.interestRate,
        format: 'percent',
      });
    }

    if (typeof params.apy === 'number') {
      metrics.push({
        key: 'apy',
        label: 'APY',
        value: params.apy,
        format: 'percent',
      });
    }

    if (typeof params.unclaimedRewards === 'number') {
      metrics.push({
        key: 'unclaimedRewards',
        label: 'Unclaimed Rewards',
        value: params.unclaimedRewards,
        format: 'number',
      });
    }

    return metrics;
  }

  private extractBorrowedValue(position: RawDeFiPositionForQuery): number {
    const borrowedFromCurrent = this.extractByCandidateKeys(position.currentValue, [
      'borrowed',
      'borrowedValue',
      'borrowValue',
      'debt',
      'debtValue',
      'liability',
      'liabilities',
    ]);
    if (borrowedFromCurrent > 0) {
      return borrowedFromCurrent;
    }

    return this.extractByCandidateKeys(position.principal, [
      'borrowed',
      'borrowedValue',
      'borrowValue',
      'debt',
      'debtValue',
      'liability',
      'liabilities',
    ]);
  }

  private extractHealthFactor(position: RawDeFiPositionForQuery): number {
    const fromCurrent = this.extractByCandidateKeys(position.currentValue, [
      'health',
      'healthFactor',
      'health_value',
      'hf',
    ]);
    if (fromCurrent > 0) {
      return fromCurrent;
    }

    return this.extractByCandidateKeys(position.principal, [
      'health',
      'healthFactor',
      'health_value',
      'hf',
    ]);
  }

  private extractByCandidateKeys(source: unknown, keys: string[]): number {
    const normalizedKeys = new Set(keys.map((key) => this.normalizeLookupKey(key)));
    return this.extractByCandidateKeysRecursive(source, normalizedKeys, 0);
  }

  private extractByCandidateKeysRecursive(source: unknown, keys: Set<string>, depth: number): number {
    if (depth > 6 || !source || typeof source !== 'object') {
      return 0;
    }

    if (Array.isArray(source)) {
      for (const item of source) {
        const nested = this.extractByCandidateKeysRecursive(item, keys, depth + 1);
        if (nested > 0) {
          return nested;
        }
      }
      return 0;
    }

    const record = source as Record<string, unknown>;

    for (const [key, value] of Object.entries(record)) {
      if (!keys.has(this.normalizeLookupKey(key))) {
        continue;
      }

      const parsed = this.extractNumericValue(value);
      if (parsed > 0) {
        return parsed;
      }
    }

    for (const value of Object.values(record)) {
      const nested = this.extractByCandidateKeysRecursive(value, keys, depth + 1);
      if (nested > 0) {
        return nested;
      }
    }

    return 0;
  }

  private normalizeLookupKey(value: string): string {
    return value.replace(/[^a-zA-Z0-9]/g, '').toLowerCase();
  }

  private extractNumericValue(source: unknown): number {
    if (typeof source === 'number') {
      return Number.isFinite(source) ? source : 0;
    }

    if (typeof source === 'string') {
      const parsed = Number(source);
      return Number.isFinite(parsed) ? parsed : 0;
    }

    if (Array.isArray(source)) {
      return source.reduce((sum, item) => sum + this.extractNumericValue(item), 0);
    }

    if (source && typeof source === 'object') {
      const record = source as Record<string, unknown>;
      if ('amount' in record) {
        return this.extractNumericValue(record.amount);
      }
      if ('value' in record) {
        return this.extractNumericValue(record.value);
      }
      if ('total' in record) {
        return this.extractNumericValue(record.total);
      }
      return Object.values(record).reduce<number>(
        (sum, item) => sum + this.extractNumericValue(item),
        0,
      );
    }

    return 0;
  }

  private extractPositionValueHistory(
    position: RawDeFiPositionForQuery,
    currentValue: number,
    timestampIso?: string,
  ): DeFiPositionValuePoint[] {
    const fromCurrent = this.extractHistoryArray(position.currentValue);
    if (fromCurrent.length > 0) {
      return fromCurrent;
    }

    const fromPrincipal = this.extractHistoryArray(position.principal);
    if (fromPrincipal.length > 0) {
      return fromPrincipal;
    }

    if (timestampIso && currentValue > 0) {
      return [{ timestamp: timestampIso, value: this.roundTo4(currentValue) }];
    }

    return [];
  }

  private extractHistoryArray(source: unknown): DeFiPositionValuePoint[] {
    if (!source || typeof source !== 'object' || Array.isArray(source)) {
      return [];
    }

    const record = source as Record<string, unknown>;
    const history = record.history;
    if (!Array.isArray(history)) {
      return [];
    }

    const points = history
      .map((entry) => {
        if (!entry || typeof entry !== 'object') {
          return null;
        }
        const row = entry as Record<string, unknown>;
        const value = this.extractNumericValue(row.value ?? row.amount ?? row.total);
        const timestamp =
          this.resolveTimestampIso(row.timestamp ?? row.date ?? row.time) || undefined;
        if (!timestamp || !Number.isFinite(value)) {
          return null;
        }
        return { timestamp, value: this.roundTo4(value) };
      })
      .filter((entry): entry is DeFiPositionValuePoint => Boolean(entry))
      .sort((left, right) => left.timestamp.localeCompare(right.timestamp));

    return points;
  }

  private computeDeFiSummary(
    positions: DeFiPositionRecord[],
    horizonBalances?: HorizonBalanceRecord[],
    horizonTotalAssetBalanceUsd?: number | null,
  ): DeFiPositionsResponse['summary'] {
    const totalCurrentValue = positions.reduce(
      (sum, position) => sum + position.totals.totalCurrentValue,
      0,
    );
    const totalDepositValue = positions.reduce(
      (sum, position) => sum + position.totals.totalDepositValue,
      0,
    );
    const borrowedValues = positions
      .map((position) => position.totals.totalBorrowedValue)
      .filter((value): value is number => typeof value === 'number' && Number.isFinite(value));
    const healthFactors = positions
      .map((position) => position.totals.healthFactor)
      .filter((value): value is number => typeof value === 'number' && Number.isFinite(value));
    const protocols = new Set<string>(positions.map((position) => position.protocol));
    const horizonTotalBalance = Array.isArray(horizonBalances)
      ? horizonBalances.reduce((sum, balance) => sum + this.toPositiveNumber(balance.balance), 0)
      : 0;
    const totalAssetBalance =
      horizonTotalBalance > 0 ? horizonTotalBalance : totalCurrentValue;
    const totalAssetBalanceUsd =
      typeof horizonTotalAssetBalanceUsd === 'number' &&
      Number.isFinite(horizonTotalAssetBalanceUsd) &&
      horizonTotalAssetBalanceUsd > 0
        ? horizonTotalAssetBalanceUsd
        : totalCurrentValue;

    return {
      totalCurrentValue: this.roundTo4(totalCurrentValue),
      totalDepositValue: this.roundTo4(totalDepositValue),
      totalBorrowedValue:
        borrowedValues.length > 0
          ? this.roundTo4(borrowedValues.reduce((sum, value) => sum + value, 0))
          : null,
      totalCurrentReturn:
        totalCurrentValue > 0 || totalDepositValue > 0
          ? this.roundTo4(totalCurrentValue - totalDepositValue)
          : null,
      healthFactor:
        healthFactors.length > 0 ? this.roundTo4(Math.min(...healthFactors)) : null,
      positionCount: positions.length,
      protocolCount: protocols.size,
      totalAssetBalance: this.roundTo4(totalAssetBalance),
      totalAssetBalanceUsd: this.roundTo4(totalAssetBalanceUsd),
    };
  }

  private resolveAvailabilityStatus(statuses: {
    mongo: DeFiSourceStatus;
    horizon: DeFiSourceStatus;
    soroban: DeFiSourceStatus;
  }): DeFiAvailability['status'] {
    if (statuses.mongo === 'unavailable') {
      return 'unavailable';
    }

    if (
      statuses.mongo === 'degraded' ||
      statuses.horizon === 'degraded' ||
      statuses.soroban === 'degraded' ||
      statuses.horizon === 'unavailable' ||
      statuses.soroban === 'unavailable'
    ) {
      return 'degraded';
    }

    return 'healthy';
  }

  private resolveTimestampIso(value: unknown): string | undefined {
    if (value instanceof Date && Number.isFinite(value.getTime())) {
      return value.toISOString();
    }

    if (typeof value === 'string' && value.trim()) {
      const parsed = new Date(value);
      if (Number.isFinite(parsed.getTime())) {
        return parsed.toISOString();
      }
    }

    return undefined;
  }

  private isStaleTimestamp(value?: string): boolean {
    if (!value) {
      return true;
    }
    const parsed = Date.parse(value);
    if (!Number.isFinite(parsed)) {
      return true;
    }
    return Date.now() - parsed > 15 * 60 * 1000;
  }

  private async getJsonWithRetry<T = unknown>(
    url: string,
    config: AxiosRequestConfig & { retries?: number } = {},
  ): Promise<T> {
    const retries = Math.min(Math.max(Math.floor(this.numberOr(config.retries, 1)), 0), 4);
    const requestConfig: AxiosRequestConfig = {
      ...config,
      timeout: config.timeout ?? this.requestTimeoutMs,
      headers: {
        Accept: 'application/json, text/plain, */*',
        'User-Agent': 'yielder-backend/market-service',
        ...(config.headers || {}),
      },
    };
    delete (requestConfig as { retries?: number }).retries;

    let lastError: unknown = null;
    for (let attempt = 0; attempt <= retries; attempt += 1) {
      try {
        const response = await axios.get<T>(url, requestConfig);
        return response.data;
      } catch (error: unknown) {
        lastError = error;
        if (attempt >= retries || !this.isRetryableRequestError(error)) {
          throw error;
        }

        const delayMs = 250 * (attempt + 1);
        await this.sleep(delayMs);
      }
    }

    throw lastError instanceof Error ? lastError : new Error('Request failed with unknown error');
  }

  private isRetryableRequestError(error: unknown): boolean {
    if (!axios.isAxiosError(error)) {
      return false;
    }

    if (!error.response) {
      return true;
    }

    const status = error.response.status;
    if (status === 408 || status === 429) {
      return true;
    }

    return status >= 500;
  }

  private warnWithCooldown(key: string, message: string): void {
    const now = Date.now();
    const lastWarningAt = this.warningTimestamps.get(key) || 0;
    if (now - lastWarningAt < this.warningCooldownMs) {
      return;
    }

    this.warningTimestamps.set(key, now);
    this.logger.warn(message);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
  }

  private extractAssetRecords(payload: unknown): unknown[] {
    const embedded = this.readObjectValue(payload, '_embedded');
    const records = this.readObjectValue(embedded, 'records');
    if (!Array.isArray(records)) {
      return [];
    }

    return records;
  }

  private parseExpertAsset(payload: unknown): TickerAsset | null {
    const assetId = this.stringOr(this.readObjectValue(payload, 'asset'), '');
    if (!assetId) {
      return null;
    }

    const { assetCode, issuer } = this.parseAssetIdentifier(assetId);
    if (!assetCode) {
      return null;
    }

    const volume7d = this.numberOr(this.readObjectValue(payload, 'volume7d'), 0);
    const rawVolume24h = volume7d > 0 ? volume7d / 7 : 0;
    const priceUsd = this.numberOr(this.readObjectValue(payload, 'price'), 0);
    const volume24hUsd = (rawVolume24h / 10_000_000) * (priceUsd > 0 ? priceUsd : 1);
    const price7d = this.readObjectValue(payload, 'price7d');
    const change24hPercent = this.derive24hChangePercent(price7d, priceUsd);
    const trustlines = this.deriveTrustlines(this.readObjectValue(payload, 'trustlines'));
    const supplyRaw = this.toPositiveNumber(this.readObjectValue(payload, 'supply'));
    const supplyTokens = supplyRaw > 0 ? supplyRaw / 10_000_000 : 0;
    const tomlInfo = this.readObjectValue(payload, 'tomlInfo');
    const tomlCode = this.optionalString(this.readObjectValue(tomlInfo, 'code'));
    const displayName = tomlCode || assetCode;

    return {
      assetCode,
      issuer,
      displayName,
      priceUsd,
      change24hPercent,
      volume24hUsd,
      trustlines,
      supplyTokens: this.roundTo4(supplyTokens),
    };
  }

  private selectRepresentativeAssetsBySymbol(assets: TickerAsset[]): TickerAsset[] {
    const representatives = new Map<string, TickerAsset>();

    for (const asset of assets) {
      const symbol = this.normalizeTokenSymbol(asset.assetCode);
      const existing = representatives.get(symbol);
      if (!existing || this.shouldPreferTickerAsset(asset, existing)) {
        representatives.set(symbol, asset);
      }
    }

    return Array.from(representatives.values());
  }

  private shouldPreferTickerAsset(candidate: TickerAsset, current: TickerAsset): boolean {
    const candidateVolume = this.toPositiveNumber(candidate.volume24hUsd);
    const currentVolume = this.toPositiveNumber(current.volume24hUsd);
    if (candidateVolume !== currentVolume) {
      return candidateVolume > currentVolume;
    }

    const candidateTrustlines = this.toPositiveNumber(candidate.trustlines);
    const currentTrustlines = this.toPositiveNumber(current.trustlines);
    if (candidateTrustlines !== currentTrustlines) {
      return candidateTrustlines > currentTrustlines;
    }

    const candidateMarketCap = this.toPositiveNumber(candidate.supplyTokens * candidate.priceUsd);
    const currentMarketCap = this.toPositiveNumber(current.supplyTokens * current.priceUsd);
    if (candidateMarketCap !== currentMarketCap) {
      return candidateMarketCap > currentMarketCap;
    }

    const candidateAbsChange = Math.abs(this.numberOr(candidate.change24hPercent, 0));
    const currentAbsChange = Math.abs(this.numberOr(current.change24hPercent, 0));
    if (candidateAbsChange !== currentAbsChange) {
      return candidateAbsChange < currentAbsChange;
    }

    const candidateId = `${candidate.assetCode}:${candidate.issuer || 'native'}`;
    const currentId = `${current.assetCode}:${current.issuer || 'native'}`;
    return candidateId.localeCompare(currentId) < 0;
  }

  private parseAssetIdentifier(assetIdentifier: string): { assetCode: string; issuer?: string } {
    if (assetIdentifier === 'XLM') {
      return { assetCode: 'XLM' };
    }

    const match = assetIdentifier.match(/^(.+)-([A-Z2-7]{56})(?:-\d+)?$/);
    if (match) {
      return {
        assetCode: match[1],
        issuer: match[2],
      };
    }

    return { assetCode: assetIdentifier };
  }

  private derive24hChangePercent(price7d: unknown, currentPrice: number): number {
    if (!Array.isArray(price7d) || price7d.length < 2 || currentPrice <= 0) {
      return 0;
    }

    const latestPair = price7d[price7d.length - 1];
    const previousPair = price7d[price7d.length - 2];
    const latest = this.readPriceFromPair(latestPair);
    const previous = this.readPriceFromPair(previousPair);

    const current = latest > 0 ? latest : currentPrice;
    if (previous <= 0) {
      return 0;
    }

    return ((current - previous) / previous) * 100;
  }

  private readPriceFromPair(pair: unknown): number {
    if (!Array.isArray(pair) || pair.length < 2) {
      return 0;
    }

    return this.numberOr(pair[1], 0);
  }

  private deriveTrustlines(value: unknown): number {
    if (Array.isArray(value) && value.length > 0) {
      return this.numberOr(value[0], 0);
    }

    if (typeof value === 'object' && value !== null) {
      const total = this.readObjectValue(value, 'total');
      const funded = this.readObjectValue(value, 'funded');
      const parsedTotal = this.numberOr(total, 0);
      if (parsedTotal > 0) {
        return parsedTotal;
      }

      return this.numberOr(funded, 0);
    }

    return this.numberOr(value, 0);
  }

  private buildProtocol(params: {
    id: string;
    name: string;
    slug: string;
    url?: string;
    category: string;
    token: string;
    logo?: string;
    audited?: boolean;
    tvl: number;
    apy: number;
    users?: number;
    change24h?: number;
  }): Protocol {
    return {
      id: params.id,
      name: params.name,
      slug: params.slug,
      url: params.url,
      category: params.category,
      tvl: this.roundTo4(params.tvl),
      apy: this.roundTo4(params.apy),
      token: params.token,
      audited: Boolean(params.audited),
      users: Math.max(0, Math.round(this.toPositiveNumber(params.users))),
      logo: params.logo || params.token,
      change24h: this.roundTo4(this.numberOr(params.change24h, 0)),
    };
  }

  private normalizeProtocolKey(value: string): string {
    return this.stringOr(value, '')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '');
  }

  private normalizeProtocolCategory(value: string): string {
    const normalized = this.normalizeProtocolKey(value);
    if (!normalized) {
      return 'defi';
    }
    if (normalized.includes('dex')) {
      return 'dex';
    }
    if (normalized.includes('lend')) {
      return 'lending';
    }
    if (normalized.includes('rwa')) {
      return 'rwa';
    }
    if (normalized.includes('yield')) {
      return 'yield';
    }
    if (normalized.includes('bridge')) {
      return 'bridge';
    }
    if (normalized.includes('stable')) {
      return 'stablecoin';
    }
    if (normalized.includes('cex')) {
      return 'cex';
    }
    return normalized;
  }

  private normalizeTokenSymbol(value: string): string {
    const normalized = this.stringOr(value, '')
      .trim()
      .toUpperCase()
      .replace(/[^A-Z0-9]/g, '');
    if (normalized) {
      return normalized.slice(0, 12);
    }
    return 'DEFI';
  }

  private resolveFallbackProtocolTokenBySlug(slug: string): string | null {
    const key = this.normalizeProtocolKey(slug);
    if (!key) {
      return null;
    }

    const matched = FALLBACK_SOROBAN_PROTOCOLS.find(
      (protocol) => this.normalizeProtocolKey(protocol.slug) === key,
    );
    if (!matched) {
      return null;
    }

    const token = this.normalizeTokenSymbol(matched.token);
    return token || null;
  }

  private resolvePreferredProtocolToken(slug: string, candidateToken: string): string {
    const normalizedCandidate = this.normalizeTokenSymbol(candidateToken);
    const fallbackToken = this.resolveFallbackProtocolTokenBySlug(slug);
    if (!fallbackToken) {
      return normalizedCandidate;
    }

    const genericTokens = new Set(['DEFI', 'XLM']);
    if (genericTokens.has(normalizedCandidate) && !genericTokens.has(fallbackToken)) {
      return fallbackToken;
    }

    return normalizedCandidate || fallbackToken;
  }

  private chooseProtocolToken(slug: string, existingToken: string, incomingToken: string): string {
    const existing = this.resolvePreferredProtocolToken(slug, existingToken);
    const incoming = this.resolvePreferredProtocolToken(slug, incomingToken);
    const genericTokens = new Set(['DEFI', 'XLM']);

    if (genericTokens.has(incoming) && !genericTokens.has(existing)) {
      return existing;
    }
    if (genericTokens.has(existing) && !genericTokens.has(incoming)) {
      return incoming;
    }

    return incoming || existing || this.resolveFallbackProtocolTokenBySlug(slug) || 'DEFI';
  }

  private resolveProtocolAuditedStatus(
    slugOrKey: string,
    fallbackAudited: boolean,
    auditedByProtocol: Map<string, boolean>,
  ): boolean {
    const key = this.normalizeProtocolKey(slugOrKey);
    if (!key) {
      return Boolean(fallbackAudited);
    }

    const catalogAudited = auditedByProtocol.get(key);
    if (typeof catalogAudited === 'boolean') {
      return catalogAudited;
    }

    return Boolean(fallbackAudited);
  }

  private buildTokenApyBaselineMap(protocols: Protocol[]): Map<string, number> {
    const map = new Map<string, number>();
    for (const protocol of protocols) {
      const apy = this.toPositiveNumber(protocol.apy);
      if (apy <= 0) {
        continue;
      }

      const token = this.normalizeTokenSymbol(protocol.token);
      if (!token || token === 'DEFI') {
        continue;
      }

      const existing = this.toPositiveNumber(map.get(token));
      if (apy > existing) {
        map.set(token, this.roundTo4(apy));
      }
    }
    return map;
  }

  private buildCategoryApyBaselineMap(protocols: Protocol[]): Map<string, number> {
    const buckets = new Map<string, number[]>();
    for (const protocol of protocols) {
      const apy = this.toPositiveNumber(protocol.apy);
      if (apy <= 0) {
        continue;
      }

      const category = this.normalizeProtocolCategory(protocol.category);
      const values = buckets.get(category);
      if (values) {
        values.push(apy);
      } else {
        buckets.set(category, [apy]);
      }
    }

    const map = new Map<string, number>();
    for (const [category, values] of buckets.entries()) {
      const baseline = this.computeMedian(values);
      if (baseline > 0) {
        map.set(category, this.roundTo4(baseline));
      }
    }
    return map;
  }

  private resolveGlobalApyBaseline(protocols: Protocol[]): number {
    const apys = protocols
      .map((protocol) => this.toPositiveNumber(protocol.apy))
      .filter((value) => value > 0);
    if (apys.length === 0) {
      return 2.5;
    }
    return this.roundTo4(Math.max(0.1, this.computeMedian(apys)));
  }

  private computeMedian(values: number[]): number {
    if (!Array.isArray(values) || values.length === 0) {
      return 0;
    }

    const sorted = values
      .map((value) => this.toPositiveNumber(value))
      .filter((value) => value > 0)
      .sort((left, right) => left - right);
    if (sorted.length === 0) {
      return 0;
    }

    const middle = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 0) {
      return (sorted[middle - 1] + sorted[middle]) / 2;
    }
    return sorted[middle];
  }

  private resolveFallbackProtocolApy(params: {
    fallback: FallbackSorobanProtocol;
    tokenBaselines: Map<string, number>;
    categoryBaselines: Map<string, number>;
    globalBaseline: number;
  }): number {
    const token = this.normalizeTokenSymbol(params.fallback.token);
    const tokenBaseline = this.toPositiveNumber(params.tokenBaselines.get(token));
    if (tokenBaseline > 0) {
      return this.roundTo4(Math.min(35, Math.max(0.1, tokenBaseline)));
    }

    const category = this.normalizeProtocolCategory(params.fallback.category);
    const categoryCandidates = this.resolveFallbackCategoryCandidates(category);
    for (const candidate of categoryCandidates) {
      const baseline = this.toPositiveNumber(params.categoryBaselines.get(candidate));
      if (baseline > 0) {
        return this.roundTo4(Math.min(35, Math.max(0.1, baseline)));
      }
    }

    const globalBaseline = this.toPositiveNumber(params.globalBaseline);
    if (globalBaseline > 0) {
      return this.roundTo4(Math.min(35, Math.max(0.1, globalBaseline)));
    }

    return 2.5;
  }

  private resolveFallbackCategoryCandidates(category: string): string[] {
    const normalized = this.normalizeProtocolCategory(category);
    const mapping: Record<string, string[]> = {
      lending: ['lending', 'yield', 'rwa', 'dex'],
      dex: ['dex', 'trading', 'yield', 'lending'],
      rwa: ['rwa', 'stablecoin', 'yield', 'lending'],
      yield: ['yield', 'lending', 'rwa', 'dex'],
      stablecoin: ['stablecoin', 'rwa', 'lending', 'dex'],
      insurance: ['insurance', 'lending', 'yield', 'rwa'],
      trading: ['trading', 'dex', 'yield'],
      bridge: ['bridge', 'dex', 'rwa', 'stablecoin'],
    };

    const preferred = mapping[normalized] || [normalized];
    const fallback = ['lending', 'rwa', 'dex', 'yield', 'stablecoin'];
    return Array.from(new Set([...preferred, ...fallback]));
  }

  private resolveProtocolDisplayName(slug: string): string {
    const key = this.normalizeProtocolKey(slug);
    const mapping: Record<string, string> = {
      aquarius: 'Aquarius',
      'aquarius-stellar': 'Aquarius',
      phoenix: 'Phoenix',
      'phoenix-defi-hub': 'Phoenix',
      soroswap: 'Soroswap',
      blend: 'Blend',
      fxdao: 'FxDAO',
      'ondo-yield-assets': 'Ondo Yield Assets',
      stellar: 'Stellar AMM',
    };
    return mapping[key] || (key ? key.replace(/-/g, ' ').replace(/\b\w/g, (m) => m.toUpperCase()) : 'Unknown');
  }

  private resolveProtocolUrl(slug: string): string | undefined {
    const normalized = this.normalizeProtocolKey(slug);
    const mapping: Record<string, string> = {
      aquarius: 'https://aqua.network',
      'aquarius-stellar': 'https://aqua.network',
      phoenix: 'https://www.phoenix-hub.io',
      'phoenix-defi-hub': 'https://www.phoenix-hub.io',
      templar: 'https://app.templarfi.org',
      'templar-protocol': 'https://app.templarfi.org',
      etherfuse: 'https://app.etherfuse.com',
      spiko: 'https://www.spiko.io',
      blend: 'https://www.blend.capital/',
      fxdao: 'https://www.fxdao.io/',
      soroswap: 'https://www.soroswap.finance/',
      'ondo-yield-assets': 'https://ondo.finance/',
    };
    return mapping[normalized];
  }

  private resolveProtocolLogoUrl(slug: string, explicitLogo?: string): string {
    const logoCandidate = this.stringOr(explicitLogo, '').trim();
    if (/^https?:\/\//i.test(logoCandidate)) {
      return logoCandidate;
    }
    const normalized = this.normalizeProtocolKey(logoCandidate || slug);
    const overrides: Record<string, string> = {
      'templar-protocol': 'https://app.templarfi.org/api/icon?size=180',
      phoenix: 'https://www.phoenix-hub.io/favicon.ico',
      'phoenix-defi-hub': 'https://www.phoenix-hub.io/favicon.ico',
      aquarius: 'https://aqua.network/favicon.png',
      'aquarius-stellar': 'https://aqua.network/favicon.png',
      etherfuse: 'https://app.etherfuse.com/favicon.ico',
      spiko: 'https://www.spiko.io/favicon.ico',
      blend: 'https://icons.llama.fi/blend.jpg',
      fxdao: 'https://icons.llama.fi/fxdao.jpg',
      soroswap: 'https://icons.llama.fi/soroswap.png',
      'ondo-yield-assets': 'https://icons.llama.fi/ondo-yield-assets.jpg',
    };
    if (overrides[normalized]) {
      return overrides[normalized];
    }
    return `${this.defiLlamaIconsBaseUrl}/${encodeURIComponent(normalized || 'stellar')}.png`;
  }

  private canonicalSorobanProtocolSlug(rawProject: string): string {
    const normalized = this.normalizeProtocolKey(rawProject);
    const aliases: Record<string, string> = {
      aquarius: 'aquarius-stellar',
      phoenix: 'phoenix-defi-hub',
      templar: 'templar-protocol',
      'blend-pools': 'blend',
      'blend-pools-v2': 'blend',
    };
    return aliases[normalized] || normalized;
  }

  private resolveDefiLlamaPoolTokenSymbol(value: unknown): string {
    const raw = this.stringOr(value, '').trim().toUpperCase();
    if (!raw) {
      return 'DEFI';
    }

    const tokens = raw
      .split(/[,\s/:-]+/)
      .map((part) => this.normalizeTokenSymbol(part))
      .filter((part) => part.length > 0);
    if (tokens.length === 0) {
      return 'DEFI';
    }

    const preferred = tokens.find((token) => token !== 'XLM' && token !== 'DEFI');
    return preferred || tokens[0];
  }

  private resolveDefiLlamaPoolApy(pool: DefiLlamaYieldPool): number {
    const direct = this.toPositiveNumber(pool.apy);
    if (direct > 0) {
      return this.roundTo4(direct);
    }

    const base = this.toPositiveNumber(pool.apyBase);
    const reward = this.toPositiveNumber(pool.apyReward);
    return this.roundTo4(base + reward);
  }

  private resolveDefiLlamaPoolCategory(value: unknown, protocolSlug?: string): string {
    const normalized = this.stringOr(value, '').trim().toLowerCase();
    if (!normalized) {
      const key = this.normalizeProtocolKey(protocolSlug || '');
      const fallbackBySlug: Record<string, string> = {
        blend: 'lending',
        soroswap: 'dex',
        fxdao: 'stablecoin',
        'ondo-yield-assets': 'rwa',
      };
      return fallbackBySlug[key] || 'defi';
    }

    if (normalized.includes('dex') || normalized.includes('amm') || normalized.includes('liquidity')) {
      return 'dex';
    }
    if (normalized.includes('lend') || normalized.includes('borrow') || normalized.includes('credit')) {
      return 'lending';
    }
    if (normalized.includes('rwa')) {
      return 'rwa';
    }
    if (normalized.includes('stake') || normalized.includes('yield')) {
      return 'yield';
    }

    return this.normalizeProtocolCategory(normalized);
  }

  private async fetchDefiLlamaStellarProtocolMetadata(): Promise<DefiLlamaStellarProtocolMetadata> {
    const emptyMetadata = (): DefiLlamaStellarProtocolMetadata => ({
      tvlByProtocol: new Map<string, number>(),
      auditedByProtocol: new Map<string, boolean>(),
    });

    if (!this.defiLlamaProtocolsUrl) {
      return emptyMetadata();
    }

    if (
      this.defiLlamaStellarProtocolMetadataCache &&
      Date.now() - this.defiLlamaStellarProtocolMetadataCache.timestamp < this.defiLlamaProtocolsCacheTtlMs
    ) {
      return {
        tvlByProtocol: new Map(this.defiLlamaStellarProtocolMetadataCache.data.tvlByProtocol),
        auditedByProtocol: new Map(this.defiLlamaStellarProtocolMetadataCache.data.auditedByProtocol),
      };
    }

    if (this.defiLlamaStellarProtocolMetadataInFlight) {
      return this.defiLlamaStellarProtocolMetadataInFlight;
    }

    const request = (async (): Promise<DefiLlamaStellarProtocolMetadata> => {
      try {
        const payload = await this.getJsonWithRetry<DefiLlamaProtocolRecord[]>(
          this.defiLlamaProtocolsUrl,
          { retries: 1 },
        );
        const records = Array.isArray(payload) ? payload : [];
        const tvlByProtocol = new Map<string, number>();
        const auditedByProtocol = new Map<string, boolean>();

        for (const record of records) {
          const name = this.stringOr(record?.name, '');
          const slug = this.stringOr(record?.slug, name);
          const canonicalSlug = this.canonicalSorobanProtocolSlug(slug || name);
          if (!canonicalSlug) {
            continue;
          }

          const chainTvl = this.resolveStellarChainTvl(record?.chainTvls);
          const chains = Array.isArray(record?.chains)
            ? (record.chains as unknown[])
                .map((chain) => this.stringOr(chain, '').trim().toLowerCase())
                .filter(Boolean)
            : [];
          const hasStellarChain = chainTvl > 0 || chains.includes('stellar');
          if (!hasStellarChain) {
            continue;
          }

          const isAudited = this.isDefiLlamaProtocolAudited(record);
          const existingAudited = Boolean(auditedByProtocol.get(canonicalSlug));
          if (isAudited || existingAudited) {
            auditedByProtocol.set(canonicalSlug, true);
          } else if (!auditedByProtocol.has(canonicalSlug)) {
            auditedByProtocol.set(canonicalSlug, false);
          }

          const totalTvl = this.toPositiveNumber(record?.tvl);
          const stellarTvl = chainTvl > 0 ? chainTvl : totalTvl;
          if (stellarTvl <= 0) {
            continue;
          }

          const existingTvl = this.toPositiveNumber(tvlByProtocol.get(canonicalSlug));
          if (stellarTvl > existingTvl) {
            tvlByProtocol.set(canonicalSlug, this.roundTo4(stellarTvl));
          }
        }

        const metadata: DefiLlamaStellarProtocolMetadata = {
          tvlByProtocol,
          auditedByProtocol,
        };
        this.defiLlamaStellarProtocolMetadataCache = {
          timestamp: Date.now(),
          data: metadata,
        };
        return {
          tvlByProtocol: new Map(tvlByProtocol),
          auditedByProtocol: new Map(auditedByProtocol),
        };
      } catch (error: unknown) {
        this.warnWithCooldown(
          'protocol-defillama-stellar-metadata-source',
          `Failed to fetch DefiLlama protocol catalog for Stellar metadata. ${this.errorMessage(error)}`,
        );
        if (this.defiLlamaStellarProtocolMetadataCache) {
          return {
            tvlByProtocol: new Map(this.defiLlamaStellarProtocolMetadataCache.data.tvlByProtocol),
            auditedByProtocol: new Map(this.defiLlamaStellarProtocolMetadataCache.data.auditedByProtocol),
          };
        }
        return emptyMetadata();
      }
    })();

    this.defiLlamaStellarProtocolMetadataInFlight = request;
    try {
      return await request;
    } finally {
      if (this.defiLlamaStellarProtocolMetadataInFlight === request) {
        this.defiLlamaStellarProtocolMetadataInFlight = null;
      }
    }
  }

  private isDefiLlamaProtocolAudited(record: DefiLlamaProtocolRecord): boolean {
    const auditsCount = this.toPositiveNumber(record?.audits);
    const auditLinksCount = Array.isArray(record?.audit_links)
      ? (record.audit_links as unknown[]).filter((link) => this.stringOr(link, '').trim().length > 0).length
      : 0;
    return auditsCount > 0 || auditLinksCount > 0;
  }

  private resolveStellarChainTvl(value: unknown): number {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      return 0;
    }

    let highest = 0;
    for (const [chainKey, chainTvl] of Object.entries(value as Record<string, unknown>)) {
      const normalizedChain = this.stringOr(chainKey, '').trim().toLowerCase();
      if (!normalizedChain.startsWith('stellar')) {
        continue;
      }

      const parsed = this.toPositiveNumber(chainTvl);
      if (parsed > highest) {
        highest = parsed;
      }
    }

    return highest;
  }

  private async fetchDefiLlamaStellarProtocols(): Promise<Protocol[]> {
    if (!this.defiLlamaYieldsUrl) {
      return [];
    }

    const payload = await this.getJsonWithRetry<DefiLlamaYieldResponse>(this.defiLlamaYieldsUrl, {
      retries: 1,
    });
    const pools = Array.isArray(payload?.data) ? payload.data : [];
    if (pools.length === 0) {
      return [];
    }

    type Aggregate = {
      id: string;
      slug: string;
      name: string;
      category: string;
      token: string;
      tokenTvl: number;
      tvl: number;
      apy: number;
    };

    const byProtocol = new Map<string, Aggregate>();

    for (const pool of pools) {
      const chain = this.stringOr(pool.chain, '').trim().toLowerCase();
      if (chain !== 'stellar') {
        continue;
      }

      const project = this.stringOr(pool.project, '');
      const slug = this.canonicalSorobanProtocolSlug(project);
      if (!slug) {
        continue;
      }

      const tvl = this.toPositiveNumber(pool.tvlUsd);
      const apy = this.resolveDefiLlamaPoolApy(pool);
      const token = this.resolveDefiLlamaPoolTokenSymbol(pool.symbol);
      const normalizedToken = this.resolvePreferredProtocolToken(slug, token);
      const category = this.resolveDefiLlamaPoolCategory(pool.category, slug);
      const name = this.resolveProtocolDisplayName(slug);

      const existing = byProtocol.get(slug);
      if (!existing) {
        byProtocol.set(slug, {
          id: slug,
          slug,
          name,
          category,
          token: normalizedToken,
          tokenTvl: tvl,
          tvl,
          apy,
        });
        continue;
      }

      existing.tvl += tvl;
      existing.apy = Math.max(existing.apy, apy);
      if (tvl >= existing.tokenTvl && normalizedToken && normalizedToken !== 'DEFI') {
        existing.token = normalizedToken;
        existing.tokenTvl = tvl;
      }

      if (existing.category === 'defi' && category !== 'defi') {
        existing.category = category;
      }
    }

    return Array.from(byProtocol.values())
      .filter((item) => item.tvl > 0 || item.apy > 0)
      .map((item) =>
        this.buildProtocol({
          id: item.id,
          name: item.name,
          slug: item.slug,
          url: this.resolveProtocolUrl(item.slug),
          category: item.category,
          token: item.token,
          logo: this.resolveProtocolLogoUrl(item.slug),
          audited: false,
          tvl: item.tvl,
          apy: item.apy,
          users: 0,
          change24h: 0,
        }),
      );
  }

  private resolveProtocolUsers(
    tokenSymbol: string,
    tvlUsd: number,
    trustlineMap: Map<string, number>,
  ): number {
    const fromTvl = this.estimateProtocolUsersFromTvl(tvlUsd);
    if (fromTvl > 0) {
      return fromTvl;
    }

    // Token trustlines are network-wide and not protocol-specific.
    // Use them only as a soft fallback when TVL-based estimation is unavailable.
    const symbol = this.normalizeTokenSymbol(tokenSymbol);
    const fromTrustlines = trustlineMap.get(symbol) || 0;
    if (fromTrustlines <= 0) {
      return 0;
    }

    return Math.max(10, Math.round(Math.sqrt(fromTrustlines)));
  }

  private resolveProtocolChange24h(
    tokenSymbol: string,
    change24hMap: Map<string, number>,
  ): number {
    const symbol = this.normalizeTokenSymbol(tokenSymbol);
    const change = this.numberOr(change24hMap.get(symbol), 0);
    if (!Number.isFinite(change)) {
      return 0;
    }
    if (Math.abs(change) < 0.0001) {
      return 0;
    }
    return this.roundTo4(change);
  }

  private estimateProtocolTvlFromTokenActivity(
    tokenSymbol: string,
    marketCapMap: Map<string, number>,
    volume24hMap: Map<string, number>,
  ): number {
    const symbol = this.normalizeTokenSymbol(tokenSymbol);
    const marketCapUsd = this.toPositiveNumber(marketCapMap.get(symbol));
    const volume24hUsd = this.toPositiveNumber(volume24hMap.get(symbol));

    if (marketCapUsd <= 0 && volume24hUsd <= 0) {
      return 0;
    }

    // Estimate TVL from token-level market data:
    // - ~1.25% of market cap (protocol share heuristic)
    // - ~5x 24h token volume (liquidity turnover heuristic)
    const fromMarketCap = marketCapUsd > 0 ? marketCapUsd * 0.0125 : 0;
    const fromVolume = volume24hUsd > 0 ? volume24hUsd * 5 : 0;
    const rawEstimate = Math.max(fromMarketCap, fromVolume);
    if (rawEstimate <= 0) {
      return 0;
    }

    const boundedEstimate = marketCapUsd > 0 ? Math.min(rawEstimate, marketCapUsd * 0.2) : rawEstimate;
    return this.roundTo4(Math.max(1_000, boundedEstimate));
  }

  private resolveProtocolChange24hWithEstimation(params: {
    tokenSymbol: string;
    tokenChange24hMap: Map<string, number>;
    apy?: number;
    tvlUsd?: number;
    explicitChange24h?: number;
  }): number {
    const explicit = this.numberOr(params.explicitChange24h, 0);
    if (Math.abs(explicit) >= 0.0001) {
      return this.roundTo4(explicit);
    }

    const tokenChange = this.resolveProtocolChange24h(params.tokenSymbol, params.tokenChange24hMap);
    if (Math.abs(tokenChange) >= 0.0001) {
      return tokenChange;
    }

    const apy = this.toPositiveNumber(params.apy);
    if (apy > 0) {
      return this.roundTo4(Math.min(8, Math.max(0.08, apy * 0.1)));
    }

    const tvl = this.toPositiveNumber(params.tvlUsd);
    if (tvl > 0) {
      return 0.05;
    }

    return 0;
  }

  private estimateProtocolUsersFromTvl(tvlUsd: number): number {
    const tvl = this.toPositiveNumber(tvlUsd);
    if (tvl <= 0) {
      return 0;
    }
    const estimate = Math.sqrt(tvl) / 3;
    return Math.max(40, Math.round(estimate));
  }

  private async fetchAssetMarketMap(limit = 200): Promise<{
    trustlines: Map<string, number>;
    change24hPercent: Map<string, number>;
    marketCapUsd: Map<string, number>;
    volume24hUsd: Map<string, number>;
  }> {
    const payload = await this.getJsonWithRetry(`${this.stellarExpertApi}/asset`, {
      retries: 2,
      params: {
        order: 'desc',
        limit: Math.max(20, Math.min(300, Math.round(limit))),
        cursor: 0,
      },
    });
    const records = this.extractAssetRecords(payload);
    const trustlines = new Map<string, number>();
    const change24hPercent = new Map<string, number>();
    const changeSourceByToken = new Map<string, TickerAsset>();
    const marketCapUsd = new Map<string, number>();
    const volume24hUsd = new Map<string, number>();
    for (const record of records) {
      const parsed = this.parseExpertAsset(record);
      if (!parsed) {
        continue;
      }
      const token = this.normalizeTokenSymbol(parsed.assetCode);
      const previous = trustlines.get(token) || 0;
      if (parsed.trustlines > previous) {
        trustlines.set(token, parsed.trustlines);
      }
      const previousChangeSource = changeSourceByToken.get(token);
      if (!previousChangeSource || this.shouldPreferTickerAsset(parsed, previousChangeSource)) {
        changeSourceByToken.set(token, parsed);
        change24hPercent.set(token, this.roundTo4(parsed.change24hPercent));
      }

      const parsedMarketCap = parsed.priceUsd > 0 ? parsed.supplyTokens * parsed.priceUsd : 0;
      const previousMarketCap = this.toPositiveNumber(marketCapUsd.get(token));
      if (parsedMarketCap > previousMarketCap) {
        marketCapUsd.set(token, this.roundTo4(parsedMarketCap));
      }

      const parsedVolume24h = this.toPositiveNumber(parsed.volume24hUsd);
      const previousVolume24h = this.toPositiveNumber(volume24hUsd.get(token));
      if (parsedVolume24h > previousVolume24h) {
        volume24hUsd.set(token, this.roundTo4(parsedVolume24h));
      }
    }
    return { trustlines, change24hPercent, marketCapUsd, volume24hUsd };
  }

  private normalizePoolTokenIdentifier(value: string): string {
    const normalized = this.stringOr(value, '').trim();
    if (!normalized) {
      return '';
    }
    if (normalized.toLowerCase() === 'native') {
      return 'XLM';
    }
    const [code] = normalized.split(':');
    return this.normalizeTokenSymbol(code || normalized);
  }

  private mergeLiquidityPools(pools: LiquidityPool[]): LiquidityPool[] {
    const deduped = new Map<string, LiquidityPool>();
    for (const pool of pools) {
      const key = `${this.normalizeProtocolKey(pool.protocolSlug || pool.protocol)}:${pool.pair.toUpperCase()}`;
      const existing = deduped.get(key);
      if (!existing) {
        deduped.set(key, pool);
        continue;
      }
      deduped.set(key, {
        ...existing,
        id: existing.id || pool.id,
        protocol: existing.protocol || pool.protocol,
        protocolSlug: existing.protocolSlug || pool.protocolSlug,
        protocolLogo: existing.protocolLogo || pool.protocolLogo,
        tvlUsd: Math.max(existing.tvlUsd, pool.tvlUsd),
        volume24hUsd: Math.max(existing.volume24hUsd, pool.volume24hUsd),
        apy: Math.max(existing.apy, pool.apy),
        feeBps: existing.feeBps > 0 ? existing.feeBps : pool.feeBps,
        totalShares: Math.max(existing.totalShares, pool.totalShares),
        users: Math.max(existing.users, pool.users),
        reserveCount: Math.max(existing.reserveCount, pool.reserveCount),
        source: existing.source || pool.source,
      });
    }
    return Array.from(deduped.values()).map((pool) => ({
      ...pool,
      tvlUsd: this.roundTo4(pool.tvlUsd),
      volume24hUsd: this.roundTo4(pool.volume24hUsd),
      apy: this.roundTo4(pool.apy),
      totalShares: this.roundTo4(pool.totalShares),
    }));
  }

  private async fetchAquariusLiquidityPools(): Promise<LiquidityPool[]> {
    const payload = await this.getJsonWithRetry(this.aquaPoolsApiUrl, { retries: 2 });
    const pools = this.resolveAquaPoolsPayload(payload);
    const mapped: LiquidityPool[] = [];
    for (const pool of pools) {
      const tokens = this.resolveAquaPoolTokens(pool.tokens_str)
        .map((token) => this.normalizePoolTokenIdentifier(token))
        .filter(Boolean);
      const pair = tokens.slice(0, 2).join('/') || 'Unknown';
      const tvlUsd = this.toPositiveNumber(pool.liquidity_usd);
      if (tvlUsd < 1_000 || tvlUsd > 50_000_000) {
        continue;
      }
      const volume24hUsd = this.resolveAquaVolume24hUsd(pool, tvlUsd);
      const feeBps = this.resolveFeeBps(this.toPositiveNumber(pool.fee), 30);
      const apy = this.resolveAquaPoolApyPercent({
        apyRaw: this.toPositiveNumber(pool.total_apy),
        tvlUsd,
        volume24hUsd,
        feeBps,
      });
      const totalShares = this.toPositiveNumber(pool.total_share);

      mapped.push({
        id: this.stringOr(pool.index, this.stringOr(pool.address, `aquarius-${pair}`)),
        pair,
        protocol: 'Aquarius',
        protocolSlug: 'aquarius-stellar',
        protocolLogo: this.resolveProtocolLogoUrl('aquarius-stellar'),
        tvlUsd,
        volume24hUsd,
        apy,
        feeBps,
        totalShares,
        users: this.estimateProtocolUsersFromTvl(tvlUsd),
        reserveCount: Math.max(tokens.length, 2),
        source: 'aquarius',
      });
    }
    return mapped;
  }

  private resolvePhoenixPoolPair(pool: PhoenixStatsPool): string {
    const symbolA = this.normalizeTokenSymbol(
      this.stringOr(pool.asset_a?.token_info?.symbol, this.stringOr(pool.asset_a_address, '')),
    );
    const symbolB = this.normalizeTokenSymbol(
      this.stringOr(pool.asset_b?.token_info?.symbol, this.stringOr(pool.asset_b_address, '')),
    );
    if (symbolA && symbolB) {
      return `${symbolA}/${symbolB}`;
    }
    return 'Unknown';
  }

  private async fetchPhoenixLiquidityPools(): Promise<LiquidityPool[]> {
    const payload = await this.getJsonWithRetry(this.phoenixPoolsApiUrl, { retries: 2 });
    const rawPools = this.resolvePhoenixPoolsPayload(payload)
      .map((pool) => ({
        poolAddress: this.stringOr(pool.pool_address, ''),
        pair: this.resolvePhoenixPoolPair(pool),
        feeBps: this.toPositiveNumber(pool.total_fee_bps),
        tvlUsd: this.toPositiveNumber(pool.tvl_usd),
      }))
      .filter((pool) => pool.poolAddress && pool.pair !== 'Unknown' && pool.tvlUsd >= 1_000);

    if (rawPools.length === 0) {
      return [];
    }

    const volumeResults = await Promise.allSettled(
      rawPools.map((pool) => this.fetchPhoenixPoolWeeklyVolumeUsd(pool.poolAddress)),
    );
    const mapped: LiquidityPool[] = [];
    for (let index = 0; index < rawPools.length; index += 1) {
      const pool = rawPools[index];
      const volumeResult = volumeResults[index];
      const volumeWeekly =
        volumeResult && volumeResult.status === 'fulfilled' ? volumeResult.value : 0;
      const volume24hUsd = volumeWeekly > 0 ? volumeWeekly / 7 : 0;
      const feeRate = pool.feeBps > 0 ? pool.feeBps / 10_000 : 0;
      const apy =
        pool.tvlUsd > 0 && volume24hUsd > 0 && feeRate > 0
          ? ((volume24hUsd * feeRate) * 365 * 100) / pool.tvlUsd
          : 0;

      mapped.push({
        id: pool.poolAddress,
        pair: pool.pair,
        protocol: 'Phoenix',
        protocolSlug: 'phoenix-defi-hub',
        protocolLogo: this.resolveProtocolLogoUrl('phoenix-defi-hub'),
        tvlUsd: pool.tvlUsd,
        volume24hUsd,
        apy,
        feeBps: pool.feeBps > 0 ? Math.round(pool.feeBps) : 30,
        totalShares: 0,
        users: this.estimateProtocolUsersFromTvl(pool.tvlUsd),
        reserveCount: 2,
        source: 'phoenix',
      });
    }
    return mapped;
  }

  private async fetchHorizonLiquidityPoolsCatalog(limit = 100): Promise<LiquidityPool[]> {
    const payload = await this.getJsonWithRetry(`${this.horizonApi}/liquidity_pools`, {
      retries: 1,
      params: {
        order: 'desc',
        limit: Math.max(20, Math.min(200, Math.round(limit))),
      },
    });
    const embedded = this.readObjectValue(payload, '_embedded');
    const recordsRaw = this.readObjectValue(embedded, 'records');
    if (!Array.isArray(recordsRaw)) {
      return [];
    }

    const records = recordsRaw as Array<HorizonLiquidityPoolResponse & { total_trustlines?: number | string }>;
    const mapped: LiquidityPool[] = [];
    for (const record of records) {
      const id = this.stringOr(record.id, '');
      if (!id) {
        continue;
      }
      const reserves = Array.isArray(record.reserves) ? (record.reserves as HorizonLiquidityPoolReserve[]) : [];
      const pair = reserves
        .map((reserve) => this.parseHorizonAssetReference(this.stringOr(reserve.asset, '')))
        .filter((asset): asset is { assetCode: string; assetIssuer?: string } => Boolean(asset))
        .map((asset) => asset.assetCode)
        .slice(0, 2)
        .join('/');
      if (!pair) {
        continue;
      }

      const protocolMeta = this.resolveLiveFallbackProtocol(pair, 'liquidity_pool_shares');
      const protocolSlug = this.normalizeProtocolKey(protocolMeta.protocol || 'stellar');
      mapped.push({
        id,
        pair,
        protocol: this.resolveProtocolDisplayName(protocolSlug),
        protocolSlug,
        protocolLogo: this.resolveProtocolLogoUrl(protocolSlug),
        tvlUsd: 0,
        volume24hUsd: 0,
        apy: 0,
        feeBps: this.toPositiveNumber(record.fee_bp),
        totalShares: this.toPositiveNumber(record.total_shares),
        users: this.toPositiveNumber(record.total_trustlines),
        reserveCount: Math.max(reserves.length, 2),
        source: 'horizon',
      });
    }
    return mapped;
  }

  private async fetchAquariusMetrics(): Promise<{ tvlUsd: number; apyPercent: number }> {
    const payload = await this.getJsonWithRetry(this.aquaPoolsApiUrl, { retries: 2 });
    const pools = this.resolveAquaPoolsPayload(payload);

    const apyPools = pools
      .map((pool) => {
        const liquidityUsd = this.toPositiveNumber(pool.liquidity_usd);
        const volume24hUsd = this.resolveAquaVolume24hUsd(pool, liquidityUsd);
        const feeBps = this.resolveFeeBps(this.toPositiveNumber(pool.fee), 30);
        const apyPercent = this.resolveAquaPoolApyPercent({
          apyRaw: this.toPositiveNumber(pool.total_apy),
          tvlUsd: liquidityUsd,
          volume24hUsd,
          feeBps,
        });
        return { apyPercent, liquidityUsd };
      })
      .filter((pool) => pool.apyPercent > 0 && pool.liquidityUsd > 0);

    let apyPercent = 0;
    if (apyPools.length > 0) {
      const totalLiquidityUsd = apyPools.reduce((sum, pool) => sum + pool.liquidityUsd, 0);
      if (totalLiquidityUsd > 0) {
        apyPercent =
          apyPools.reduce((sum, pool) => sum + pool.apyPercent * pool.liquidityUsd, 0) /
          totalLiquidityUsd;
      }
    }

    const tvlUsd = pools
      .map((pool) => ({
        liquidityUsd: this.toPositiveNumber(pool.liquidity_usd),
        tokens: this.resolveAquaPoolTokens(pool.tokens_str),
      }))
      .filter((pool) => pool.liquidityUsd >= 1_000 && pool.liquidityUsd <= 25_000_000)
      .filter((pool) => this.isCanonicalAquaPool(pool.tokens))
      .reduce((sum, pool) => sum + pool.liquidityUsd, 0);

    return {
      tvlUsd: this.roundTo4(tvlUsd),
      apyPercent: this.roundTo4(apyPercent),
    };
  }

  private resolveAquaPoolsPayload(payload: unknown): AquaPool[] {
    if (Array.isArray(payload)) {
      return payload as AquaPool[];
    }

    const items = this.readObjectValue(payload, 'items');
    if (Array.isArray(items)) {
      return items as AquaPool[];
    }

    return [];
  }

  private resolveAquaPoolTokens(rawTokens: unknown): string[] {
    if (!Array.isArray(rawTokens)) {
      return [];
    }

    return rawTokens
      .filter((token): token is string => typeof token === 'string' && token.trim().length > 0)
      .map((token) => token.toLowerCase());
  }

  private resolveAquaVolume24hUsd(pool: AquaPool, tvlUsd: number): number {
    const rawVolume24hUsd = this.toPositiveNumber(pool.volume_24h_usd);
    const rawVolume = rawVolume24hUsd > 0 ? rawVolume24hUsd : this.toPositiveNumber(pool.volume_usd);
    return rawVolume > 0 && tvlUsd > 0 ? Math.min(rawVolume, tvlUsd * 20) : 0;
  }

  private resolveFeeBps(rawFee: number, fallbackBps: number): number {
    if (rawFee <= 0) {
      return fallbackBps;
    }
    if (rawFee <= 1) {
      return Math.round(rawFee * 10_000);
    }
    if (rawFee <= 100) {
      return Math.round(rawFee * 100);
    }
    return Math.round(rawFee);
  }

  private resolveAquaPoolApyPercent(params: {
    apyRaw: number;
    tvlUsd: number;
    volume24hUsd: number;
    feeBps: number;
  }): number {
    const normalizedApy = params.apyRaw > 0 && params.apyRaw <= 1.5 ? params.apyRaw * 100 : params.apyRaw;
    const directApy = this.parseApyPercent(normalizedApy);
    if (directApy > 0) {
      return this.roundTo4(directApy);
    }

    const feeRate = params.feeBps > 0 ? Math.min(params.feeBps / 10_000, 1) : 0;
    if (params.tvlUsd <= 0 || params.volume24hUsd <= 0 || feeRate <= 0) {
      return 0;
    }

    const estimatedApy = ((params.volume24hUsd * feeRate) * 365 * 100) / params.tvlUsd;
    return this.roundTo4(Math.max(0, Math.min(500, estimatedApy)));
  }

  private isCanonicalAquaPool(tokens: string[]): boolean {
    return tokens.some(
      (token) =>
        token.includes('native') ||
        token.includes('xlm') ||
        token.includes('usdc') ||
        token.includes('eurc'),
    );
  }

  private async fetchPhoenixMetrics(): Promise<{ tvlUsd: number; apyPercent: number }> {
    const payload = await this.getJsonWithRetry(this.phoenixPoolsApiUrl, { retries: 2 });
    const pools = this.resolvePhoenixPoolsPayload(payload)
      .map((pool) => ({
        poolAddress: this.stringOr(pool.pool_address, ''),
        feeBps: this.toPositiveNumber(pool.total_fee_bps),
        tvlUsd: this.toPositiveNumber(pool.tvl_usd),
      }))
      .filter((pool) => pool.poolAddress && pool.feeBps > 0 && pool.tvlUsd > 0);

    if (pools.length === 0) {
      return { tvlUsd: 0, apyPercent: 0 };
    }

    const weeklyVolumeResults = await Promise.allSettled(
      pools.map((pool) => this.fetchPhoenixPoolWeeklyVolumeUsd(pool.poolAddress)),
    );

    let totalTvlUsd = 0;
    let totalEstimatedDailyFeesUsd = 0;

    for (let index = 0; index < pools.length; index += 1) {
      const pool = pools[index];
      const volumeResult = weeklyVolumeResults[index];
      const weeklyVolumeUsd = volumeResult.status === 'fulfilled' ? volumeResult.value : 0;
      const dailyVolumeUsd = weeklyVolumeUsd > 0 ? weeklyVolumeUsd / 7 : 0;
      const feeRate = Math.min(pool.feeBps / 10_000, 1);
      const estimatedDailyFeesUsd = dailyVolumeUsd * feeRate;

      totalTvlUsd += pool.tvlUsd;
      totalEstimatedDailyFeesUsd += estimatedDailyFeesUsd;
    }

    const apyPercent =
      totalTvlUsd > 0 && totalEstimatedDailyFeesUsd > 0
        ? (totalEstimatedDailyFeesUsd * 365 * 100) / totalTvlUsd
        : 0;

    return {
      tvlUsd: this.roundTo4(totalTvlUsd),
      apyPercent: this.roundTo4(apyPercent),
    };
  }

  private resolvePhoenixPoolsPayload(payload: unknown): PhoenixStatsPool[] {
    if (Array.isArray(payload)) {
      return payload as PhoenixStatsPool[];
    }

    const data = this.readObjectValue(payload, 'data');
    if (Array.isArray(data)) {
      return data as PhoenixStatsPool[];
    }

    return [];
  }

  private async fetchPhoenixPoolWeeklyVolumeUsd(poolAddress: string): Promise<number> {
    const endpoint = `${this.phoenixVolumeApiBaseUrl}/trading-vol/${encodeURIComponent(poolAddress)}/perweek`;
    const payload = await this.getJsonWithRetry(endpoint, {
      retries: 2,
      timeout: 10000,
    });
    const points = this.resolvePhoenixWeeklyVolumePayload(payload)
      .map((point) => ({
        weekOrder: this.resolvePhoenixWeekOrder(point.week),
        usdVolume: this.toPositiveNumber(point.usdVolume),
      }))
      .filter((point) => point.weekOrder > 0 && point.usdVolume > 0)
      .sort((left, right) => right.weekOrder - left.weekOrder)
      .slice(0, 4);

    if (points.length === 0) {
      return 0;
    }

    return points.reduce((sum, point) => sum + point.usdVolume, 0) / points.length;
  }

  private resolvePhoenixWeeklyVolumePayload(payload: unknown): PhoenixWeeklyVolumePoint[] {
    const tradingVolume = this.readObjectValue(payload, 'tradingVolume');
    if (!Array.isArray(tradingVolume)) {
      return [];
    }

    return tradingVolume as PhoenixWeeklyVolumePoint[];
  }

  private resolvePhoenixWeekOrder(week: PhoenixWeekBucket | undefined): number {
    if (!week || typeof week !== 'object') {
      return 0;
    }

    const year = this.numberOr(week.year, 0);
    const weekNumber = this.numberOr(week.week, 0);
    if (year <= 0 || weekNumber <= 0) {
      return 0;
    }

    return year * 100 + weekNumber;
  }

  private async fetchTemplarMetrics(): Promise<{ tvlUsd: number; apyPercent: number }> {
    const [marketsPayload, snapshotsPayload] = await Promise.all([
      this.getJsonWithRetry(this.templarMarketsApiUrl, { retries: 2 }),
      this.getJsonWithRetry(this.templarSnapshotsApiUrl, { retries: 2 }),
    ]);

    const markets = this.resolveTemplarMarketsPayload(marketsPayload);
    const marketDurations = this.resolveTemplarMarketDurations(marketsPayload);
    const stellarDeployments = new Set(
      markets
        .filter((market) => this.isTemplarStellarMarket(market))
        .map((market) => this.stringOr(market.deployment, ''))
        .filter(Boolean),
    );

    const snapshots = this.resolveTemplarSnapshotsPayload(snapshotsPayload)
      .map((snapshot) => {
        const deployment = this.stringOr(snapshot.deployment, '');
        const annualPercent = this.resolveTemplarAnnualPercent(snapshot, marketDurations.get(deployment));
        const liquidity = this.toPositiveNumber(snapshot.availableBalance);
        return { deployment, annualPercent, liquidity };
      })
      .filter((snapshot) => snapshot.deployment)
      .filter(
        (snapshot) =>
          stellarDeployments.has(snapshot.deployment) || snapshot.deployment.toLowerCase().includes('xlm'),
      );

    const tvlUsd = snapshots.reduce((sum, snapshot) => sum + snapshot.liquidity, 0);
    const apySnapshots = snapshots.filter((snapshot) => snapshot.annualPercent > 0);

    let apyPercent = 0;
    if (apySnapshots.length > 0) {
      const weightedLiquidity = apySnapshots.reduce((sum, snapshot) => sum + snapshot.liquidity, 0);
      if (weightedLiquidity > 0) {
        apyPercent =
          apySnapshots.reduce((sum, snapshot) => sum + snapshot.annualPercent * snapshot.liquidity, 0) /
          weightedLiquidity;
      } else {
        apyPercent =
          apySnapshots.reduce((sum, snapshot) => sum + snapshot.annualPercent, 0) / apySnapshots.length;
      }
    }

    return {
      tvlUsd: this.roundTo4(tvlUsd),
      apyPercent: this.roundTo4(apyPercent),
    };
  }

  private resolveTemplarMarketsPayload(payload: unknown): TemplarMarket[] {
    if (Array.isArray(payload)) {
      return payload as TemplarMarket[];
    }

    const markets = this.readObjectValue(payload, 'markets');
    if (Array.isArray(markets)) {
      return markets as TemplarMarket[];
    }

    return [];
  }

  private resolveTemplarSnapshotsPayload(payload: unknown): TemplarSnapshot[] {
    if (Array.isArray(payload)) {
      return payload as TemplarSnapshot[];
    }

    const snapshots = this.readObjectValue(payload, 'marketSnapshots');
    if (Array.isArray(snapshots)) {
      return snapshots as TemplarSnapshot[];
    }

    return [];
  }

  private resolveTemplarMarketDurations(payload: unknown): Map<string, number> {
    const result = new Map<string, number>();
    const marketDataArray = this.readObjectValue(payload, 'marketDataArray');
    if (!Array.isArray(marketDataArray)) {
      return result;
    }

    for (const entry of marketDataArray as TemplarMarketDataEntry[]) {
      const deployment = this.stringOr(entry.deployment, '');
      if (!deployment) {
        continue;
      }

      const durationMs = this.toPositiveNumber(entry.config?.time_chunk_configuration?.duration_ms);
      if (durationMs > 0) {
        result.set(deployment, durationMs);
      }
    }

    return result;
  }

  private isTemplarStellarMarket(market: TemplarMarket): boolean {
    const deployment = this.stringOr(market.deployment, '').toLowerCase();
    if (deployment.includes('ixlm') || deployment.includes('stellar')) {
      return true;
    }

    const borrowSymbol = this.stringOr(market.borrowMetadata?.symbol, '').toLowerCase();
    const collateralSymbol = this.stringOr(market.collateralMetadata?.symbol, '').toLowerCase();
    return borrowSymbol === 'xlm' || collateralSymbol === 'xlm';
  }

  private resolveTemplarAnnualPercent(snapshot: TemplarSnapshot, durationMs?: number): number {
    const yieldRate = this.toPositiveNumber(snapshot.yield);
    const fallbackRate = this.toPositiveNumber(snapshot.snapshot?.interest_rate);
    const rawRate = yieldRate > 0 ? yieldRate : fallbackRate;
    if (rawRate <= 0) {
      return 0;
    }

    const candidates: number[] = [];

    if (rawRate <= 1) {
      candidates.push(rawRate * 100);

      const intervalMs = durationMs && durationMs > 0 ? durationMs : 10 * 60 * 1000;
      const annualizedIntervalPercent = (rawRate * 365 * 24 * 60 * 60 * 1000 * 100) / intervalMs;
      candidates.push(annualizedIntervalPercent);
    }

    if (rawRate > 1 && rawRate <= 100) {
      candidates.push(rawRate);
    }

    if (rawRate > 1 && rawRate <= 10_000) {
      candidates.push(rawRate / 100);
    }

    const sensible = candidates
      .map((candidate) => (Number.isFinite(candidate) && candidate > 0 ? candidate : 0))
      .filter((candidate) => candidate > 0 && candidate < 250);

    if (sensible.length > 0) {
      return Number(Math.max(...sensible).toFixed(4));
    }

    const fallback = candidates
      .map((candidate) => (Number.isFinite(candidate) && candidate > 0 ? candidate : 0))
      .filter((candidate) => candidate > 0)
      .sort((left, right) => left - right)[0];
    return fallback ? Number(fallback.toFixed(4)) : 0;
  }

  private async fetchSpikoMetrics(): Promise<{ tvlUsd: number; apyPercent: number }> {
    if (this.spikoShareClasses.length === 0) {
      return { tvlUsd: 0, apyPercent: 0 };
    }

    const shareClassResults = await Promise.allSettled(
      this.spikoShareClasses.map(async (shareClass) => {
        const [yieldPayload, totalsPayload] = await Promise.all([
          this.getJsonWithRetry<SpikoYieldResponse>(
            `${this.spikoPublicApiBaseUrl}/share-classes/${encodeURIComponent(shareClass)}/yield`,
            {
              retries: 2,
              timeout: 10000,
            },
          ),
          this.getJsonWithRetry<SpikoTotalsResponse>(
            `${this.spikoPublicApiBaseUrl}/share-classes/${encodeURIComponent(shareClass)}/totals`,
            {
              retries: 2,
              timeout: 10000,
            },
          ),
        ]);

        const apyPercent = this.resolveSpikoApyPercent(yieldPayload);
        const tvlUsd = this.toPositiveNumber(totalsPayload?.totalAssets?.value);
        return { apyPercent, tvlUsd };
      }),
    );

    let weightedApy = 0;
    let weightedTvl = 0;
    let maxApy = 0;
    let totalTvlUsd = 0;

    for (const result of shareClassResults) {
      if (result.status !== 'fulfilled') {
        continue;
      }

      const apyPercent = this.toPositiveNumber(result.value.apyPercent);
      const tvlUsd = this.toPositiveNumber(result.value.tvlUsd);
      if (tvlUsd > 0) {
        totalTvlUsd += tvlUsd;
      }

      if (apyPercent <= 0) {
        continue;
      }

      maxApy = Math.max(maxApy, apyPercent);
      if (tvlUsd > 0) {
        weightedApy += apyPercent * tvlUsd;
        weightedTvl += tvlUsd;
      }
    }

    const apyPercent = weightedTvl > 0 ? weightedApy / weightedTvl : maxApy;
    return {
      tvlUsd: this.roundTo4(totalTvlUsd),
      apyPercent: this.roundTo4(apyPercent),
    };
  }

  private resolveSpikoApyPercent(payload: SpikoYieldResponse): number {
    const candidates = [
      this.normalizeRatePercent(payload.dailyYield),
      this.normalizeRatePercent(payload.weeklyYield),
      this.normalizeRatePercent(payload.monthlyYield),
    ].filter((value) => value > 0);

    if (candidates.length === 0) {
      return 0;
    }

    return this.roundTo4(candidates[0]);
  }

  private async fetchEtherfuseMetrics(): Promise<{ tvlUsd: number; apyPercent: number }> {
    const [bondsPayload, bridgePayload] = await Promise.all([
      this.getJsonWithRetry(this.etherfuseStablebondsApiUrl, { retries: 2 }),
      this.getJsonWithRetry(this.etherfuseBridgingOptionsApiUrl, { retries: 2 }),
    ]);

    const bonds = this.resolveEtherfuseBondsPayload(bondsPayload)
      .map((bond) => ({
        symbol: this.stringOr(bond.mint?.symbol, '').toLowerCase(),
        currency: this.stringOr(bond.currency, '').toLowerCase(),
        supply: this.toPositiveNumber(bond.mint?.supply),
        unitPrice: this.toPositiveNumber(bond.mint?.currentTokenAmount),
        ratePercent: this.resolveEtherfuseRatePercent(bond),
      }))
      .filter((bond) => bond.symbol);

    const tvlUsd = bonds
      .filter((bond) => bond.currency === 'usd')
      .reduce((sum, bond) => sum + bond.supply * (bond.unitPrice > 0 ? bond.unitPrice : 1), 0);

    const ratedBonds = bonds.filter((bond) => bond.ratePercent > 0);
    if (ratedBonds.length === 0) {
      return {
        tvlUsd: this.roundTo4(tvlUsd),
        apyPercent: 0,
      };
    }

    const stellarSupplies = this.resolveEtherfuseStellarSupplies(bridgePayload);
    let weightedRate = 0;
    let totalSupply = 0;

    for (const bond of ratedBonds) {
      const supply = stellarSupplies.get(bond.symbol) || 0;
      if (supply <= 0) {
        continue;
      }

      weightedRate += bond.ratePercent * supply;
      totalSupply += supply;
    }

    const apyPercent =
      totalSupply > 0
        ? weightedRate / totalSupply
        : ratedBonds.reduce((sum, bond) => sum + bond.ratePercent, 0) / ratedBonds.length;

    return {
      tvlUsd: this.roundTo4(tvlUsd),
      apyPercent: this.roundTo4(apyPercent),
    };
  }

  private resolveEtherfuseBondsPayload(payload: unknown): EtherfuseBond[] {
    if (Array.isArray(payload)) {
      return payload as EtherfuseBond[];
    }

    const bonds = this.readObjectValue(payload, 'bonds');
    if (Array.isArray(bonds)) {
      return bonds as EtherfuseBond[];
    }

    return [];
  }

  private resolveEtherfuseRatePercent(bond: EtherfuseBond): number {
    const interestRateBps = this.toPositiveNumber(bond.currentIssuance?.interestRateBps);
    if (interestRateBps > 0) {
      return Number((interestRateBps / 100).toFixed(4));
    }

    const rawInterestRate = this.toPositiveNumber(bond.interestRate);
    if (rawInterestRate <= 0) {
      return 0;
    }

    if (rawInterestRate <= 1) {
      return Number((rawInterestRate * 100).toFixed(4));
    }

    if (rawInterestRate <= 100) {
      return Number(rawInterestRate.toFixed(4));
    }

    if (rawInterestRate <= 10_000) {
      return Number((rawInterestRate / 100).toFixed(4));
    }

    return 0;
  }

  private resolveEtherfuseStellarSupplies(payload: unknown): Map<string, number> {
    const result = new Map<string, number>();
    if (typeof payload !== 'object' || payload === null) {
      return result;
    }

    for (const [symbol, entryRaw] of Object.entries(payload as EtherfuseBridgingOptionsResponse)) {
      const symbolKey = this.stringOr(symbol, '').toLowerCase();
      if (!symbolKey || typeof entryRaw !== 'object' || entryRaw === null) {
        continue;
      }

      const stellar = this.readObjectValue(entryRaw, 'stellar');
      const totalSupply = this.toPositiveNumber(this.readObjectValue(stellar, 'totalSupply'));
      if (totalSupply > 0) {
        result.set(symbolKey, totalSupply);
      }
    }

    return result;
  }

  private estimateLedgerStatsFromLedgers(payload: unknown): LedgerEstimate {
    const embedded = this.readObjectValue(payload, '_embedded');
    const recordsRaw = this.readObjectValue(embedded, 'records');
    if (!Array.isArray(recordsRaw) || recordsRaw.length === 0) {
      return { txCount24h: 0, operationCount24h: 0, baseFeeStroops: 100 };
    }

    const records = (recordsRaw as HorizonLedgerRecord[])
      .map((record) => this.parseLedgerRecord(record))
      .filter((record): record is { closedAt: number; txCount: number; opCount: number; baseFee: number } =>
        Boolean(record),
      );

    if (records.length === 0) {
      return { txCount24h: 0, operationCount24h: 0, baseFeeStroops: 100 };
    }

    const newest = records[0];
    const oldest = records[records.length - 1];
    let windowSec = Math.abs(newest.closedAt - oldest.closedAt) / 1000;
    if (windowSec < 30) {
      windowSec = Math.max(records.length * 5, 30);
    }

    const txInWindow = records.reduce((sum, record) => sum + record.txCount, 0);
    const opsInWindow = records.reduce((sum, record) => sum + record.opCount, 0);

    const txCount24h = Math.round((txInWindow / windowSec) * 86_400);
    const operationCount24h = Math.round((opsInWindow / windowSec) * 86_400);

    return {
      txCount24h: Math.max(0, txCount24h),
      operationCount24h: Math.max(0, operationCount24h),
      baseFeeStroops: Math.max(1, newest.baseFee),
    };
  }

  private parseLedgerRecord(record: HorizonLedgerRecord): {
    closedAt: number;
    txCount: number;
    opCount: number;
    baseFee: number;
  } | null {
    const closedAtIso = this.stringOr(record.closed_at, '');
    if (!closedAtIso) {
      return null;
    }

    const closedAt = Date.parse(closedAtIso);
    if (!Number.isFinite(closedAt)) {
      return null;
    }

    return {
      closedAt,
      txCount: this.toPositiveNumber(record.successful_transaction_count),
      opCount: this.toPositiveNumber(record.operation_count),
      baseFee: this.toPositiveNumber(record.base_fee_in_stroops) || 100,
    };
  }

  private resolveContractCallRatio(payload: unknown): number {
    const embedded = this.readObjectValue(payload, '_embedded');
    const records = this.readObjectValue(embedded, 'records');
    if (!Array.isArray(records) || records.length === 0) {
      return 0;
    }

    let invokeCount = 0;
    for (const record of records) {
      const type = this.stringOr(this.readObjectValue(record, 'type'), '');
      if (type === 'invoke_host_function') {
        invokeCount += 1;
      }
    }

    return invokeCount / records.length;
  }

  private readObjectValue(source: unknown, key: string): unknown {
    if (typeof source !== 'object' || source === null) {
      return undefined;
    }

    return (source as Record<string, unknown>)[key];
  }

  private numberOr(value: unknown, fallback: number): number {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  private resolvePositiveIntEnv(
    rawValue: string | undefined,
    fallback: number,
    min: number,
    max: number,
  ): number {
    const parsed = Number.parseInt((rawValue || '').trim(), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return fallback;
    }

    return Math.min(Math.max(parsed, min), max);
  }

  private extractFiniteNumber(value: unknown): number | null {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  private toPositiveNumber(value: unknown): number {
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  private parseApyPercent(value: unknown): number {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0 || parsed > 500) {
      return 0;
    }
    return parsed;
  }

  private normalizeRatePercent(value: unknown): number {
    const raw = Number(value);
    if (!Number.isFinite(raw) || raw <= 0) {
      return 0;
    }

    if (raw <= 1) {
      return raw * 100;
    }

    if (raw <= 100) {
      return raw;
    }

    if (raw <= 10_000) {
      return raw / 100;
    }

    return 0;
  }

  private roundTo4(value: number): number {
    if (!Number.isFinite(value)) {
      return 0;
    }
    return Number(value.toFixed(4));
  }

  private stringOr(value: unknown, fallback: string): string {
    if (typeof value === 'number' && Number.isFinite(value)) {
      return String(value);
    }

    return typeof value === 'string' && value.trim() ? value : fallback;
  }

  private optionalString(value: unknown): string | undefined {
    return typeof value === 'string' && value.trim() ? value : undefined;
  }

  private toIdentifierString(value: unknown): string | undefined {
    if (typeof value === 'string' && value.trim()) {
      return value.trim();
    }
    if (typeof value === 'number' && Number.isFinite(value)) {
      return String(value);
    }
    if (value && typeof value === 'object') {
      const asString = String(value);
      if (asString && asString !== '[object Object]') {
        return asString;
      }
    }
    return undefined;
  }

  private errorMessage(error: unknown): string {
    if (axios.isAxiosError(error)) {
      const method = this.stringOr(error.config?.method, 'get').toUpperCase();
      const url = this.stringOr(error.config?.url, '');
      const status = error.response?.status;
      const statusText = this.stringOr(error.response?.statusText, '');
      const code = this.stringOr(error.code, '');
      const message = this.stringOr(error.message, 'request failed');

      const requestLabel = url ? `${method} ${url}` : method;
      const statusLabel = status ? `status=${status}${statusText ? ` ${statusText}` : ''}` : '';
      const codeLabel = code ? `code=${code}` : '';
      const context = [requestLabel, statusLabel, codeLabel].filter(Boolean).join(', ');
      return context ? `${message} (${context})` : message;
    }

    if (error instanceof Error && error.message) {
      return error.message;
    }

    if (typeof error === 'string' && error.trim()) {
      return error;
    }

    if (typeof error === 'object' && error !== null) {
      const message = this.readObjectValue(error, 'message');
      if (typeof message === 'string' && message.trim()) {
        return message;
      }

      try {
        const serialized = JSON.stringify(error);
        if (serialized && serialized !== '{}') {
          return serialized;
        }
      } catch {
        return 'unknown object error';
      }
    }

    return 'unknown error';
  }
}
