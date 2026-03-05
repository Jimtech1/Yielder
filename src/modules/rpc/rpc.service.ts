import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { MessageEvent } from '@nestjs/common/interfaces/http/message-event.interface';
import axios from 'axios';
import { ethers } from 'ethers';
import { Observable, Subscription, from, interval, of } from 'rxjs';
import { catchError, map, shareReplay, startWith, switchMap } from 'rxjs/operators';
import * as StellarSdk from 'stellar-sdk';
import * as https from 'https';

type RpcCall = {
  method: string;
  params?: unknown[];
};

type RpcCallExecutionOptions = {
  timeoutMs?: number;
  maxRetries?: number;
  endpointLimit?: number;
};

type JsonRpcRequest = {
  jsonrpc: '2.0';
  id: number;
  method: string;
  params: unknown[];
};

type JsonRpcSuccess = {
  jsonrpc: '2.0';
  id: number;
  result: unknown;
};

type JsonRpcErrorObject = {
  code: number;
  message: string;
  data?: unknown;
};

type JsonRpcFailure = {
  jsonrpc: '2.0';
  id: number | null;
  error: JsonRpcErrorObject;
};

type JsonRpcResponse = JsonRpcSuccess | JsonRpcFailure;

type ChainStatus = {
  chain: string;
  status: 'connected' | 'degraded' | 'disconnected' | 'unknown';
  endpoint?: string;
  primaryEndpoint?: string;
  configuredEndpoints?: string[];
  oneRpcConfigured?: boolean;
  oneRpcActive?: boolean;
  blockNumber?: number;
  slot?: number;
  warning?: string;
  cached?: boolean;
};

type CachedChainStatus = {
  timestamp: number;
  value: ChainStatus;
};

type ChainHeadEvent = {
  chain: string;
  source: 'websocket' | 'polling';
  endpoint?: string;
  blockNumber?: number;
  slot?: number;
  warning?: string;
  timestamp: string;
};

type ChainMetric = {
  calls: number;
  successes: number;
  failures: number;
  retries: number;
  fallbackSwitches: number;
  rateLimitedErrors: number;
};

type RpcUsageMetrics = {
  totalCalls: number;
  successfulCalls: number;
  failedCalls: number;
  retryCount: number;
  fallbackSwitches: number;
  rateLimitedErrors: number;
  batchRequests: number;
  batchedCalls: number;
  cacheHits: number;
  staleCacheFallbacks: number;
  wsSubscriptions: number;
  wsErrors: number;
  lastUpdated: string;
  perChain: Record<string, ChainMetric>;
};

type RpcError = Error & {
  rpcCode?: number;
};

type MultiSourceUrlListOptions = {
  appendFallback?: boolean;
};

@Injectable()
export class RpcService {
  private readonly logger = new Logger(RpcService.name);
  private readonly horizonTestnetUrls: string[];
  private readonly horizonMainnetUrls: string[];
  private readonly axelarRpcUrls: string[];
  private readonly axelarWsUrls: string[];
  private readonly requestTimeoutMs: number;
  private readonly maxRetries: number;
  private readonly statusRequestTimeoutMs: number;
  private readonly statusMaxRetries: number;
  private readonly statusEndpointLimit: number;
  private readonly backoffBaseMs: number;
  private readonly statusCacheTtlMs: number;
  private readonly headPollIntervalMs: number;
  private readonly tlsRecoveryEnabled: boolean;
  private readonly forceIpv4: boolean;
  private readonly disableProxyOnTlsRecovery: boolean;
  private readonly hardenedHttpsAgent: https.Agent;

  private readonly statusCache = new Map<string, CachedChainStatus>();
  private readonly chainHeadStreams = new Map<string, Observable<ChainHeadEvent>>();
  private requestId = 1;
  private readonly metrics: RpcUsageMetrics = {
    totalCalls: 0,
    successfulCalls: 0,
    failedCalls: 0,
    retryCount: 0,
    fallbackSwitches: 0,
    rateLimitedErrors: 0,
    batchRequests: 0,
    batchedCalls: 0,
    cacheHits: 0,
    staleCacheFallbacks: 0,
    wsSubscriptions: 0,
    wsErrors: 0,
    lastUpdated: new Date().toISOString(),
    perChain: {},
  };

  constructor() {
    const configuredNetwork = this.resolveStellarNetwork(process.env.STELLAR_NETWORK);
    const testnetFallback =
      process.env.STELLAR_HORIZON_URL_TESTNET ||
      (configuredNetwork === 'testnet' ? process.env.STELLAR_HORIZON_URL : undefined) ||
      'https://horizon-testnet.stellar.org';
    const mainnetFallback =
      process.env.STELLAR_HORIZON_URL_MAINNET ||
      (configuredNetwork === 'mainnet' || configuredNetwork === 'public'
        ? process.env.STELLAR_HORIZON_URL
        : undefined) ||
      'https://horizon.stellar.org';
    const legacyHorizonUrls = this.resolveUrlList(process.env.STELLAR_HORIZON_URLS, []);
    const testnetUrls = this.resolveUrlList(process.env.STELLAR_HORIZON_URLS_TESTNET, [
      testnetFallback,
    ]);
    const mainnetUrls = this.resolveUrlList(process.env.STELLAR_HORIZON_URLS_MAINNET, [
      mainnetFallback,
    ]);
    this.horizonTestnetUrls =
      legacyHorizonUrls.length > 0 && configuredNetwork !== 'mainnet' && configuredNetwork !== 'public'
        ? legacyHorizonUrls
        : testnetUrls;
    this.horizonMainnetUrls =
      legacyHorizonUrls.length > 0 && (configuredNetwork === 'mainnet' || configuredNetwork === 'public')
        ? legacyHorizonUrls
        : mainnetUrls;
    const appendAxelarDefaultFallbacks = this.resolveBoolean(
      process.env.AXELAR_RPC_APPEND_DEFAULT_FALLBACKS,
      true,
    );
    this.axelarRpcUrls = this.resolveMultiSourceUrlList(
      [
        process.env.AXELAR_RPC_URLS,
        process.env.RPC_URLS_AXELAR,
        process.env.AXELAR_RPC_BACKUP_URLS,
        process.env.RPC_BACKUP_URLS_AXELAR,
      ],
      [
        process.env.AXELAR_RPC_URL,
        process.env.RPC_URL_AXELAR,
        'https://1rpc.io/axelar-rpc',
        'https://axelar-rpc.publicnode.com',
      ],
      { appendFallback: appendAxelarDefaultFallbacks },
    );
    this.axelarWsUrls = this.resolveUrlList(process.env.AXELAR_WS_URLS, []);
    this.requestTimeoutMs = this.resolvePositiveInt(process.env.RPC_TIMEOUT_MS, 12000, 2000);
    this.maxRetries = this.resolvePositiveInt(process.env.RPC_MAX_RETRIES, 2, 0);
    this.statusRequestTimeoutMs = this.resolvePositiveInt(process.env.RPC_STATUS_TIMEOUT_MS, 2500, 500);
    this.statusMaxRetries = this.resolvePositiveInt(process.env.RPC_STATUS_MAX_RETRIES, 1, 0);
    this.statusEndpointLimit = this.resolvePositiveInt(process.env.RPC_STATUS_MAX_ENDPOINTS, 2, 1);
    this.backoffBaseMs = this.resolvePositiveInt(process.env.RPC_BACKOFF_BASE_MS, 250, 50);
    this.statusCacheTtlMs = this.resolvePositiveInt(process.env.RPC_STATUS_CACHE_TTL_MS, 10000, 1000);
    this.headPollIntervalMs = this.resolvePositiveInt(process.env.RPC_HEAD_POLL_INTERVAL_MS, 5000, 1000);
    this.tlsRecoveryEnabled = this.resolveBoolean(
      process.env.RPC_TLS_RECOVERY_ENABLED,
      true,
    );
    this.forceIpv4 = this.resolveBoolean(process.env.RPC_FORCE_IPV4, true);
    this.disableProxyOnTlsRecovery = this.resolveBoolean(
      process.env.RPC_DISABLE_PROXY_ON_TLS_RECOVERY,
      true,
    );
    this.hardenedHttpsAgent = new https.Agent({
      keepAlive: true,
      family: this.forceIpv4 ? 4 : undefined,
      minVersion: 'TLSv1.2',
    });
  }

  async executeChainCall(chain: string, method: string, params: unknown[] = []) {
    const normalizedChain = chain.toLowerCase();
    if (!method || !method.trim()) {
      throw new BadRequestException('RPC method is required');
    }

    this.recordCall(normalizedChain);
    try {
      const { result, endpoint } = await this.performJsonRpcCall(normalizedChain, {
        method,
        params,
      });

      this.recordSuccess(normalizedChain);
      return {
        chain: normalizedChain,
        method,
        endpoint,
        result,
      };
    } catch (error: unknown) {
      this.recordFailure(normalizedChain);
      throw new BadRequestException({
        message: `Failed to execute ${normalizedChain} RPC call`,
        details: this.getErrorMessage(error),
      });
    }
  }

  async executeBatchChainCalls(chain: string, calls: RpcCall[]) {
    const normalizedChain = chain.toLowerCase();
    if (!Array.isArray(calls) || calls.length === 0) {
      throw new BadRequestException('Batch calls payload is empty');
    }

    this.recordCall(normalizedChain);
    this.metrics.batchRequests += 1;
    this.metrics.batchedCalls += calls.length;
    this.touchMetrics();

    const requests = calls.map((call) => {
      if (!call.method || !call.method.trim()) {
        throw new BadRequestException('Each batch call must include a method');
      }

      return this.toJsonRpcRequest(call.method, call.params || []);
    });

    try {
      const { responses, endpoint } = await this.performBatchJsonRpcCall(normalizedChain, requests);
      const responsesById = new Map<number, JsonRpcResponse>();
      for (const response of responses) {
        if (typeof response.id === 'number') {
          responsesById.set(response.id, response);
        }
      }

      const result = requests.map((request) => {
        const response = responsesById.get(request.id);
        if (!response) {
          return {
            id: request.id,
            method: request.method,
            error: {
              code: -32000,
              message: 'Missing RPC response entry',
            },
          };
        }

        if ('error' in response) {
          return {
            id: request.id,
            method: request.method,
            error: response.error,
          };
        }

        return {
          id: request.id,
          method: request.method,
          result: response.result,
        };
      });

      this.recordSuccess(normalizedChain);
      return {
        chain: normalizedChain,
        endpoint,
        count: requests.length,
        results: result,
      };
    } catch (error: unknown) {
      this.recordFailure(normalizedChain);
      throw new BadRequestException({
        message: `Failed to execute ${normalizedChain} batch RPC call`,
        details: this.getErrorMessage(error),
      });
    }
  }

  async getChainStatus(chain: string): Promise<ChainStatus> {
    const normalizedChain = chain.toLowerCase();
    const cached = this.statusCache.get(normalizedChain);
    if (cached && Date.now() - cached.timestamp < this.statusCacheTtlMs) {
      this.metrics.cacheHits += 1;
      this.touchMetrics();
      return this.withEndpointDiagnostics(normalizedChain, {
        ...cached.value,
        cached: true,
      });
    }

    this.recordCall(normalizedChain);
    try {
      const status = this.withEndpointDiagnostics(
        normalizedChain,
        await this.fetchLiveChainStatus(normalizedChain),
      );
      this.statusCache.set(normalizedChain, {
        timestamp: Date.now(),
        value: status,
      });

      this.recordSuccess(normalizedChain);
      return status;
    } catch (error: unknown) {
      const stale = this.statusCache.get(normalizedChain);
      if (stale) {
        this.metrics.staleCacheFallbacks += 1;
        this.touchMetrics();
        this.recordSuccess(normalizedChain);
        const staleStatus = this.withEndpointDiagnostics(normalizedChain, stale.value);
        return {
          ...staleStatus,
          status: 'degraded',
          cached: true,
          warning: `Using stale cached status because live RPC check failed: ${this.getErrorMessage(error)}`,
        };
      }

      this.recordFailure(normalizedChain);
      return this.withEndpointDiagnostics(normalizedChain, {
        chain: normalizedChain,
        status: 'disconnected',
        warning: this.getErrorMessage(error),
      });
    }
  }

  streamChainHeads(chain: string): Observable<MessageEvent> {
    const stream = this.getOrCreateChainHeadStream(chain.toLowerCase());
    return stream.pipe(map((event) => ({ data: event } as MessageEvent)));
  }

  getMetrics(): RpcUsageMetrics {
    return {
      ...this.metrics,
      perChain: { ...this.metrics.perChain },
    };
  }

  async broadcastTransaction(transactionXdr: string, network?: string) {
    const resolvedNetwork = this.resolveRequestedStellarNetwork(network);
    const transaction = new StellarSdk.Transaction(
      transactionXdr,
      this.getNetworkPassphrase(resolvedNetwork),
    );
    this.recordCall('stellar');

    try {
      const { response, endpoint } = await this.submitTransactionWithFallback(
        transaction,
        resolvedNetwork,
      );

      this.recordSuccess('stellar');
      return {
        hash: response.hash,
        ledger: response.ledger,
        successful: response.successful,
        resultXdr: response.result_xdr,
        endpoint,
        network: resolvedNetwork,
      };
    } catch (error: unknown) {
      this.recordFailure('stellar');
      const details = this.extractStellarError(error);
      this.logger.error(`Broadcast failed: ${JSON.stringify(details)}`);
      throw new BadRequestException({
        message: 'Failed to broadcast Stellar transaction',
        details,
      });
    }
  }

  private async fetchLiveChainStatus(chain: string): Promise<ChainStatus> {
    const statusCallOptions: RpcCallExecutionOptions = {
      timeoutMs: this.statusRequestTimeoutMs,
      maxRetries: this.statusMaxRetries,
      endpointLimit: this.statusEndpointLimit,
    };

    if (chain === 'axelar') {
      try {
        const { result, endpoint } = await this.performJsonRpcCall(chain, {
          method: 'status',
          params: [],
        }, statusCallOptions);

        const blockNumber = this.parseAxelarTendermintBlockNumber(result);
        if (blockNumber <= 0) {
          throw new Error('Axelar status response missing latest block height');
        }

        return {
          chain,
          status: 'connected',
          endpoint,
          blockNumber,
        };
      } catch (statusError: unknown) {
        const { result, endpoint } = await this.performJsonRpcCall(chain, {
          method: 'eth_blockNumber',
          params: [],
        }, statusCallOptions);

        const blockNumber = this.parseEvmBlockNumber(result);
        if (blockNumber <= 0) {
          throw statusError;
        }

        return {
          chain,
          status: 'connected',
          endpoint,
          blockNumber,
        };
      }
    }

    return {
      chain,
      status: 'unknown',
    };
  }

  private getOrCreateChainHeadStream(chain: string): Observable<ChainHeadEvent> {
    const existing = this.chainHeadStreams.get(chain);
    if (existing) {
      return existing;
    }

    const stream =
      chain === 'axelar' ? this.createAxelarHeadStream(chain) : this.createPollingHeadStream(chain);
    const sharedStream = stream.pipe(shareReplay({ bufferSize: 1, refCount: true }));
    this.chainHeadStreams.set(chain, sharedStream);

    return sharedStream;
  }

  private createAxelarHeadStream(chain: string): Observable<ChainHeadEvent> {
    if (this.axelarWsUrls.length === 0) {
      return this.createPollingHeadStream(chain, 'AXELAR_WS_URLS not configured, polling fallback active.');
    }

    return new Observable<ChainHeadEvent>((subscriber) => {
      this.metrics.wsSubscriptions += 1;
      this.touchMetrics();

      let disposed = false;
      let wsProvider: ethers.WebSocketProvider | null = null;
      let pollingSubscription: Subscription | null = null;

      const startPollingFallback = (warning: string) => {
        if (pollingSubscription) {
          return;
        }

        pollingSubscription = this.createPollingHeadStream(chain, warning).subscribe({
          next: (event) => subscriber.next(event),
          error: (error: unknown) => subscriber.error(error),
        });
      };

      const connect = (index: number) => {
        if (disposed) {
          return;
        }

        if (index >= this.axelarWsUrls.length) {
          startPollingFallback('All Axelar WebSocket endpoints failed, polling fallback active.');
          return;
        }

        const url = this.axelarWsUrls[index];
        try {
          wsProvider = new ethers.WebSocketProvider(url);
        } catch (error: unknown) {
          this.metrics.wsErrors += 1;
          this.touchMetrics();
          this.logger.warn(`Failed to initialize WebSocket provider for ${url}: ${this.getErrorMessage(error)}`);
          this.recordFallbackSwitch(chain);
          connect(index + 1);
          return;
        }

        const provider = wsProvider;
        const handleBlock = (blockNumber: number) => {
          const status: ChainStatus = {
            chain,
            status: 'connected',
            endpoint: url,
            blockNumber,
          };

          this.statusCache.set(chain, {
            timestamp: Date.now(),
            value: status,
          });

          subscriber.next({
            chain,
            source: 'websocket',
            endpoint: url,
            blockNumber,
            timestamp: new Date().toISOString(),
          });
        };

        const handleError = (error: unknown) => {
          this.metrics.wsErrors += 1;
          this.touchMetrics();
          this.logger.warn(`Axelar WebSocket error on ${url}: ${this.getErrorMessage(error)}`);
          this.recordFallbackSwitch(chain);
          provider.destroy();
          connect(index + 1);
        };

        provider.on('block', handleBlock);
        provider.on('error', handleError as never);
      };

      connect(0);

      return () => {
        disposed = true;
        if (pollingSubscription) {
          pollingSubscription.unsubscribe();
        }
        if (wsProvider) {
          wsProvider.destroy();
        }
      };
    });
  }

  private createPollingHeadStream(chain: string, warning?: string): Observable<ChainHeadEvent> {
    return interval(this.headPollIntervalMs).pipe(
      startWith(0),
      switchMap(() =>
        from(this.getChainStatus(chain)).pipe(
          map((status) => ({
            chain,
            source: 'polling' as const,
            endpoint: status.endpoint,
            blockNumber: status.blockNumber,
            slot: status.slot,
            warning: warning || status.warning,
            timestamp: new Date().toISOString(),
          })),
          catchError((error: unknown) =>
            of({
              chain,
              source: 'polling' as const,
              warning: `Polling failed: ${this.getErrorMessage(error)}`,
              timestamp: new Date().toISOString(),
            }),
          ),
        ),
      ),
    );
  }

  private async submitTransactionWithFallback(
    transaction: StellarSdk.Transaction,
    network?: string,
  ): Promise<{
    response: StellarSdk.Horizon.HorizonApi.SubmitTransactionResponse;
    endpoint: string;
  }> {
    const endpoints = this.getStellarHorizonUrls(network);

    const result = await this.withFallback(
      'stellar',
      endpoints,
      async (endpoint) => {
        const server = new StellarSdk.Horizon.Server(endpoint);
        const response = await server.submitTransaction(transaction);
        return {
          response,
          endpoint,
        };
      },
      (error) => this.isRetryableError(error),
    );

    return result;
  }

  private async performJsonRpcCall(
    chain: string,
    call: RpcCall,
    options?: RpcCallExecutionOptions,
  ): Promise<{ result: unknown; endpoint: string }> {
    const endpoints = this.getHttpEndpoints(chain);
    if (endpoints.length === 0) {
      throw new Error(`No RPC endpoints configured for ${chain}`);
    }

    const request = this.toJsonRpcRequest(call.method, call.params || []);
    const timeoutMs = this.resolveTimeoutMs(options?.timeoutMs);
    return this.withFallback(
      chain,
      endpoints,
      async (endpoint) => {
        const response = await this.postJsonRpcWithTlsRecovery<JsonRpcResponse>(
          endpoint,
          request,
          timeoutMs,
        );

        const data = response.data;
        if (!data || Array.isArray(data)) {
          throw new Error(`Invalid JSON-RPC response from ${endpoint}`);
        }
        if ('error' in data) {
          throw this.createRpcError(data.error.code, data.error.message);
        }

        return {
          result: data.result,
          endpoint,
        };
      },
      (error) => this.isRetryableError(error),
      options,
    );
  }

  private async performBatchJsonRpcCall(
    chain: string,
    requests: JsonRpcRequest[],
  ): Promise<{ responses: JsonRpcResponse[]; endpoint: string }> {
    const endpoints = this.getHttpEndpoints(chain);
    if (endpoints.length === 0) {
      throw new Error(`No RPC endpoints configured for ${chain}`);
    }

    return this.withFallback(
      chain,
      endpoints,
      async (endpoint) => {
        const response = await this.postJsonRpcWithTlsRecovery<JsonRpcResponse[]>(
          endpoint,
          requests,
          this.requestTimeoutMs,
        );

        if (!Array.isArray(response.data)) {
          throw new Error(`Invalid batch JSON-RPC response from ${endpoint}`);
        }

        const allRateLimited =
          response.data.length > 0 &&
          response.data.every(
            (item) => 'error' in item && this.isRateLimitRpcCode(item.error.code),
          );
        if (allRateLimited) {
          throw this.createRpcError(-32005, 'Batch request rate-limited');
        }

        return {
          responses: response.data,
          endpoint,
        };
      },
      (error) => this.isRetryableError(error),
    );
  }

  private async withFallback<T>(
    chain: string,
    endpoints: string[],
    request: (endpoint: string) => Promise<T>,
    shouldRetry: (error: unknown) => boolean,
    options?: RpcCallExecutionOptions,
  ): Promise<T> {
    let lastError: unknown = new Error('Unknown RPC failure');
    const maxRetries = this.resolveMaxRetries(options?.maxRetries);
    const endpointLimit = this.resolveEndpointLimit(options?.endpointLimit, endpoints.length);
    const effectiveEndpoints = endpoints.slice(0, endpointLimit);

    for (let endpointIndex = 0; endpointIndex < effectiveEndpoints.length; endpointIndex += 1) {
      const endpoint = effectiveEndpoints[endpointIndex];
      if (endpointIndex > 0) {
        this.recordFallbackSwitch(chain);
      }

      const attempts = maxRetries + 1;
      for (let attempt = 1; attempt <= attempts; attempt += 1) {
        try {
          return await request(endpoint);
        } catch (error: unknown) {
          lastError = error;
          if (this.isRateLimitError(error)) {
            this.recordRateLimited(chain);
          }

          const canRetry = attempt < attempts && shouldRetry(error);
          if (!canRetry) {
            break;
          }

          this.recordRetry(chain);
          await this.sleep(this.backoffDelayMs(attempt));
        }
      }
    }

    throw lastError;
  }

  private toJsonRpcRequest(method: string, params: unknown[]): JsonRpcRequest {
    const id = this.requestId;
    this.requestId += 1;

    return {
      jsonrpc: '2.0',
      id,
      method,
      params,
    };
  }

  private getHttpEndpoints(chain: string): string[] {
    if (chain === 'axelar') {
      return this.axelarRpcUrls;
    }

    const envSuffix = this.toEnvKey(chain);
    const primaryList = this.resolveUrlList(process.env[`RPC_URLS_${envSuffix}`], []);
    const backupList = this.resolveUrlList(process.env[`RPC_BACKUP_URLS_${envSuffix}`], []);
    const legacySingle = (process.env[`RPC_URL_${envSuffix}`] || '').trim();

    return [...new Set([...primaryList, ...(legacySingle ? [legacySingle] : []), ...backupList])];
  }

  private withEndpointDiagnostics(chain: string, status: ChainStatus): ChainStatus {
    const configuredEndpoints = this.getConfiguredEndpoints(chain);
    const normalizedActiveEndpoint =
      typeof status.endpoint === 'string' && status.endpoint.trim()
        ? status.endpoint.trim()
        : undefined;
    const oneRpcConfigured = configuredEndpoints.some((endpoint) =>
      this.isOneRpcEndpoint(endpoint),
    );
    const oneRpcActive = normalizedActiveEndpoint
      ? this.isOneRpcEndpoint(normalizedActiveEndpoint)
      : false;

    return {
      ...status,
      endpoint: normalizedActiveEndpoint,
      primaryEndpoint: configuredEndpoints[0],
      configuredEndpoints,
      oneRpcConfigured,
      oneRpcActive,
    };
  }

  private getConfiguredEndpoints(chain: string): string[] {
    if (chain === 'stellar') {
      return this.getStellarHorizonUrls();
    }

    return this.getHttpEndpoints(chain);
  }

  private isOneRpcEndpoint(endpoint: string): boolean {
    const normalized = endpoint.trim().toLowerCase();
    if (!normalized) {
      return false;
    }

    try {
      const hostname = new URL(normalized).hostname.toLowerCase();
      return hostname === '1rpc.io' || hostname.endsWith('.1rpc.io');
    } catch {
      return normalized.includes('1rpc.io');
    }
  }

  private resolveUrlList(rawValue: string | undefined, fallback: string[]): string[] {
    const parsed = this.parseUrlCsv(rawValue);
    const sanitizedFallback = fallback.map((value) => value.trim()).filter((value) => value.length > 0);

    const finalList = parsed.length > 0 ? parsed : sanitizedFallback;
    return [...new Set(finalList)];
  }

  private resolveMultiSourceUrlList(
    rawValues: Array<string | undefined>,
    fallbackValues: Array<string | undefined>,
    options?: MultiSourceUrlListOptions,
  ): string[] {
    const parsed = rawValues.flatMap((value) => this.parseUrlCsv(value));
    const fallback = fallbackValues
      .map((value) => (value || '').trim())
      .filter((value) => value.length > 0);

    if (options?.appendFallback) {
      return [...new Set([...parsed, ...fallback])];
    }

    if (parsed.length > 0) {
      return [...new Set(parsed)];
    }

    return [...new Set(fallback)];
  }

  private parseUrlCsv(rawValue: string | undefined): string[] {
    return (rawValue || '')
      .split(',')
      .map((value) => value.trim())
      .filter((value) => value.length > 0);
  }

  private toEnvKey(chain: string): string {
    return chain.replace(/[^a-zA-Z0-9]/g, '_').toUpperCase();
  }

  private resolvePositiveInt(rawValue: string | undefined, fallback: number, min: number): number {
    const parsed = Number(rawValue);
    if (!Number.isFinite(parsed)) {
      return fallback;
    }

    return Math.max(min, Math.floor(parsed));
  }

  private resolveBoolean(rawValue: string | undefined, fallback: boolean): boolean {
    const normalized = (rawValue || '').trim().toLowerCase();
    if (!normalized) {
      return fallback;
    }

    if (['1', 'true', 'yes', 'on'].includes(normalized)) {
      return true;
    }
    if (['0', 'false', 'no', 'off'].includes(normalized)) {
      return false;
    }

    return fallback;
  }

  private resolveTimeoutMs(timeoutMs?: number): number {
    if (!Number.isFinite(timeoutMs)) {
      return this.requestTimeoutMs;
    }

    return Math.max(500, Math.floor(timeoutMs as number));
  }

  private resolveMaxRetries(maxRetries?: number): number {
    if (!Number.isFinite(maxRetries)) {
      return this.maxRetries;
    }

    return Math.max(0, Math.floor(maxRetries as number));
  }

  private resolveEndpointLimit(endpointLimit: number | undefined, endpointCount: number): number {
    if (!Number.isFinite(endpointLimit)) {
      return Math.max(1, endpointCount);
    }

    return Math.max(1, Math.min(Math.floor(endpointLimit as number), endpointCount));
  }

  private parseEvmBlockNumber(value: unknown): number {
    if (typeof value === 'string' && value.startsWith('0x')) {
      return Number.parseInt(value, 16);
    }

    return this.parseNumber(value);
  }

  private parseAxelarTendermintBlockNumber(value: unknown): number {
    if (!value || typeof value !== 'object') {
      return 0;
    }

    const payload = value as Record<string, unknown>;
    const syncInfo = payload.sync_info;
    if (!syncInfo || typeof syncInfo !== 'object') {
      return 0;
    }

    const latestHeight = (syncInfo as Record<string, unknown>).latest_block_height;
    return this.parseNumber(latestHeight);
  }

  private parseNumber(value: unknown): number {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 0) {
      return 0;
    }

    return Math.floor(parsed);
  }

  private createRpcError(code: number, message: string): RpcError {
    const error = new Error(`RPC error ${code}: ${message}`) as RpcError;
    error.rpcCode = code;
    return error;
  }

  private async postJsonRpcWithTlsRecovery<TResponse>(
    endpoint: string,
    payload: unknown,
    timeoutMs: number,
  ): Promise<{ data: TResponse }> {
    const isOneRpcTarget = this.isOneRpcEndpoint(endpoint);
    const primaryRequestConfig = {
      timeout: timeoutMs,
      headers: { 'Content-Type': 'application/json' as const },
      ...(isOneRpcTarget ? { httpsAgent: this.hardenedHttpsAgent, proxy: false as const } : {}),
    };

    try {
      return await axios.post<TResponse>(endpoint, payload, primaryRequestConfig);
    } catch (error: unknown) {
      if (!this.shouldAttemptTlsRecovery(error)) {
        throw error;
      }

      this.logger.warn(
        `TLS recovery retry for RPC endpoint ${endpoint} (forceIpv4=${String(
          this.forceIpv4,
        )}, disableProxy=${String(this.disableProxyOnTlsRecovery)})`,
      );

      return axios.post<TResponse>(endpoint, payload, {
        timeout: timeoutMs,
        headers: { 'Content-Type': 'application/json' },
        httpsAgent: this.hardenedHttpsAgent,
        ...(this.disableProxyOnTlsRecovery ? { proxy: false as const } : {}),
      });
    }
  }

  private shouldAttemptTlsRecovery(error: unknown): boolean {
    if (!this.tlsRecoveryEnabled || !axios.isAxiosError(error)) {
      return false;
    }

    const code = (error.code || '').toUpperCase();
    if (
      code === 'EPROTO' ||
      code === 'ECONNRESET' ||
      code === 'ERR_SSL_WRONG_VERSION_NUMBER' ||
      code === 'UNABLE_TO_VERIFY_LEAF_SIGNATURE'
    ) {
      return true;
    }

    return this.isTlsHandshakeError(error);
  }

  private isRetryableError(error: unknown): boolean {
    if (this.isRateLimitError(error)) {
      return true;
    }

    if (this.isTlsHandshakeError(error)) {
      return true;
    }

    if (axios.isAxiosError(error)) {
      const status = error.response?.status;
      if (typeof status === 'number' && status >= 500) {
        return true;
      }

      if (error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT' || error.code === 'ECONNRESET') {
        return true;
      }
    }

    return false;
  }

  private isTlsHandshakeError(error: unknown): boolean {
    const message = this.getErrorMessage(error).toLowerCase();
    return (
      message.includes('before secure tls connection was established') ||
      message.includes('tls') ||
      message.includes('ssl')
    );
  }

  private isRateLimitError(error: unknown): boolean {
    if (axios.isAxiosError(error)) {
      return error.response?.status === 429;
    }

    if (this.hasRpcCode(error)) {
      const code = (error as RpcError).rpcCode;
      if (typeof code === 'number' && this.isRateLimitRpcCode(code)) {
        return true;
      }
    }

    const message = this.getErrorMessage(error).toLowerCase();
    return message.includes('rate limit') || message.includes('too many requests');
  }

  private isRateLimitRpcCode(code: number): boolean {
    return code === 429 || code === -32005;
  }

  private hasRpcCode(error: unknown): error is RpcError {
    return Boolean(error && typeof error === 'object' && 'rpcCode' in error);
  }

  private backoffDelayMs(attempt: number): number {
    const baseDelay = this.backoffBaseMs * Math.pow(2, Math.max(attempt - 1, 0));
    const jitter = Math.floor(Math.random() * this.backoffBaseMs);
    return Math.min(baseDelay + jitter, 10000);
  }

  private async sleep(ms: number): Promise<void> {
    await new Promise<void>((resolve) => setTimeout(resolve, ms));
  }

  private extractStellarError(error: unknown): unknown {
    const details =
      (error as { response?: { data?: { extras?: { result_codes?: unknown } } } })?.response?.data?.extras
        ?.result_codes ||
      (error as { response?: { data?: unknown } })?.response?.data;
    if (details) {
      return details;
    }

    return this.getErrorMessage(error);
  }

  private getErrorMessage(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }

    if (typeof error === 'string') {
      return error;
    }

    return 'unknown_error';
  }

  private recordCall(chain: string): void {
    this.metrics.totalCalls += 1;
    const chainMetric = this.getOrCreateChainMetric(chain);
    chainMetric.calls += 1;
    this.touchMetrics();
  }

  private recordSuccess(chain: string): void {
    this.metrics.successfulCalls += 1;
    const chainMetric = this.getOrCreateChainMetric(chain);
    chainMetric.successes += 1;
    this.touchMetrics();
  }

  private recordFailure(chain: string): void {
    this.metrics.failedCalls += 1;
    const chainMetric = this.getOrCreateChainMetric(chain);
    chainMetric.failures += 1;
    this.touchMetrics();
  }

  private recordRetry(chain: string): void {
    this.metrics.retryCount += 1;
    const chainMetric = this.getOrCreateChainMetric(chain);
    chainMetric.retries += 1;
    this.touchMetrics();
  }

  private recordFallbackSwitch(chain: string): void {
    this.metrics.fallbackSwitches += 1;
    const chainMetric = this.getOrCreateChainMetric(chain);
    chainMetric.fallbackSwitches += 1;
    this.touchMetrics();
  }

  private recordRateLimited(chain: string): void {
    this.metrics.rateLimitedErrors += 1;
    const chainMetric = this.getOrCreateChainMetric(chain);
    chainMetric.rateLimitedErrors += 1;
    this.touchMetrics();
  }

  private getOrCreateChainMetric(chain: string): ChainMetric {
    const existing = this.metrics.perChain[chain];
    if (existing) {
      return existing;
    }

    const created: ChainMetric = {
      calls: 0,
      successes: 0,
      failures: 0,
      retries: 0,
      fallbackSwitches: 0,
      rateLimitedErrors: 0,
    };
    this.metrics.perChain[chain] = created;
    return created;
  }

  private touchMetrics(): void {
    this.metrics.lastUpdated = new Date().toISOString();
  }

  private resolveStellarNetwork(network?: string): string {
    return (network || 'testnet').trim().toLowerCase();
  }

  private resolveRequestedStellarNetwork(network?: string): 'testnet' | 'mainnet' {
    const normalized = this.resolveStellarNetwork(network || process.env.STELLAR_NETWORK);
    return normalized === 'mainnet' || normalized === 'public' ? 'mainnet' : 'testnet';
  }

  private getStellarHorizonUrls(network?: string): string[] {
    const resolvedNetwork = this.resolveRequestedStellarNetwork(network);
    return resolvedNetwork === 'mainnet'
      ? this.horizonMainnetUrls
      : this.horizonTestnetUrls;
  }

  private getNetworkPassphrase(network?: string): string {
    const resolvedNetwork = this.resolveRequestedStellarNetwork(network);
    return resolvedNetwork === 'mainnet'
      ? StellarSdk.Networks.PUBLIC
      : StellarSdk.Networks.TESTNET;
  }
}
