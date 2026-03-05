import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';

export interface PriceQuote {
  price: number;
  confidence: number;
  publishTime: number;
}

type CachedQuote = {
  quote: PriceQuote;
  timestamp: number;
};

type HermesParsedUpdate = {
  id?: unknown;
  price?: {
    price?: unknown;
    conf?: unknown;
    expo?: unknown;
    publish_time?: unknown;
  };
};

@Injectable()
export class PriceFeedService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PriceFeedService.name);
  private readonly cache = new Map<string, CachedQuote>();
  private readonly CACHE_TTL = 5 * 1000; // 5 seconds
  private readonly STELLAR_EXPERT_ASSET_URL = 'https://api.stellar.expert/explorer/public/asset';
  private readonly pythHermesUrl: string;
  private readonly pythTimeoutMs: number;
  private readonly pythMaxRetries: number;
  private readonly pythRetryDelayMs: number;
  private readonly pythWarningCooldownMs: number;
  private readonly inFlightBatchRequests = new Map<string, Promise<Map<string, PriceQuote>>>();
  private readonly inFlightExpertBatchRequests = new Map<string, Promise<Map<string, PriceQuote>>>();
  private readonly warningTimestamps = new Map<string, number>();
  private readonly streamEnabled: boolean;
  private readonly streamUrl: string;
  private readonly streamReconnectBaseMs: number;
  private readonly streamReconnectMaxMs: number;
  private readonly streamQuoteMaxAgeMs: number;
  private readonly streamDefaultAssets: string[];
  private readonly streamPairOverrides: Record<string, string>;
  private readonly streamPairs = new Map<string, Set<string>>();
  private streamSocket: WebSocket | null = null;
  private streamReconnectTimer: NodeJS.Timeout | null = null;
  private streamReconnectAttempts = 0;
  private streamRequestId = 1;
  private isShuttingDown = false;

  // Mapping of internal Asset Symbol -> Pyth Price Feed ID (Hex)
  private readonly ASSET_MAP: Record<string, string> = {
    'XLM': 'b7a8eba68a997cd0210c2e1e4ee811ad2d174b3611c22d9ebf16f4cb7e9ba850', // XLM/USD
    'USDC': 'eaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a', // USDC/USD
    'EURC': 'a995d00bb36a63cef7fd2c287dc105fc8f3d93779f062f09551b0af3e81ec30b', // EUR/USD (used for EURC valuation)
    'BTC': 'e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43', // BTC/USD
    'ETH': 'ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace', // ETH/USD
    'AXL': '60144b1d5c9e9851732ad1d9760e3485ef80be39b984f6bf60f82b28a2b7f126', // AXL/USD
  };
  private readonly STELLAR_EXPERT_ASSET_MAP: Record<string, string> = {
    'AQUA': 'AQUA-GBNZILSTVQZ4R7IKQDGHYGY2QXL5QOFJYQMXPKWRRM5PAV7Y4M67AQUA',
  };

  constructor(private configService: ConfigService) {
    this.pythHermesUrl = this.resolveBaseUrlConfig('PYTH_HERMES_URL', 'https://hermes.pyth.network');
    this.pythTimeoutMs = this.getPositiveIntConfig('PYTH_TIMEOUT_MS', 12000);
    this.pythMaxRetries = this.getPositiveIntConfig('PYTH_MAX_RETRIES', 2);
    this.pythRetryDelayMs = this.getPositiveIntConfig('PYTH_RETRY_DELAY_MS', 300);
    this.pythWarningCooldownMs = this.getPositiveIntConfig('PYTH_WARNING_COOLDOWN_MS', 60000);
    this.streamEnabled = this.getBooleanConfig('PRICE_STREAM_ENABLED', true);
    this.streamUrl = this.resolveStreamUrlConfig('PRICE_STREAM_URL', 'wss://stream.binance.com:9443/ws');
    this.streamReconnectBaseMs = this.getPositiveIntConfig('PRICE_STREAM_RECONNECT_BASE_MS', 1000);
    this.streamReconnectMaxMs = this.getPositiveIntConfig('PRICE_STREAM_RECONNECT_MAX_MS', 30000);
    this.streamQuoteMaxAgeMs = this.getPositiveIntConfig('PRICE_STREAM_QUOTE_MAX_AGE_MS', 8000);
    this.streamDefaultAssets = this.parseAssetListConfig(
      'PRICE_STREAM_DEFAULT_ASSETS',
      'XLM,BTC,ETH,USDC,AXL,EURC',
    );
    this.streamPairOverrides = this.parseStreamPairOverridesConfig('PRICE_STREAM_PAIR_OVERRIDES');
  }

  onModuleInit(): void {
    if (!this.streamEnabled) {
      return;
    }

    this.ensureStreamSubscriptions(this.streamDefaultAssets);
    this.connectStream();
  }

  onModuleDestroy(): void {
    this.isShuttingDown = true;
    this.clearStreamReconnectTimer();

    if (!this.streamSocket) {
      return;
    }

    try {
      this.streamSocket.close();
    } catch {
      // no-op
    } finally {
      this.streamSocket = null;
    }
  }

  async getPrice(assetId: string): Promise<number> {
    const normalizedAssetId = this.normalizeAssetId(assetId);
    if (!normalizedAssetId) {
      return 0;
    }

    const quotes = await this.getQuotes([normalizedAssetId]);
    return quotes.get(normalizedAssetId)?.price || 0;
  }

  async getPrices(assetIds: string[]): Promise<Map<string, number>> {
    const normalizedAssetIds = this.normalizeAssetIds(assetIds);
    const quoteMap = await this.getQuotes(normalizedAssetIds);
    const result = new Map<string, number>();
    for (const id of normalizedAssetIds) {
      result.set(id, quoteMap.get(id)?.price ?? 0);
    }

    return result;
  }

  async getQuotes(assetIds: string[]): Promise<Map<string, PriceQuote>> {
    const normalizedAssetIds = this.normalizeAssetIds(assetIds);
    if (normalizedAssetIds.length === 0) {
      return new Map<string, PriceQuote>();
    }

    this.ensureStreamSubscriptions(normalizedAssetIds);

    const results = new Map<string, PriceQuote>();
    const missingIds: string[] = [];

    const now = Date.now();
    for (const id of normalizedAssetIds) {
      const cached = this.cache.get(id);
      if (cached && now - cached.timestamp < this.CACHE_TTL) {
        results.set(id, cached.quote);
      } else {
        missingIds.push(id);
      }
    }

    if (missingIds.length > 0) {
      const supportedMissing = missingIds.filter((id) => this.ASSET_MAP[id]);

      if (supportedMissing.length > 0) {
        try {
          const fetchedQuotes = await this.fetchQuotesFromPythDeduped(supportedMissing);
          const fetchedAt = Date.now();
          for (const [id, quote] of fetchedQuotes) {
            this.cache.set(id, { quote, timestamp: fetchedAt });
            results.set(id, quote);
          }
        } catch (error: unknown) {
          this.logger.error(
            `Failed to fetch prices for ${supportedMissing.join(', ')}: ${this.getErrorMessage(error)}`,
          );
        }
      }

      const expertSupportedMissing = missingIds.filter(
        (id) => this.STELLAR_EXPERT_ASSET_MAP[id] && !results.has(id),
      );

      if (expertSupportedMissing.length > 0) {
        try {
          const expertQuotes = await this.fetchQuotesFromStellarExpertDeduped(expertSupportedMissing);
          const fetchedAt = Date.now();
          for (const [id, quote] of expertQuotes) {
            this.cache.set(id, { quote, timestamp: fetchedAt });
            results.set(id, quote);
          }
        } catch (error: unknown) {
          this.logger.error(
            `Failed to fetch Stellar Expert prices for ${expertSupportedMissing.join(', ')}: ${this.getErrorMessage(error)}`,
          );
        }
      }

      for (const id of missingIds) {
        if (!results.has(id)) {
          const stale = this.cache.get(id);
          if (stale) {
            results.set(id, stale.quote);
          } else {
            results.set(id, this.emptyQuote());
          }
        }
      }
    }

    return results;
  }

  getLiveQuotes(assetIds: string[], maxAgeMs = this.streamQuoteMaxAgeMs): Map<string, PriceQuote> {
    const normalizedAssetIds = this.normalizeAssetIds(assetIds);
    if (normalizedAssetIds.length === 0) {
      return new Map<string, PriceQuote>();
    }

    this.ensureStreamSubscriptions(normalizedAssetIds);

    const now = Date.now();
    const liveQuotes = new Map<string, PriceQuote>();
    for (const assetId of normalizedAssetIds) {
      const cached = this.cache.get(assetId);
      if (!cached || now - cached.timestamp > maxAgeMs) {
        continue;
      }

      if (!Number.isFinite(cached.quote.price) || cached.quote.price <= 0) {
        continue;
      }

      liveQuotes.set(assetId, cached.quote);
    }

    return liveQuotes;
  }

  private async fetchQuotesFromStellarExpertDeduped(assetIds: string[]): Promise<Map<string, PriceQuote>> {
    const requestKey = this.createBatchRequestKey(assetIds);
    const existingRequest = this.inFlightExpertBatchRequests.get(requestKey);
    if (existingRequest) {
      return existingRequest;
    }

    const request = this.fetchQuotesFromStellarExpert(assetIds).finally(() => {
      if (this.inFlightExpertBatchRequests.get(requestKey) === request) {
        this.inFlightExpertBatchRequests.delete(requestKey);
      }
    });

    this.inFlightExpertBatchRequests.set(requestKey, request);
    return request;
  }

  private async fetchQuotesFromStellarExpert(assetIds: string[]): Promise<Map<string, PriceQuote>> {
    const normalizedAssetIds = [
      ...new Set(assetIds.map((id) => id.trim().toUpperCase()).filter(Boolean)),
    ];
    if (normalizedAssetIds.length === 0) {
      return new Map<string, PriceQuote>();
    }

    const results = new Map<string, PriceQuote>();
    const fetchResults = await Promise.allSettled(
      normalizedAssetIds.map(async (assetId) => {
        const assetIdentifier = this.STELLAR_EXPERT_ASSET_MAP[assetId];
        if (!assetIdentifier) {
          return;
        }

        const payload = await this.fetchStellarExpertAssetWithRetry(assetIdentifier);
        const quote = this.resolveStellarExpertQuote(payload);
        if (quote) {
          results.set(assetId, quote);
        }
      }),
    );

    const failedCount = fetchResults.filter((result) => result.status === 'rejected').length;
    if (failedCount > 0) {
      this.warnWithCooldown(
        'stellar-expert-batch',
        `Stellar Expert fallback failed for ${failedCount}/${fetchResults.length} assets`,
      );
    }

    return results;
  }

  private async fetchStellarExpertAssetWithRetry(assetIdentifier: string): Promise<unknown> {
    const attempts = this.pythMaxRetries + 1;

    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        const response = await axios.get(
          `${this.STELLAR_EXPERT_ASSET_URL}/${encodeURIComponent(assetIdentifier)}`,
          {
            timeout: this.pythTimeoutMs,
          },
        );
        return response.data;
      } catch (error: unknown) {
        const isFinalAttempt = attempt === attempts;
        if (isFinalAttempt || !this.isRetryablePythError(error)) {
          throw error;
        }

        await this.sleep(this.pythRetryDelayMs * attempt);
      }
    }

    return null;
  }

  private resolveStellarExpertQuote(payload: unknown): PriceQuote | null {
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
      return null;
    }

    const record = payload as Record<string, unknown>;
    const price = Number(record.price);
    if (!Number.isFinite(price) || price <= 0) {
      return null;
    }

    const publishTime = this.resolveStellarExpertPublishTime(record);
    return {
      price,
      confidence: 0,
      publishTime,
    };
  }

  private resolveStellarExpertPublishTime(record: Record<string, unknown>): number {
    const explicitPublishTime = this.toUnixSeconds(record.publish_time);
    if (explicitPublishTime > 0) {
      return explicitPublishTime;
    }

    const updatedAt = this.toUnixSeconds(record.updatedAt);
    if (updatedAt > 0) {
      return updatedAt;
    }

    return Math.floor(Date.now() / 1000);
  }

  private toUnixSeconds(value: unknown): number {
    if (typeof value === 'number' && Number.isFinite(value) && value > 0) {
      return value > 10_000_000_000 ? Math.floor(value / 1000) : Math.floor(value);
    }

    if (typeof value === 'string') {
      const numericValue = Number(value);
      if (Number.isFinite(numericValue) && numericValue > 0) {
        return numericValue > 10_000_000_000
          ? Math.floor(numericValue / 1000)
          : Math.floor(numericValue);
      }

      const parsedDateMs = Date.parse(value);
      if (Number.isFinite(parsedDateMs) && parsedDateMs > 0) {
        return Math.floor(parsedDateMs / 1000);
      }
    }

    return 0;
  }

  private async fetchQuotesFromPyth(assetIds: string[]): Promise<Map<string, PriceQuote>> {
    const uniquePythIds = [
      ...new Set(
        assetIds
          .map((id) => this.ASSET_MAP[id])
          .filter((id): id is string => Boolean(id))
          .map((id) => this.normalizePythId(id))
          .filter((id) => this.isValidPythId(id)),
      ),
    ];

    if (uniquePythIds.length === 0) return new Map<string, PriceQuote>();

    const results = new Map<string, PriceQuote>();
    const pythQuoteMap = new Map<string, PriceQuote>();
    let batchErrorMessage = '';
    let failedSingleRequests = 0;
    let lastSingleError = '';
    let attemptedSingleFallback = false;

    try {
      const updates = await this.fetchLatestUpdatesWithRetry(uniquePythIds);
      this.populatePythQuoteMap(pythQuoteMap, updates);
    } catch (error: unknown) {
      batchErrorMessage = this.getErrorMessage(error);

      // If one ID is stale, Hermes can fail the entire batch.
      // Retry per-id to salvage prices for the valid feeds.
      if (this.shouldFallbackToSingleFeed(error)) {
        attemptedSingleFallback = true;
        for (const pythId of uniquePythIds) {
          try {
            const updates = await this.fetchLatestUpdatesWithRetry([pythId]);
            this.populatePythQuoteMap(pythQuoteMap, updates);
          } catch (singleError: unknown) {
            failedSingleRequests += 1;
            lastSingleError = this.getErrorMessage(singleError);
          }
        }

        if (failedSingleRequests > 0 && pythQuoteMap.size === 0) {
          this.warnWithCooldown(
            'pyth-single-fallback',
            `Pyth single-feed fallback failed for ${failedSingleRequests}/${uniquePythIds.length} feeds. Last error: ${lastSingleError}`,
          );
        }
      }
    }

    if (batchErrorMessage) {
      if (pythQuoteMap.size === 0) {
        this.warnWithCooldown('pyth-batch', `Pyth batch API error after retries: ${batchErrorMessage}`);
      } else {
        const recoveredCount = pythQuoteMap.size;
        const unresolvedCount = Math.max(0, uniquePythIds.length - recoveredCount);
        this.logger.debug(
          `Pyth batch request degraded but recovered ${recoveredCount}/${uniquePythIds.length} feeds via ${attemptedSingleFallback ? 'single-feed fallback' : 'partial batch response'} (${unresolvedCount} unresolved). Initial error: ${batchErrorMessage}`,
        );
      }
    }

    for (const asset of assetIds) {
      const pythId = this.ASSET_MAP[asset];
      const normalizedId = pythId ? this.normalizePythId(pythId) : undefined;
      if (normalizedId && pythQuoteMap.has(normalizedId)) {
        results.set(asset, pythQuoteMap.get(normalizedId)!);
      }
    }

    return results;
  }

  private async fetchQuotesFromPythDeduped(assetIds: string[]): Promise<Map<string, PriceQuote>> {
    const requestKey = this.createBatchRequestKey(assetIds);
    const existingRequest = this.inFlightBatchRequests.get(requestKey);
    if (existingRequest) {
      return existingRequest;
    }

    const request = this.fetchQuotesFromPyth(assetIds).finally(() => {
      if (this.inFlightBatchRequests.get(requestKey) === request) {
        this.inFlightBatchRequests.delete(requestKey);
      }
    });

    this.inFlightBatchRequests.set(requestKey, request);
    return request;
  }

  private async fetchLatestUpdates(pythIds: string[]): Promise<HermesParsedUpdate[]> {
    const params = new URLSearchParams();
    pythIds.forEach((id) => params.append('ids[]', id));

    const response = await axios.get(`${this.pythHermesUrl}/v2/updates/price/latest`, {
      params,
      timeout: this.pythTimeoutMs,
    });

    const updates = response.data?.parsed;
    return Array.isArray(updates) ? (updates as HermesParsedUpdate[]) : [];
  }

  private async fetchLatestUpdatesWithRetry(pythIds: string[]): Promise<HermesParsedUpdate[]> {
    const attempts = this.pythMaxRetries + 1;

    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        return await this.fetchLatestUpdates(pythIds);
      } catch (error: unknown) {
        const isFinalAttempt = attempt === attempts;
        if (isFinalAttempt || !this.isRetryablePythError(error)) {
          throw error;
        }

        await this.sleep(this.pythRetryDelayMs * attempt);
      }
    }

    return [];
  }

  private populatePythQuoteMap(target: Map<string, PriceQuote>, updates: HermesParsedUpdate[]) {
    for (const update of updates) {
      const id = this.normalizePythId(String(update?.id || ''));
      if (!this.isValidPythId(id)) {
        continue;
      }

      const quote = this.resolveQuote(update);
      if (quote) {
        target.set(id, quote);
      }
    }
  }

  private resolveQuote(update: HermesParsedUpdate): PriceQuote | null {
    const rawPrice = Number(update?.price?.price);
    const rawConf = Number(update?.price?.conf);
    const expo = Number(update?.price?.expo);
    const publishTime = Number(update?.price?.publish_time);

    if (!Number.isFinite(rawPrice) || !Number.isFinite(expo)) {
      return null;
    }

    const multiplier = Math.pow(10, expo);
    const price = rawPrice * multiplier;
    if (!Number.isFinite(price)) {
      return null;
    }

    const confidence = Number.isFinite(rawConf) ? rawConf * multiplier : 0;
    return {
      price,
      confidence: Number.isFinite(confidence) && confidence > 0 ? confidence : 0,
      publishTime: Number.isFinite(publishTime) && publishTime > 0 ? Math.floor(publishTime) : 0,
    };
  }

  private emptyQuote(): PriceQuote {
    return {
      price: 0,
      confidence: 0,
      publishTime: 0,
    };
  }

  private normalizePythId(id: string): string {
    return id.startsWith('0x') ? id.slice(2) : id;
  }

  private isValidPythId(id: string): boolean {
    return /^[a-fA-F0-9]{64}$/.test(id);
  }

  private isRetryablePythError(error: unknown): boolean {
    if (!axios.isAxiosError(error)) {
      return false;
    }

    if (error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT' || error.code === 'ECONNRESET') {
      return true;
    }

    const status = error.response?.status;
    return status === 429 || (typeof status === 'number' && status >= 500);
  }

  private shouldFallbackToSingleFeed(error: unknown): boolean {
    if (!axios.isAxiosError(error)) {
      return true;
    }

    if (error.response) {
      return true;
    }

    return this.isRetryablePythError(error);
  }

  private normalizeAssetId(assetId: string): string {
    return assetId.trim().toUpperCase();
  }

  private normalizeAssetIds(assetIds: string[]): string[] {
    return [...new Set(assetIds.map((assetId) => this.normalizeAssetId(assetId)).filter(Boolean))];
  }

  private ensureStreamSubscriptions(assetIds: string[]): void {
    if (!this.streamEnabled || assetIds.length === 0) {
      return;
    }

    const newPairs: string[] = [];
    for (const assetId of assetIds) {
      const pair = this.resolveStreamPair(assetId);
      if (!pair) {
        continue;
      }

      const existingAssets = this.streamPairs.get(pair);
      if (!existingAssets) {
        this.streamPairs.set(pair, new Set([assetId]));
        newPairs.push(pair);
        continue;
      }

      existingAssets.add(assetId);
    }

    if (newPairs.length === 0) {
      return;
    }

    if (this.streamSocket && this.streamSocket.readyState === WebSocket.OPEN) {
      this.sendStreamSubscribe(newPairs);
      return;
    }

    this.connectStream();
  }

  private connectStream(): void {
    if (!this.streamEnabled || this.isShuttingDown || this.streamSocket) {
      return;
    }

    try {
      const socket = new WebSocket(this.streamUrl);
      this.streamSocket = socket;

      socket.onopen = () => {
        this.handleStreamOpen(socket);
      };

      socket.onmessage = (event: { data: unknown }) => {
        this.handleStreamMessage(event.data);
      };

      socket.onerror = () => {
        this.warnWithCooldown(
          'price-stream-error',
          `Live price stream error for ${this.streamUrl}. Waiting for reconnect.`,
        );
      };

      socket.onclose = () => {
        this.handleStreamClose(socket);
      };
    } catch (error: unknown) {
      this.warnWithCooldown(
        'price-stream-connect-failed',
        `Failed to initialize live price stream connection. ${this.getErrorMessage(error)}`,
      );
      this.scheduleStreamReconnect();
    }
  }

  private handleStreamOpen(socket: WebSocket): void {
    if (this.streamSocket !== socket) {
      return;
    }

    this.streamReconnectAttempts = 0;
    this.clearStreamReconnectTimer();
    this.sendStreamSubscribe(Array.from(this.streamPairs.keys()));
  }

  private handleStreamClose(socket: WebSocket): void {
    if (this.streamSocket !== socket) {
      return;
    }

    this.streamSocket = null;
    if (this.isShuttingDown || !this.streamEnabled) {
      return;
    }

    this.scheduleStreamReconnect();
  }

  private handleStreamMessage(data: unknown): void {
    const payloadText = this.decodeStreamMessageData(data);
    if (!payloadText) {
      return;
    }

    let parsedPayload: unknown;
    try {
      parsedPayload = JSON.parse(payloadText);
    } catch {
      return;
    }

    if (!parsedPayload || typeof parsedPayload !== 'object' || Array.isArray(parsedPayload)) {
      return;
    }

    const envelope = parsedPayload as Record<string, unknown>;
    if ('result' in envelope && !('data' in envelope)) {
      return;
    }

    const eventPayload =
      envelope.data && typeof envelope.data === 'object' && !Array.isArray(envelope.data)
        ? (envelope.data as Record<string, unknown>)
        : envelope;

    const pairSymbolCandidate =
      (typeof eventPayload.s === 'string' ? eventPayload.s : undefined) ||
      (typeof eventPayload.symbol === 'string' ? eventPayload.symbol : undefined);
    if (!pairSymbolCandidate) {
      return;
    }

    const pair = pairSymbolCandidate.trim().toLowerCase();
    if (!this.isValidStreamPair(pair)) {
      return;
    }

    const rawPriceCandidate = eventPayload.p ?? eventPayload.c ?? eventPayload.price;
    const price = Number(rawPriceCandidate);
    if (!Number.isFinite(price) || price <= 0) {
      return;
    }

    const publishTime = this.toUnixSeconds(eventPayload.E ?? eventPayload.T ?? Date.now());
    const quote: PriceQuote = {
      price,
      confidence: 0,
      publishTime: publishTime > 0 ? publishTime : Math.floor(Date.now() / 1000),
    };

    const cachedQuote: CachedQuote = {
      quote,
      timestamp: Date.now(),
    };

    const linkedAssets = this.streamPairs.get(pair);
    if (!linkedAssets || linkedAssets.size === 0) {
      return;
    }

    for (const assetId of linkedAssets) {
      this.cache.set(assetId, cachedQuote);
    }
  }

  private decodeStreamMessageData(data: unknown): string | null {
    if (typeof data === 'string') {
      return data;
    }

    if (data instanceof ArrayBuffer) {
      return Buffer.from(data).toString('utf8');
    }

    if (ArrayBuffer.isView(data)) {
      return Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString('utf8');
    }

    return null;
  }

  private sendStreamSubscribe(pairs: string[]): void {
    if (!this.streamSocket || this.streamSocket.readyState !== WebSocket.OPEN) {
      return;
    }

    const normalizedPairs = [...new Set(pairs.map((pair) => pair.trim().toLowerCase()).filter(Boolean))]
      .filter((pair) => this.isValidStreamPair(pair));
    if (normalizedPairs.length === 0) {
      return;
    }

    const payload = {
      method: 'SUBSCRIBE',
      params: normalizedPairs.map((pair) => `${pair}@trade`),
      id: this.streamRequestId++,
    };

    try {
      this.streamSocket.send(JSON.stringify(payload));
    } catch (error: unknown) {
      this.warnWithCooldown(
        'price-stream-subscribe',
        `Failed to subscribe to live price channels (${normalizedPairs.join(', ')}). ${this.getErrorMessage(error)}`,
      );
    }
  }

  private scheduleStreamReconnect(): void {
    if (this.isShuttingDown || this.streamReconnectTimer) {
      return;
    }

    const attempt = this.streamReconnectAttempts++;
    const delayMs = Math.min(this.streamReconnectBaseMs * Math.pow(2, attempt), this.streamReconnectMaxMs);
    this.streamReconnectTimer = setTimeout(() => {
      this.streamReconnectTimer = null;
      this.connectStream();
    }, delayMs);
  }

  private clearStreamReconnectTimer(): void {
    if (!this.streamReconnectTimer) {
      return;
    }

    clearTimeout(this.streamReconnectTimer);
    this.streamReconnectTimer = null;
  }

  private resolveStreamPair(assetId: string): string | null {
    const normalizedAsset = this.normalizeAssetId(assetId);
    if (!normalizedAsset) {
      return null;
    }

    const overridePair = this.streamPairOverrides[normalizedAsset];
    const rawPair = overridePair && overridePair.trim() ? overridePair : `${normalizedAsset}USDT`;
    const pair = rawPair.trim().toLowerCase();
    return this.isValidStreamPair(pair) ? pair : null;
  }

  private isValidStreamPair(pair: string): boolean {
    return /^[a-z0-9]{5,24}$/.test(pair);
  }

  private getErrorMessage(error: unknown): string {
    if (axios.isAxiosError(error)) {
      const method = typeof error.config?.method === 'string' && error.config.method.trim()
        ? error.config.method.toUpperCase()
        : 'GET';
      const url = typeof error.config?.url === 'string' && error.config.url.trim()
        ? error.config.url
        : `${this.pythHermesUrl}/v2/updates/price/latest`;
      const status = error.response?.status;
      const code = typeof error.code === 'string' && error.code.trim() ? error.code : '';
      const message = typeof error.message === 'string' && error.message.trim() ? error.message : 'request failed';

      const context = [`${method} ${url}`];
      if (typeof status === 'number') {
        context.push(`status=${status}`);
      }
      if (code) {
        context.push(`code=${code}`);
      }

      return `${message} (${context.join(', ')})`;
    }

    if (error instanceof Error && error.message) {
      return error.message;
    }

    if (typeof error === 'string' && error.trim()) {
      return error;
    }

    return 'unknown error';
  }

  private getPositiveIntConfig(key: string, fallback: number): number {
    const value = this.configService.get<string | number | undefined>(key);
    const parsed = Number(value);

    if (!Number.isFinite(parsed) || parsed <= 0) {
      return fallback;
    }

    return Math.floor(parsed);
  }

  private getBooleanConfig(key: string, fallback: boolean): boolean {
    const value = this.configService.get<string | number | boolean | undefined>(key);
    if (typeof value === 'boolean') {
      return value;
    }

    if (typeof value === 'number') {
      return value !== 0;
    }

    if (typeof value !== 'string') {
      return fallback;
    }

    const normalized = value.trim().toLowerCase();
    if (normalized === 'true' || normalized === '1' || normalized === 'yes' || normalized === 'on') {
      return true;
    }

    if (normalized === 'false' || normalized === '0' || normalized === 'no' || normalized === 'off') {
      return false;
    }

    return fallback;
  }

  private parseAssetListConfig(key: string, fallbackCsv: string): string[] {
    const rawValue = this.configService.get<string | undefined>(key);
    const source = typeof rawValue === 'string' && rawValue.trim() ? rawValue : fallbackCsv;
    return this.normalizeAssetIds(source.split(','));
  }

  private parseStreamPairOverridesConfig(key: string): Record<string, string> {
    const overrides: Record<string, string> = {
      EURC: 'eurusdt',
    };

    const rawValue = this.configService.get<string | undefined>(key);
    if (typeof rawValue !== 'string' || !rawValue.trim()) {
      return overrides;
    }

    const entries = rawValue.split(',');
    for (const entry of entries) {
      const trimmed = entry.trim();
      if (!trimmed) {
        continue;
      }

      const splitAt = trimmed.includes('=') ? trimmed.indexOf('=') : trimmed.indexOf(':');
      if (splitAt <= 0 || splitAt >= trimmed.length - 1) {
        continue;
      }

      const asset = this.normalizeAssetId(trimmed.slice(0, splitAt));
      const pair = trimmed.slice(splitAt + 1).trim().toLowerCase();
      if (!asset || !this.isValidStreamPair(pair)) {
        continue;
      }

      overrides[asset] = pair;
    }

    return overrides;
  }

  private resolveStreamUrlConfig(key: string, fallback: string): string {
    const value = this.configService.get<string | undefined>(key);
    if (typeof value !== 'string' || !value.trim()) {
      return fallback;
    }

    return value.trim();
  }

  private resolveBaseUrlConfig(key: string, fallback: string): string {
    const value = this.configService.get<string | undefined>(key);
    if (typeof value !== 'string' || !value.trim()) {
      return fallback;
    }

    return value.trim().replace(/\/+$/, '');
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private createBatchRequestKey(assetIds: string[]): string {
    return [...new Set(assetIds.map((assetId) => assetId.trim().toUpperCase()).filter(Boolean))]
      .sort()
      .join(',');
  }

  private warnWithCooldown(key: string, message: string): void {
    const now = Date.now();
    const lastWarningAt = this.warningTimestamps.get(key) ?? 0;
    if (now - lastWarningAt < this.pythWarningCooldownMs) {
      return;
    }

    this.warningTimestamps.set(key, now);
    this.logger.warn(message);
  }
}
