import { Injectable, Logger } from '@nestjs/common';
import { MessageEvent } from '@nestjs/common/interfaces/http/message-event.interface';
import { Observable, from, interval, merge, of } from 'rxjs';
import { catchError, map, shareReplay, startWith, switchMap } from 'rxjs/operators';
import { Asset, MarketService, NetworkStats, Protocol } from '../market/market.service';
import { OraclePriceFeed, OracleService } from '../oracle/oracle.service';

type RealtimeSnapshot = {
  type: 'snapshot';
  timestamp: string;
  refreshMs: number;
  networkStats: NetworkStats;
  protocols: Protocol[];
  trendingAssets: Asset[];
  oraclePrices: OraclePriceFeed[];
};

type RealtimePriceTick = {
  type: 'price_tick';
  timestamp: string;
  tickMs: number;
  oraclePrices: OraclePriceFeed[];
};

@Injectable()
export class RealtimeService {
  private readonly logger = new Logger(RealtimeService.name);
  private readonly snapshotSymbols = ['XLM', 'BTC', 'ETH', 'USDC', 'AXL'];
  private readonly refreshMs = this.resolveIntervalMs(process.env.REALTIME_TICK_MS, 1000, 1000, 60000);
  private readonly priceTickMs = this.resolveIntervalMs(
    process.env.REALTIME_PRICE_TICK_MS,
    500,
    200,
    10000,
  );
  private readonly stream$: Observable<MessageEvent>;
  private trackedPriceSymbols = new Set<string>(this.snapshotSymbols);
  private lastSnapshot: RealtimeSnapshot = {
    type: 'snapshot',
    timestamp: new Date().toISOString(),
    refreshMs: this.refreshMs,
    networkStats: {
      volume24h: 0,
      activeContracts: 0,
      uniqueWallets: 0,
      txCount24h: 0,
      fees24h: 0,
      contractCalls24h: 0,
    },
    protocols: [],
    trendingAssets: [],
    oraclePrices: [],
  };

  constructor(
    private readonly marketService: MarketService,
    private readonly oracleService: OracleService,
  ) {
    this.stream$ = this.buildStream();
  }

  stream(): Observable<MessageEvent> {
    return this.stream$;
  }

  getMetadata() {
    return {
      status: 'ok',
      event: 'snapshot|price_tick',
      refreshMs: this.refreshMs,
      priceTickMs: this.priceTickMs,
    };
  }

  private buildStream(): Observable<MessageEvent> {
    const snapshot$ = interval(this.refreshMs).pipe(
      startWith(0),
      switchMap(() =>
        from(this.createSnapshot()).pipe(
          map((snapshot): MessageEvent => ({ data: snapshot })),
          catchError((error: unknown) => {
            this.logger.warn(`Failed to build realtime snapshot. ${this.errorMessage(error)}`);
            return of({ data: this.lastSnapshot } as MessageEvent);
          }),
        ),
      ),
    );

    const priceTick$ = interval(this.priceTickMs).pipe(
      startWith(0),
      map(() => this.createPriceTick()),
      map((tick): MessageEvent => ({ data: tick })),
      catchError((error: unknown) => {
        this.logger.warn(`Failed to build realtime price tick. ${this.errorMessage(error)}`);
        return of({ data: this.createPriceTick() } as MessageEvent);
      }),
    );

    return merge(snapshot$, priceTick$).pipe(shareReplay({ bufferSize: 1, refCount: true }));
  }

  private async createSnapshot(): Promise<RealtimeSnapshot> {
    const snapshotSymbolsQuery = this.snapshotSymbols.join(',');
    const [networkStatsResult, protocolsResult, trendingAssetsResult, oraclePricesResult] =
      await Promise.allSettled([
        this.marketService.getNetworkStats(),
        this.marketService.getSorobanProtocols(),
        this.marketService.getTrendingAssets(),
        this.oracleService.getPrices(snapshotSymbolsQuery),
      ]);

    const snapshot: RealtimeSnapshot = {
      type: 'snapshot',
      timestamp: new Date().toISOString(),
      refreshMs: this.refreshMs,
      networkStats:
        networkStatsResult.status === 'fulfilled'
          ? networkStatsResult.value
          : this.lastSnapshot.networkStats,
      protocols:
        protocolsResult.status === 'fulfilled'
          ? protocolsResult.value
          : this.lastSnapshot.protocols,
      trendingAssets:
        trendingAssetsResult.status === 'fulfilled'
          ? trendingAssetsResult.value
          : this.lastSnapshot.trendingAssets,
      oraclePrices:
        oraclePricesResult.status === 'fulfilled'
          ? oraclePricesResult.value
          : this.lastSnapshot.oraclePrices,
    };

    this.refreshTrackedPriceSymbols(snapshot.trendingAssets);
    this.lastSnapshot = snapshot;
    return snapshot;
  }

  private createPriceTick(): RealtimePriceTick {
    const symbols = Array.from(this.trackedPriceSymbols);
    const symbolsQuery = symbols.join(',');
    const oraclePrices = this.oracleService
      .getLivePrices(symbolsQuery)
      .filter((feed) => Number.isFinite(feed.price) && feed.price > 0);
    return {
      type: 'price_tick',
      timestamp: new Date().toISOString(),
      tickMs: this.priceTickMs,
      oraclePrices,
    };
  }

  private refreshTrackedPriceSymbols(trendingAssets: Asset[]): void {
    const nextSymbols = new Set<string>(this.snapshotSymbols);

    for (const asset of trendingAssets) {
      const symbol = asset.symbol.trim().toUpperCase();
      if (!symbol || symbol.length > 12 || !/^[A-Z0-9]+$/.test(symbol)) {
        continue;
      }

      nextSymbols.add(symbol);
      if (nextSymbols.size >= 80) {
        break;
      }
    }

    this.trackedPriceSymbols = nextSymbols;
  }

  private resolveIntervalMs(rawValue: string | undefined, fallback: number, min: number, max: number): number {
    const parsed = Number(rawValue);
    if (!Number.isFinite(parsed)) {
      return fallback;
    }

    return Math.min(Math.max(Math.floor(parsed), min), max);
  }

  private errorMessage(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }

    return String(error);
  }
}
