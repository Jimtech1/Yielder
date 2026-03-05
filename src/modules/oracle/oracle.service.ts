import { Injectable } from '@nestjs/common';
import { PriceFeedService, PriceQuote } from '../portfolio/price-feed.service';

export interface OraclePriceFeed {
  symbol: string;
  price: number;
  change24h: number;
  confidence: number;
  lastUpdate: number;
}

type OracleHistoryPoint = {
  timestamp: number;
  price: number;
};

@Injectable()
export class OracleService {
  private readonly defaultSymbols = ['XLM', 'BTC', 'ETH', 'USDC', 'AXL'];
  private readonly historyBySymbol = new Map<string, OracleHistoryPoint[]>();
  private readonly lastQuoteBySymbol = new Map<string, PriceQuote>();
  private readonly historyWindowMs = 24 * 60 * 60 * 1000;
  private readonly maxHistoryPointsPerSymbol = 30000;

  constructor(private readonly priceFeedService: PriceFeedService) {}

  async getPrices(symbolsQuery?: string): Promise<OraclePriceFeed[]> {
    const symbols = this.parseSymbols(symbolsQuery);
    const quoteMap = await this.priceFeedService.getQuotes(symbols);
    return this.buildPriceFeeds(symbols, quoteMap);
  }

  getLivePrices(symbolsQuery?: string): OraclePriceFeed[] {
    const symbols = this.parseSymbols(symbolsQuery);
    const liveQuoteMap = this.priceFeedService.getLiveQuotes(symbols);
    return this.buildPriceFeeds(symbols, liveQuoteMap);
  }

  private buildPriceFeeds(symbols: string[], quoteMap: Map<string, PriceQuote>): OraclePriceFeed[] {
    const now = Date.now();
    return symbols.map((symbol) => {
      const quote = this.resolveQuote(symbol, quoteMap.get(symbol));
      const eventTime = quote.publishTime > 0 ? quote.publishTime * 1000 : now;

      return {
        symbol: `${symbol}/USD`,
        price: quote.price,
        change24h: this.computeRollingChange(symbol, quote.price, eventTime),
        confidence: quote.confidence,
        lastUpdate: eventTime,
      };
    });
  }

  private resolveQuote(symbol: string, incomingQuote: PriceQuote | undefined): PriceQuote {
    if (incomingQuote && Number.isFinite(incomingQuote.price) && incomingQuote.price > 0) {
      this.lastQuoteBySymbol.set(symbol, incomingQuote);
      return incomingQuote;
    }

    const previousQuote = this.lastQuoteBySymbol.get(symbol);
    if (previousQuote) {
      return previousQuote;
    }

    return this.emptyQuote();
  }

  private parseSymbols(symbolsQuery?: string): string[] {
    const source = symbolsQuery || this.defaultSymbols.join(',');
    const parsed = source
      .split(',')
      .map((symbol) => symbol.trim().toUpperCase())
      .filter((symbol) => symbol.length > 0);

    const unique = [...new Set(parsed)];
    return unique.length > 0 ? unique.slice(0, 20) : this.defaultSymbols;
  }

  private computeRollingChange(symbol: string, latestPrice: number, timestamp: number): number {
    if (!Number.isFinite(latestPrice) || latestPrice <= 0) {
      return 0;
    }

    const pointTimestamp = Number.isFinite(timestamp) && timestamp > 0 ? timestamp : Date.now();
    const history = this.historyBySymbol.get(symbol) ?? [];
    const lastPoint = history[history.length - 1];

    if (!lastPoint || pointTimestamp > lastPoint.timestamp) {
      history.push({ timestamp: pointTimestamp, price: latestPrice });
    } else if (pointTimestamp === lastPoint.timestamp) {
      lastPoint.price = latestPrice;
    }

    const cutoff = pointTimestamp - this.historyWindowMs;
    while (history.length > 0 && history[0].timestamp < cutoff) {
      history.shift();
    }

    if (history.length > this.maxHistoryPointsPerSymbol) {
      history.splice(0, history.length - this.maxHistoryPointsPerSymbol);
    }

    this.historyBySymbol.set(symbol, history);

    const baseline = history[0];
    if (!baseline || baseline.price <= 0) {
      return 0;
    }

    const change = ((latestPrice - baseline.price) / baseline.price) * 100;
    return Number.isFinite(change) ? Number(change.toFixed(4)) : 0;
  }

  private emptyQuote(): PriceQuote {
    return {
      price: 0,
      confidence: 0,
      publishTime: 0,
    };
  }
}
