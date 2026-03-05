import { BadRequestException, ForbiddenException, Injectable } from '@nestjs/common';
import { ethers } from 'ethers';
import * as StellarSdk from 'stellar-sdk';
import axios from 'axios';

const STELLAR_BASE_UNITS = 10_000_000n;

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

type CircleCctpV2CustomToken = {
  address: string;
  symbol: string;
  decimals: number;
};

type CircleCctpV2CustomBuildContext = {
  srcChainKey: string;
  dstChainKey: string;
  srcToken: CircleCctpV2CustomToken;
  dstToken: CircleCctpV2CustomToken;
};

type CircleCctpV2CustomTxBuilderMode =
  | 'disabled'
  | 'simulate-auto'
  | 'simulate-stellar-xdr'
  | 'simulate-evm-tx'
  | 'custom-template';

type AllbridgeChainDetails = {
  chainSymbol?: string;
  tokens?: AllbridgeTokenDetails[];
};

type AllbridgeTokenDetails = {
  chainSymbol?: string;
  tokenAddress?: string;
  symbol?: string;
  decimals?: number;
  name?: string;
};

@Injectable()
export class CircleCctpV2CustomTxBuilderService {
  private readonly enabled = this.resolveBooleanEnv(
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_BUILDER_ENABLED,
  );
  private readonly mode = this.resolveMode(
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_BUILDER_MODE,
  );
  private readonly apiKey = process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_API_KEY?.trim() || '';
  private readonly simulatedStellarDestination =
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_SIM_STELLAR_DESTINATION?.trim() || '';
  private readonly simulatedStellarMemo =
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_SIM_STELLAR_MEMO?.trim() || 'Yielder simulated CCTP tx';
  private readonly simulatedStellarNetwork =
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_SIM_STELLAR_NETWORK?.trim() || '';
  private readonly simulatedEvmBridgeTarget =
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_SIM_EVM_BRIDGE_TARGET?.trim() || '';
  private readonly allbridgeApiBaseUrl =
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_API_URL?.trim() ||
    'https://core.api.allbridgecoreapi.net';
  private readonly allbridgeApiTimeoutMs = this.resolvePositiveInt(
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_REQUEST_TIMEOUT_MS,
    20000,
    3000,
  );
  private readonly allbridgeMessenger =
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_MESSENGER?.trim().toUpperCase() ||
    'ALLBRIDGE';
  private readonly allbridgeGasFeePaymentMethod =
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_GAS_FEE_PAYMENT_METHOD
      ?.trim()
      .toUpperCase() || '';
  private readonly allbridgeHeaders = this.resolveJsonRecord(
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_HEADERS,
  );
  private readonly allbridgeApiKey =
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_API_KEY?.trim() || '';
  private readonly allbridgeTokenSymbol =
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_TOKEN_SYMBOL?.trim().toUpperCase() ||
    'USDC';
  private readonly allbridgePreferredStellarChainSymbol =
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_STELLAR_CHAIN_SYMBOL?.trim().toUpperCase() ||
    'SRB';
  private readonly allbridgeChainSymbolOverrides = this.resolveChainSymbolOverrides(
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_CHAIN_SYMBOL_OVERRIDES,
  );
  private allbridgeChainsCache: { timestamp: number; data: Record<string, AllbridgeChainDetails> } | null =
    null;
  private readonly allbridgeChainsCacheTtlMs = this.resolvePositiveInt(
    process.env.STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_CHAINS_CACHE_TTL_MS,
    120000,
    1000,
  );
  private readonly testnetHorizonUrl =
    process.env.STELLAR_HORIZON_URL_TESTNET ||
    process.env.STELLAR_HORIZON_URL ||
    'https://horizon-testnet.stellar.org';
  private readonly mainnetHorizonUrl =
    process.env.STELLAR_HORIZON_URL_MAINNET ||
    process.env.STELLAR_HORIZON_URL ||
    'https://horizon.stellar.org';

  assertApiKey(apiKey?: string): void {
    if (!this.apiKey) {
      return;
    }
    if ((apiKey || '').trim() !== this.apiKey) {
      throw new ForbiddenException('Invalid x-api-key for custom Circle CCTP v2 tx-builder endpoint');
    }
  }

  isEnabled(): boolean {
    return this.enabled && this.mode !== 'disabled';
  }

  supportsCustomTemplateMode(): boolean {
    return this.enabled && this.mode === 'custom-template';
  }

  supportsCustomTemplateStellarNetwork(): boolean {
    return this.resolveStellarNetwork() === 'mainnet';
  }

  supportsChainKey(chainKey: string): boolean {
    try {
      this.resolveAllbridgeChainSymbol(chainKey);
      return true;
    } catch {
      return false;
    }
  }

  async buildTransactionPayload(params: {
    request: CircleCctpV2TxBuilderRequest;
    context: CircleCctpV2CustomBuildContext;
  }): Promise<Record<string, unknown> | null> {
    if (!this.isEnabled()) {
      return null;
    }

    const sourceIsStellar = params.context.srcChainKey.trim().toLowerCase() === 'stellar';

    switch (this.mode) {
      case 'simulate-auto':
        return sourceIsStellar
          ? this.buildSimulatedStellarPayload(params.request, params.context)
          : this.buildSimulatedEvmPayload(params.request);
      case 'simulate-stellar-xdr':
        return this.buildSimulatedStellarPayload(params.request, params.context);
      case 'simulate-evm-tx':
        return this.buildSimulatedEvmPayload(params.request);
      case 'custom-template':
        return this.buildCustomTemplatePayload(params.request, params.context);
      case 'disabled':
      default:
        return null;
    }
  }

  private async buildSimulatedStellarPayload(
    request: CircleCctpV2TxBuilderRequest,
    context: CircleCctpV2CustomBuildContext,
  ): Promise<Record<string, unknown>> {
    if (context.srcChainKey.trim().toLowerCase() !== 'stellar') {
      throw new BadRequestException(
        'simulate-stellar-xdr mode requires srcChainKey=stellar so the source wallet can sign the XDR',
      );
    }
    if (!this.looksLikeStellarAddress(request.srcAddress)) {
      throw new BadRequestException('simulate-stellar-xdr mode requires a valid Stellar srcAddress');
    }
    if (context.srcToken.symbol.trim().toUpperCase() !== 'USDC') {
      throw new BadRequestException('simulate-stellar-xdr mode supports USDC source token only');
    }

    const network = this.resolveStellarNetwork();
    const horizon = new StellarSdk.Horizon.Server(
      network === 'mainnet' ? this.mainnetHorizonUrl : this.testnetHorizonUrl,
    );
    const amount = this.normalizeStellarAmount(request.srcAmount);
    const source = request.srcAddress.trim();
    const account = await horizon.loadAccount(source);
    const fee = await this.resolveBaseFee(horizon);

    const builder = new StellarSdk.TransactionBuilder(account, {
      fee,
      networkPassphrase: this.networkPassphrase(network),
    });
    builder.addOperation(
      StellarSdk.Operation.manageData({
        name: 'yielder_cctp_sim',
        // Use a stable value so the op succeeds whether the key exists or not.
        value: '1',
        source,
      }),
    );
    if (this.simulatedStellarMemo) {
      builder.addMemo(StellarSdk.Memo.text(this.simulatedStellarMemo.slice(0, 28)));
    }
    builder.setTimeout(180);

    const tx = builder.build();
    return {
      bridgeStellarTransaction: {
        xdr: tx.toXDR(),
        network,
      },
      metadata: {
        simulated: true,
        mode: 'simulate-stellar-xdr',
        requestedSrcAmount: amount,
        note: 'Simulation only: this payload uses a no-op Stellar manageData operation for in-app signing flow validation and does not perform real cross-chain bridging.',
      },
    };
  }

  private buildSimulatedEvmPayload(
    request: CircleCctpV2TxBuilderRequest,
  ): Record<string, unknown> {
    const sourceAddress = this.normalizeEvmAddress(request.srcAddress);
    if (!sourceAddress) {
      throw new BadRequestException(
        'simulate-evm-tx mode requires an EVM srcAddress (source chain must be EVM)',
      );
    }

    const destination =
      this.normalizeEvmAddress(this.simulatedEvmBridgeTarget) || sourceAddress;

    return {
      approvalTransaction: null,
      bridgeTransaction: {
        to: destination,
        data: '0x',
        value: '0',
        from: sourceAddress,
      },
      metadata: {
        simulated: true,
        mode: 'simulate-evm-tx',
        note: 'Simulation only: this sends a no-op EVM transaction for end-to-end UI testing and does not bridge assets.',
      },
    };
  }

  private async buildCustomTemplatePayload(
    request: CircleCctpV2TxBuilderRequest,
    context: CircleCctpV2CustomBuildContext,
  ): Promise<Record<string, unknown>> {
    this.assertCustomTemplateStellarNetworkSupported();

    const sourceChainSymbol = this.resolveAllbridgeChainSymbol(context.srcChainKey);
    const destinationChainSymbol = this.resolveAllbridgeChainSymbol(context.dstChainKey);
    const chainsMap = await this.fetchAllbridgeChains();
    const sourceToken = this.resolveAllbridgeToken({
      chainsMap,
      chainSymbol: sourceChainSymbol,
      requestedToken: request.srcToken,
      expectedSymbol: context.srcToken.symbol,
    });
    const destinationToken = this.resolveAllbridgeToken({
      chainsMap,
      chainSymbol: destinationChainSymbol,
      requestedToken: request.dstToken,
      expectedSymbol: context.dstToken.symbol,
    });
    const sourceTokenAddress = this.requireAllbridgeTokenAddress(sourceToken, sourceChainSymbol);
    const destinationTokenAddress = this.requireAllbridgeTokenAddress(
      destinationToken,
      destinationChainSymbol,
    );

    const rawBridgeTransaction = await this.requestAllbridgeEndpoint('/raw/bridge', {
      amount: request.srcAmountBaseUnits,
      sender: request.srcAddress,
      recipient: request.dstAddress,
      sourceToken: sourceTokenAddress,
      destinationToken: destinationTokenAddress,
      messenger: this.allbridgeMessenger,
      feePaymentMethod: this.allbridgeGasFeePaymentMethod || 'WITH_NATIVE_CURRENCY',
    });

    const xdr = this.extractStellarTransactionXdr(rawBridgeTransaction);
    if (xdr) {
      return {
        bridgeStellarTransaction: {
          xdr,
          network: this.resolveStellarNetwork(),
        },
        metadata: {
          provider: 'allbridge-core',
          messenger: this.allbridgeMessenger,
          sourceChainSymbol,
          destinationChainSymbol,
          sourceToken: sourceToken.symbol,
          destinationToken: destinationToken.symbol,
        },
      };
    }

    const bridgeTransaction = this.extractEvmTransaction(rawBridgeTransaction);
    if (!bridgeTransaction) {
      throw new BadRequestException(
        `Allbridge /raw/bridge response did not contain an executable transaction payload: ${this.stringifyCompact(
          rawBridgeTransaction,
        )}`,
      );
    }

    const approvalTransaction = await this.buildAllbridgeApprovalTransaction({
      sourceChainSymbol,
      sourceToken,
      sourceAddress: request.srcAddress,
      sourceAmount: request.srcAmount,
      sourceAmountBaseUnits: request.srcAmountBaseUnits,
    });

    return {
      approvalTransaction,
      bridgeTransaction,
      metadata: {
        provider: 'allbridge-core',
        messenger: this.allbridgeMessenger,
        sourceChainSymbol,
        destinationChainSymbol,
        sourceToken: sourceToken.symbol,
        destinationToken: destinationToken.symbol,
      },
    };
  }

  private assertCustomTemplateStellarNetworkSupported(): void {
    if (!this.supportsCustomTemplateMode()) {
      return;
    }
    if (this.supportsCustomTemplateStellarNetwork()) {
      return;
    }

    throw new BadRequestException(
      'Allbridge custom-template mode supports Stellar mainnet only. Set STELLAR_NETWORK=mainnet and use a funded mainnet Stellar account, or switch STELLAR_CIRCLE_CCTP_V2_CUSTOM_TX_BUILDER_MODE away from custom-template for testnet.',
    );
  }

  private resolveMode(input?: string): CircleCctpV2CustomTxBuilderMode {
    const value = (input || 'disabled').trim().toLowerCase();
    switch (value) {
      case 'disabled':
      case 'simulate-auto':
      case 'simulate-stellar-xdr':
      case 'simulate-evm-tx':
      case 'custom-template':
        return value;
      default:
        return 'disabled';
    }
  }

  private resolveBooleanEnv(raw?: string): boolean {
    if (!raw) {
      return false;
    }
    return ['1', 'true', 'yes', 'on'].includes(raw.trim().toLowerCase());
  }

  private resolvePositiveInt(raw: string | undefined, fallback: number, min: number): number {
    const parsed = Number.parseInt((raw || '').trim(), 10);
    if (!Number.isFinite(parsed) || parsed < min) {
      return fallback;
    }
    return Math.floor(parsed);
  }

  private resolveJsonRecord(raw: string | undefined): Record<string, string> {
    if (!raw || !raw.trim()) {
      return {};
    }
    try {
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object') {
        return {};
      }
      const out: Record<string, string> = {};
      for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
        if (typeof value === 'string' && value.trim()) {
          out[key] = value.trim();
        }
      }
      return out;
    } catch {
      return {};
    }
  }

  private resolveAllbridgeChainSymbol(chainKey: string): string {
    const normalized = chainKey.trim().toLowerCase();
    const override = this.allbridgeChainSymbolOverrides[normalized];
    if (override) {
      return override;
    }
    if (normalized === 'stellar') {
      return this.allbridgePreferredStellarChainSymbol;
    }

    const mapping: Record<string, string> = {
      ethereum: 'ETH',
      eth: 'ETH',
      arbitrum: 'ARB',
      arb: 'ARB',
      polygon: 'POL',
      matic: 'POL',
      avalanche: 'AVA',
      avax: 'AVA',
      optimism: 'OPT',
      op: 'OPT',
      base: 'BAS',
      bsc: 'BSC',
      'bnb-chain': 'BSC',
      bnb: 'BSC',
      celo: 'CEL',
      sonic: 'SNC',
      unichain: 'UNI',
      linea: 'LIN',
    };
    const resolved = mapping[normalized];
    if (!resolved) {
      throw new BadRequestException(
        `Allbridge provider does not support chainKey "${chainKey}" in this integration.`,
      );
    }
    return resolved;
  }

  private resolveChainSymbolOverrides(raw: string | undefined): Record<string, string> {
    if (!raw || !raw.trim()) {
      return {};
    }

    try {
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object') {
        return {};
      }

      const out: Record<string, string> = {};
      for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
        const normalizedKey = key.trim().toLowerCase();
        const normalizedValue =
          typeof value === 'string' ? value.trim().toUpperCase() : '';
        if (normalizedKey && normalizedValue) {
          out[normalizedKey] = normalizedValue;
        }
      }

      return out;
    } catch {
      return {};
    }
  }

  private async fetchAllbridgeChains(): Promise<Record<string, AllbridgeChainDetails>> {
    const now = Date.now();
    if (
      this.allbridgeChainsCache &&
      now - this.allbridgeChainsCache.timestamp < this.allbridgeChainsCacheTtlMs
    ) {
      return this.allbridgeChainsCache.data;
    }

    let normalized = this.normalizeAllbridgeChainsPayload(
      await this.requestAllbridgeEndpoint('/chains', {}),
    );

    // Public Core API deployments often expose token metadata via /token-info
    // while custom REST deployments expose /chains with token lists.
    if (!this.hasAnyAllbridgeTokens(normalized)) {
      normalized = this.normalizeAllbridgeChainsPayload(
        await this.requestAllbridgeEndpoint('/token-info', {
          filter: 'all',
        }),
      );
    }

    if (!this.hasAnyAllbridgeTokens(normalized)) {
      throw new BadRequestException(
        'Allbridge chain metadata endpoint did not return token lists. Configure STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_API_URL to an Allbridge Core REST API deployment that supports /chains or /token-info with tokens.',
      );
    }

    this.allbridgeChainsCache = { timestamp: now, data: normalized };
    return normalized;
  }

  private normalizeAllbridgeChainsPayload(payload: unknown): Record<string, AllbridgeChainDetails> {
    if (!payload || typeof payload !== 'object') {
      return {};
    }

    const normalized: Record<string, AllbridgeChainDetails> = {};
    for (const [chainSymbol, value] of Object.entries(payload as Record<string, unknown>)) {
      if (!value || typeof value !== 'object') {
        continue;
      }
      const chainDetails = value as Record<string, unknown>;
      const tokens = Array.isArray(chainDetails.tokens)
        ? chainDetails.tokens.filter((token) => token && typeof token === 'object')
        : [];
      normalized[chainSymbol.toUpperCase()] = {
        chainSymbol: chainSymbol.toUpperCase(),
        tokens: tokens as AllbridgeTokenDetails[],
      };
    }

    return normalized;
  }

  private hasAnyAllbridgeTokens(chains: Record<string, AllbridgeChainDetails>): boolean {
    return Object.values(chains).some(
      (chain) => Array.isArray(chain.tokens) && chain.tokens.length > 0,
    );
  }

  private resolveAllbridgeToken(params: {
    chainsMap: Record<string, AllbridgeChainDetails>;
    chainSymbol: string;
    requestedToken: string;
    expectedSymbol: string;
  }): AllbridgeTokenDetails {
    const chain = params.chainsMap[params.chainSymbol.toUpperCase()];
    if (!chain || !Array.isArray(chain.tokens) || chain.tokens.length === 0) {
      throw new BadRequestException(
        `Allbridge chain metadata did not return tokens for chain "${params.chainSymbol}"`,
      );
    }

    const requested = params.requestedToken.trim().toLowerCase();
    const expectedSymbol = (params.expectedSymbol || this.allbridgeTokenSymbol).trim().toUpperCase();
    const byAddress = chain.tokens.find((token) => {
      if (!token || typeof token.tokenAddress !== 'string') {
        return false;
      }
      return token.tokenAddress.trim().toLowerCase() === requested;
    });
    if (byAddress) {
      return byAddress;
    }

    const bySymbol = chain.tokens.find(
      (token) => (token.symbol || '').trim().toUpperCase() === expectedSymbol,
    );
    if (bySymbol) {
      return bySymbol;
    }

    throw new BadRequestException(
      `Allbridge token "${params.requestedToken}" (expected symbol ${expectedSymbol}) is not available on chain "${params.chainSymbol}"`,
    );
  }

  private requireAllbridgeTokenAddress(token: AllbridgeTokenDetails, chainSymbol: string): string {
    const tokenAddress = (token.tokenAddress || '').trim();
    if (!tokenAddress) {
      throw new BadRequestException(
        `Allbridge token ${(token.symbol || 'UNKNOWN').trim()} on ${chainSymbol} does not include tokenAddress`,
      );
    }
    return tokenAddress;
  }

  private async buildAllbridgeApprovalTransaction(params: {
    sourceChainSymbol: string;
    sourceToken: AllbridgeTokenDetails;
    sourceAddress: string;
    sourceAmount: string;
    sourceAmountBaseUnits: string;
  }): Promise<{ to: string; data: string; value: string; from?: string } | null> {
    const chain = params.sourceChainSymbol.toUpperCase();
    if (chain === 'STLR' || chain === 'SRB' || chain === 'SOL' || chain === 'SUI' || chain === 'TRX') {
      return null;
    }

    const tokenAddress = (params.sourceToken.tokenAddress || '').trim();
    if (!tokenAddress) {
      return null;
    }

    const allowanceResponse = await this.requestAllbridgeEndpoint('/check/bridge/allowance', {
      ownerAddress: params.sourceAddress,
      tokenAddress,
      amount: params.sourceAmountBaseUnits || params.sourceAmount,
      feePaymentMethod: this.allbridgeGasFeePaymentMethod || 'WITH_NATIVE_CURRENCY',
    });
    if (this.readAllbridgeAllowanceResult(allowanceResponse)) {
      return null;
    }

    const approvalResponse = await this.requestAllbridgeEndpoint('/raw/bridge/approve', {
      ownerAddress: params.sourceAddress,
      tokenAddress,
      amount: params.sourceAmountBaseUnits || params.sourceAmount,
      messenger: this.allbridgeMessenger,
    });
    return this.extractEvmTransaction(approvalResponse);
  }

  private readAllbridgeAllowanceResult(payload: unknown): boolean {
    if (typeof payload === 'boolean') {
      return payload;
    }
    if (!payload || typeof payload !== 'object') {
      return false;
    }
    const record = payload as Record<string, unknown>;
    const boolKeys = [
      'allowed',
      'isAllowed',
      'sufficient',
      'hasAllowance',
      'enough',
      'result',
      'success',
    ];
    for (const key of boolKeys) {
      if (typeof record[key] === 'boolean') {
        return record[key] as boolean;
      }
    }
    if (typeof record.allowance === 'string') {
      try {
        return BigInt(record.allowance) > 0n;
      } catch {
        return false;
      }
    }
    return false;
  }

  private async requestAllbridgeEndpoint(
    path: string,
    query: Record<string, string>,
  ): Promise<unknown> {
    const base = this.allbridgeApiBaseUrl.trim();
    if (!base) {
      throw new BadRequestException(
        'Allbridge API URL is not configured. Set STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_API_URL.',
      );
    }

    const headers: Record<string, string> = { ...this.allbridgeHeaders };
    if (this.allbridgeApiKey) {
      headers['x-api-key'] = this.allbridgeApiKey;
    }

    try {
      const response = await axios.get(`${base.replace(/\/+$/, '')}${path}`, {
        params: query,
        timeout: this.allbridgeApiTimeoutMs,
        ...(Object.keys(headers).length > 0 ? { headers } : {}),
      });
      return response.data;
    } catch (error: unknown) {
      if (
        axios.isAxiosError(error) &&
        typeof error.response?.data === 'object' &&
        error.response?.data !== null &&
        String((error.response.data as Record<string, unknown>).message || '')
          .toLowerCase()
          .includes('missing authentication token')
      ) {
        throw new BadRequestException(
          `Allbridge endpoint "${base}" does not expose "${path}". Configure STELLAR_CIRCLE_CCTP_V2_CUSTOM_ALLBRIDGE_API_URL to an Allbridge Core REST API deployment that supports raw transaction endpoints (/raw/bridge, /raw/approve, /check/allowance).`,
        );
      }
      throw new BadRequestException(
        `Allbridge request failed for ${path}: ${this.describeAxiosError(error)}`,
      );
    }
  }

  private describeAxiosError(error: unknown): string {
    if (!axios.isAxiosError(error)) {
      return error instanceof Error ? error.message : String(error);
    }
    const status = error.response?.status;
    const data = error.response?.data;
    const message =
      (data &&
        typeof data === 'object' &&
        (data as Record<string, unknown>).message &&
        String((data as Record<string, unknown>).message)) ||
      (typeof data === 'string' ? data : '') ||
      error.message;
    return status ? `${status} ${message}` : message;
  }

  private extractStellarTransactionXdr(payload: unknown): string | null {
    if (typeof payload === 'string' && payload.trim()) {
      return payload.trim();
    }
    if (!payload || typeof payload !== 'object') {
      return null;
    }

    const candidates: unknown[] = [];
    const record = payload as Record<string, unknown>;
    candidates.push(record.xdr, record.tx, record.transaction, record.rawTx, record.envelopeXdr);
    if (record.tx && typeof record.tx === 'object') {
      const tx = record.tx as Record<string, unknown>;
      candidates.push(tx.xdr, tx.envelopeXdr, tx.transactionXdr);
    }
    if (record.transaction && typeof record.transaction === 'object') {
      const tx = record.transaction as Record<string, unknown>;
      candidates.push(tx.xdr, tx.envelopeXdr, tx.transactionXdr);
    }

    for (const candidate of candidates) {
      if (typeof candidate === 'string' && candidate.trim()) {
        return candidate.trim();
      }
    }
    return null;
  }

  private extractEvmTransaction(
    payload: unknown,
  ): { to: string; data: string; value: string; from?: string } | null {
    if (!payload || typeof payload !== 'object') {
      return null;
    }

    const queue: Array<Record<string, unknown>> = [payload as Record<string, unknown>];
    const visited = new Set<Record<string, unknown>>();
    while (queue.length > 0) {
      const current = queue.shift() as Record<string, unknown>;
      if (visited.has(current)) {
        continue;
      }
      visited.add(current);

      const to = typeof current.to === 'string' ? current.to.trim() : '';
      const data = typeof current.data === 'string' ? current.data.trim() : '';
      if (to && data && this.looksLikeEvmAddress(to) && data.startsWith('0x')) {
        const value = this.normalizeTxValue(current.value);
        const fromCandidate = typeof current.from === 'string' ? current.from.trim() : '';
        return {
          to,
          data,
          value,
          ...(fromCandidate && this.looksLikeEvmAddress(fromCandidate)
            ? { from: fromCandidate }
            : {}),
        };
      }

      for (const key of ['tx', 'transaction', 'rawTx', 'rawTransaction', 'result']) {
        const nested = current[key];
        if (nested && typeof nested === 'object' && !Array.isArray(nested)) {
          queue.push(nested as Record<string, unknown>);
        }
      }
    }

    return null;
  }

  private looksLikeEvmAddress(value: string): boolean {
    try {
      ethers.getAddress(value);
      return true;
    } catch {
      return false;
    }
  }

  private normalizeTxValue(value: unknown): string {
    if (value === null || value === undefined || value === '') {
      return '0';
    }
    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (!trimmed) {
        return '0';
      }
      try {
        return BigInt(trimmed).toString();
      } catch {
        return '0';
      }
    }
    if (typeof value === 'number' && Number.isFinite(value) && Number.isInteger(value) && value >= 0) {
      return BigInt(value).toString();
    }
    return '0';
  }

  private stringifyCompact(value: unknown): string {
    try {
      const serialized = JSON.stringify(value);
      return serialized.length > 800 ? `${serialized.slice(0, 800)}...` : serialized;
    } catch {
      return String(value);
    }
  }

  private resolveStellarNetwork(): 'testnet' | 'mainnet' {
    const normalizedOverride = this.simulatedStellarNetwork.trim().toLowerCase();
    if (normalizedOverride === 'testnet') {
      return 'testnet';
    }
    if (normalizedOverride === 'mainnet' || normalizedOverride === 'public') {
      return 'mainnet';
    }

    const configured = (process.env.STELLAR_NETWORK || 'testnet').trim().toLowerCase();
    return configured === 'mainnet' || configured === 'public' ? 'mainnet' : 'testnet';
  }

  private networkPassphrase(network: 'testnet' | 'mainnet'): string {
    return network === 'mainnet' ? StellarSdk.Networks.PUBLIC : StellarSdk.Networks.TESTNET;
  }

  private async resolveBaseFee(server: StellarSdk.Horizon.Server): Promise<string> {
    try {
      const fee = await server.fetchBaseFee();
      if (typeof fee === 'number' && Number.isFinite(fee) && fee > 0) {
        return Math.floor(fee).toString();
      }
    } catch {
      // Keep fallback fee for local simulation mode.
    }
    return '100';
  }

  private normalizeEvmAddress(address: string): string | null {
    try {
      return ethers.getAddress(address.trim());
    } catch {
      return null;
    }
  }

  private looksLikeStellarAddress(address: string): boolean {
    try {
      return StellarSdk.StrKey.isValidEd25519PublicKey(address.trim());
    } catch {
      return false;
    }
  }

  private normalizeStellarAmount(rawAmount: string): string {
    const trimmed = rawAmount.trim();
    if (!/^\d+(\.\d+)?$/.test(trimmed)) {
      throw new BadRequestException('simulate-stellar-xdr mode requires a decimal srcAmount');
    }

    const [integerPartRaw, fractionalPartRaw = ''] = trimmed.split('.');
    if (fractionalPartRaw.length > 7) {
      throw new BadRequestException('simulate-stellar-xdr mode supports up to 7 decimal places');
    }

    const integerPart = BigInt(integerPartRaw);
    const fractionalPart = BigInt(fractionalPartRaw.padEnd(7, '0') || '0');
    const baseUnits = integerPart * STELLAR_BASE_UNITS + fractionalPart;
    if (baseUnits <= 0n) {
      throw new BadRequestException('simulate-stellar-xdr mode requires srcAmount > 0');
    }

    const whole = baseUnits / STELLAR_BASE_UNITS;
    const fraction = (baseUnits % STELLAR_BASE_UNITS).toString().padStart(7, '0').replace(/0+$/, '');
    return fraction ? `${whole.toString()}.${fraction}` : whole.toString();
  }
}
