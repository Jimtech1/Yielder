import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import axios from 'axios';
import { AnchorQuoteDto } from './dto/anchor-quote.dto';
import { AnchorFlow, AnchorSessionDto } from './dto/anchor-session.dto';
import { AnchorAuthChallengeDto } from './dto/anchor-auth-challenge.dto';
import { AnchorAuthTokenDto } from './dto/anchor-auth-token.dto';
import { AnchorTransactionDto } from './dto/anchor-transaction.dto';

@Injectable()
export class AnchorService {
  private readonly logger = new Logger(AnchorService.name);
  private readonly anchorBaseUrl = process.env.ANCHOR_API_BASE_URL;
  private readonly quoteBaseUrl = process.env.ANCHOR_QUOTE_URL || process.env.ANCHOR_API_BASE_URL;
  private readonly webAuthUrl = this.resolveWebAuthUrl();

  async getHealth() {
    return {
      configured: Boolean(this.anchorBaseUrl),
      anchorBaseUrl: this.anchorBaseUrl || null,
      quoteBaseUrl: this.quoteBaseUrl || null,
      webAuthUrl: this.webAuthUrl || null,
      supportedFlows: ['deposit', 'withdraw'],
    };
  }

  async getWebAuthChallenge(dto: AnchorAuthChallengeDto) {
    if (!this.webAuthUrl) {
      throw new BadRequestException(
        'Anchor web auth is not configured. Set ANCHOR_WEB_AUTH_URL or ANCHOR_API_BASE_URL.',
      );
    }

    try {
      const response = await axios.get(this.webAuthUrl, {
        params: {
          account: dto.account,
          ...(dto.memo ? { memo: dto.memo } : {}),
        },
        timeout: 15000,
      });

      return response.data;
    } catch (error: any) {
      this.logger.error(`Anchor web auth challenge failed: ${error.message}`);
      throw new BadRequestException({
        message: 'Failed to fetch anchor web auth challenge',
        details: error.response?.data || error.message,
      });
    }
  }

  async exchangeWebAuthToken(dto: AnchorAuthTokenDto) {
    if (!this.webAuthUrl) {
      throw new BadRequestException(
        'Anchor web auth is not configured. Set ANCHOR_WEB_AUTH_URL or ANCHOR_API_BASE_URL.',
      );
    }

    try {
      const response = await axios.post(
        this.webAuthUrl,
        {
          transaction: dto.transaction,
        },
        {
          timeout: 15000,
        },
      );

      return response.data;
    } catch (error: any) {
      this.logger.error(`Anchor web auth token exchange failed: ${error.message}`);
      throw new BadRequestException({
        message: 'Failed to exchange anchor web auth token',
        details: error.response?.data || error.message,
      });
    }
  }

  async getInfo() {
    if (!this.anchorBaseUrl) {
      return {
        configured: false,
        message: 'Anchor API is not configured. Set ANCHOR_API_BASE_URL to enable SEP-6/SEP-24 flows.',
      };
    }

    try {
      const response = await axios.get(`${this.anchorBaseUrl}/info`, {
        timeout: 15000,
      });
      return response.data;
    } catch (error: any) {
      this.logger.error(`Anchor /info failed: ${error.message}`);
      throw new BadRequestException({
        message: 'Failed to fetch anchor info',
        details: error.response?.data || error.message,
      });
    }
  }

  async getQuote(dto: AnchorQuoteDto) {
    if (!dto.sellAmount && !dto.buyAmount) {
      throw new BadRequestException('Either sellAmount or buyAmount must be provided');
    }

    if (!this.quoteBaseUrl) {
      throw new BadRequestException(
        'Anchor quotes are not configured. Set ANCHOR_QUOTE_URL or ANCHOR_API_BASE_URL.',
      );
    }

    const payload = {
      sell_asset: dto.sellAsset,
      buy_asset: dto.buyAsset,
      ...(dto.sellAmount ? { sell_amount: dto.sellAmount } : {}),
      ...(dto.buyAmount ? { buy_amount: dto.buyAmount } : {}),
      ...(dto.countryCode ? { country_code: dto.countryCode } : {}),
    };
    const headers = dto.authToken
      ? { Authorization: `Bearer ${dto.authToken}` }
      : undefined;
    const quoteContexts = ['sep24', 'sep6'];
    let lastPostError: any = null;
    let lastGetError: any = null;

    for (const context of quoteContexts) {
      const contextPayload = {
        context,
        ...payload,
      };
      try {
        const response = await axios.post(
          `${this.quoteBaseUrl}/quote`,
          contextPayload,
          {
            headers,
            timeout: 15000,
          },
        );
        return response.data;
      } catch (postError: any) {
        lastPostError = postError;
        try {
          const response = await axios.get(`${this.quoteBaseUrl}/quote`, {
            params: contextPayload,
            headers,
            timeout: 15000,
          });
          return response.data;
        } catch (getError: any) {
          lastGetError = getError;
          this.logger.warn(
            `Anchor quote request failed for context=${context}: POST=${postError.message}, GET=${getError.message}`,
          );
        }
      }
    }

    this.logger.error('Anchor quote request failed for all supported contexts (sep24, sep6)');
    throw new BadRequestException({
      message: 'Failed to fetch anchor quote',
      details:
        lastGetError?.response?.data ||
        lastPostError?.response?.data ||
        lastGetError?.message ||
        lastPostError?.message,
    });
  }

  async createInteractiveSession(dto: AnchorSessionDto) {
    if (!this.anchorBaseUrl) {
      return {
        configured: false,
        status: 'not_configured',
        flow: dto.flow,
        message: 'Set ANCHOR_API_BASE_URL to enable interactive anchor sessions.',
      };
    }

    const endpointCandidates =
      dto.flow === AnchorFlow.DEPOSIT
        ? ['deposit']
        : ['withdraw', 'withdrawal'];
    const requestPayload = {
      asset_code: dto.assetCode,
      account: dto.account,
      ...(dto.amount ? { amount: dto.amount } : {}),
      ...(dto.quoteId ? { quote_id: dto.quoteId } : {}),
      ...(dto.lang ? { lang: dto.lang } : {}),
      ...(dto.countryCode ? { country_code: dto.countryCode } : {}),
    };
    const requestHeaders = dto.authToken
      ? { Authorization: `Bearer ${dto.authToken}` }
      : undefined;
    let lastPostError: any = null;
    let lastGetError: any = null;

    for (const endpoint of endpointCandidates) {
      const requestUrl = `${this.anchorBaseUrl}/transactions/${endpoint}/interactive`;
      try {
        const response = await axios.post(
          requestUrl,
          requestPayload,
          {
            headers: requestHeaders,
            timeout: 15000,
          },
        );
        return response.data;
      } catch (postError: any) {
        lastPostError = postError;

        // Some anchors only expose SEP-24 interactive endpoints as GET with query params.
        // Try GET as a compatibility fallback before surfacing the failure.
        const postStatus = Number(postError?.response?.status);
        const shouldTryGetFallback = ![401, 403].includes(postStatus);

        if (!shouldTryGetFallback) {
          continue;
        }

        try {
          const getResponse = await axios.get(requestUrl, {
            params: requestPayload,
            headers: requestHeaders,
            timeout: 15000,
          });
          return getResponse.data;
        } catch (getError: any) {
          lastGetError = getError;
          this.logger.warn(
            `Anchor interactive session attempt failed for endpoint=${endpoint}: POST=${postError.message}, GET=${getError.message}`,
          );
        }
      }
    }

    const providerMessage =
      this.extractProviderErrorMessage(lastGetError) ||
      this.extractProviderErrorMessage(lastPostError);
    throw new BadRequestException({
      message: providerMessage
        ? `Failed to create interactive anchor session: ${providerMessage}`
        : 'Failed to create interactive anchor session',
      details: {
        post: lastPostError?.response?.data || lastPostError?.message,
        get: lastGetError?.response?.data || lastGetError?.message,
      },
    });
  }

  async getTransaction(dto: AnchorTransactionDto) {
    if (!this.anchorBaseUrl) {
      throw new BadRequestException(
        'Anchor API is not configured. Set ANCHOR_API_BASE_URL to enable transaction lookups.',
      );
    }

    const id = dto.id?.trim();
    const externalTransactionId = dto.externalTransactionId?.trim();
    const stellarTransactionId = dto.stellarTransactionId?.trim();
    if (!id && !externalTransactionId && !stellarTransactionId) {
      throw new BadRequestException(
        'Provide one of id, externalTransactionId, or stellarTransactionId',
      );
    }

    try {
      const response = await axios.get(`${this.anchorBaseUrl}/transaction`, {
        params: {
          ...(id ? { id } : {}),
          ...(externalTransactionId ? { external_transaction_id: externalTransactionId } : {}),
          ...(stellarTransactionId ? { stellar_transaction_id: stellarTransactionId } : {}),
          ...(dto.lang?.trim() ? { lang: dto.lang.trim() } : {}),
        },
        ...(dto.authToken?.trim()
          ? {
              headers: {
                Authorization: `Bearer ${dto.authToken.trim()}`,
              },
            }
          : {}),
        timeout: 15000,
      });

      return response.data;
    } catch (error: any) {
      this.logger.error(`Anchor transaction lookup failed: ${error.message}`);
      throw new BadRequestException({
        message: 'Failed to fetch anchor transaction',
        details: error.response?.data || error.message,
      });
    }
  }

  private extractProviderErrorMessage(error: any): string | null {
    const data = error?.response?.data;
    if (!data) {
      return typeof error?.message === 'string' ? error.message : null;
    }

    if (typeof data === 'string') {
      const trimmed = data.trim();
      return trimmed || null;
    }

    if (typeof data === 'object') {
      const messageFromObject =
        (typeof data.error_description === 'string' && data.error_description) ||
        (typeof data.error === 'string' && data.error) ||
        (typeof data.detail === 'string' && data.detail) ||
        (typeof data.message === 'string' && data.message) ||
        (Array.isArray(data.message)
          ? data.message.filter((entry: unknown) => typeof entry === 'string').join(', ')
          : null);

      if (messageFromObject) {
        return messageFromObject;
      }
    }

    return typeof error?.message === 'string' ? error.message : null;
  }

  private resolveWebAuthUrl(): string | undefined {
    if (process.env.ANCHOR_WEB_AUTH_URL) {
      return process.env.ANCHOR_WEB_AUTH_URL;
    }

    if (!this.anchorBaseUrl) {
      return undefined;
    }

    try {
      const base = new URL(this.anchorBaseUrl);
      return `${base.origin}/auth`;
    } catch {
      return undefined;
    }
  }
}
