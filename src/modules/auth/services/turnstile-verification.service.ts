import { BadRequestException, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';

interface TurnstileVerifyResponse {
  success: boolean;
  'error-codes'?: string[];
}

@Injectable()
export class TurnstileVerificationService {
  private readonly verifyUrl = 'https://challenges.cloudflare.com/turnstile/v0/siteverify';

  constructor(private readonly configService: ConfigService) {}

  async verify(turnstileToken?: string, remoteIp?: string): Promise<void> {
    const secret = this.configService.get<string>('TURNSTILE_SECRET_KEY');

    // Keep auth usable in local/dev environments where Turnstile is not configured.
    if (!secret) {
      return;
    }

    if (!turnstileToken) {
      throw new BadRequestException('Cloudflare verification is required');
    }

    const payload = new URLSearchParams();
    payload.set('secret', secret);
    payload.set('response', turnstileToken);
    if (remoteIp) {
      payload.set('remoteip', remoteIp);
    }

    try {
      const { data } = await axios.post<TurnstileVerifyResponse>(
        this.verifyUrl,
        payload.toString(),
        {
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          timeout: 10000,
        },
      );

      if (!data.success) {
        const errorCode = data['error-codes']?.join(', ');
        const suffix = errorCode ? `: ${errorCode}` : '';
        throw new BadRequestException(`Cloudflare verification failed${suffix}`);
      }
    } catch (error) {
      if (error instanceof BadRequestException) {
        throw error;
      }
      throw new BadRequestException('Cloudflare verification failed');
    }
  }
}
