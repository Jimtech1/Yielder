import { Injectable, UnauthorizedException } from '@nestjs/common';
import { PassportStrategy } from '@nestjs/passport';
import { ExtractJwt, Strategy } from 'passport-jwt';
import { ConfigService } from '@nestjs/config';
import { AuthService } from './auth.service';

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor(
    private authService: AuthService,
    private configService: ConfigService,
  ) {
    const jwtSecret = configService.get<string>('JWT_SECRET');
    if (!jwtSecret) {
      throw new Error('JWT_SECRET must be configured');
    }

    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      secretOrKey: jwtSecret,
    });
  }

  async validate(payload: any) {
    const user = await this.authService.validateUser(payload.sub);
    if (!user) throw new UnauthorizedException();
    if ((user as any).isSuspended === true) {
      throw new UnauthorizedException('Account is suspended');
    }
    const currentTokenVersion =
      typeof (user as any).tokenVersion === 'number' &&
      Number.isFinite((user as any).tokenVersion)
        ? Math.max(0, Math.floor((user as any).tokenVersion))
        : 0;
    const payloadTokenVersion =
      typeof payload?.tokenVersion === 'number' &&
      Number.isFinite(payload.tokenVersion)
        ? Math.max(0, Math.floor(payload.tokenVersion))
        : 0;
    if (payloadTokenVersion !== currentTokenVersion) {
      throw new UnauthorizedException('Session has been revoked');
    }
    const userId = (user as any)._id.toString();
    const subscriptionTier =
      typeof (user as any).subscriptionTier === 'string'
        ? (user as any).subscriptionTier
        : 'free';
    return {
      id: userId,
      userId,
      email: user.email,
      role: user.role,
      subscriptionTier,
      tokenVersion: currentTokenVersion,
    };
  }
}
