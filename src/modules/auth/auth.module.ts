import { Module } from '@nestjs/common';
import { JwtModule } from '@nestjs/jwt';
import { PassportModule } from '@nestjs/passport';
import { MongooseModule } from '@nestjs/mongoose';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { AuthService } from './auth.service';
import { AuthController } from './auth.controller';
import { JwtStrategy } from './jwt.strategy';
import { User, UserSchema } from './schemas/user.schema';
import { RefreshToken, RefreshTokenSchema } from './schemas/refresh-token.schema';
import { PasswordReset, PasswordResetSchema } from './schemas/password-reset.schema';
import { AuthChallenge, AuthChallengeSchema } from './schemas/auth-challenge.schema';
import { ConnectedWallet, ConnectedWalletSchema } from './schemas/connected-wallet.schema';
import { ChallengeService } from './services/challenge.service';
import { SignatureVerificationService } from './services/signature-verification.service';
import { TurnstileVerificationService } from './services/turnstile-verification.service';
import { AccessModule } from '../access/access.module';
import { NotificationsModule } from '../notifications/notifications.module';
import { Wallet, WalletSchema } from '../wallet/schemas/wallet.schema';
import { Transaction, TransactionSchema } from '../wallet/schemas/transaction.schema';
import {
  AdminAuditEvent,
  AdminAuditEventSchema,
} from '../notifications/schemas/admin-audit-event.schema';

@Module({
  imports: [
    ConfigModule,
    AccessModule,
    NotificationsModule,
    MongooseModule.forFeature([
      { name: User.name, schema: UserSchema },
      { name: RefreshToken.name, schema: RefreshTokenSchema },
      { name: PasswordReset.name, schema: PasswordResetSchema },
      { name: AuthChallenge.name, schema: AuthChallengeSchema },
      { name: ConnectedWallet.name, schema: ConnectedWalletSchema },
      { name: Wallet.name, schema: WalletSchema },
      { name: Transaction.name, schema: TransactionSchema },
      { name: AdminAuditEvent.name, schema: AdminAuditEventSchema },
    ]),
    PassportModule,
    JwtModule.registerAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        const jwtSecret = configService.get<string>('JWT_SECRET');
        if (!jwtSecret) {
          throw new Error('JWT_SECRET must be configured');
        }

        return {
          secret: jwtSecret,
        };
      },
    }),
  ],
  controllers: [AuthController],
  providers: [
    AuthService, 
    JwtStrategy,
    ChallengeService,
    SignatureVerificationService,
    TurnstileVerificationService,
  ],
  exports: [AuthService],
})
export class AuthModule {}
