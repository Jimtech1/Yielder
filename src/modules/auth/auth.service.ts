import {
  Injectable,
  UnauthorizedException,
  BadRequestException,
  NotFoundException,
  ForbiddenException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { ConfigService } from '@nestjs/config';
import { InjectModel } from '@nestjs/mongoose';
import { Model, Types } from 'mongoose';
import * as argon2 from 'argon2';
import { createHash, randomBytes } from 'crypto';
import axios from 'axios';
import { RegisterDto, LoginDto, RefreshTokenDto, ForgotPasswordDto, ResetPasswordDto, GoogleLoginDto } from './dto';
import { WalletChallengeDto, WalletLoginDto } from './dto/wallet-auth.dto';
import { User, UserDocument } from './schemas/user.schema';
import { RefreshToken, RefreshTokenDocument } from './schemas/refresh-token.schema';
import { PasswordReset, PasswordResetDocument } from './schemas/password-reset.schema';
import { ConnectedWallet, ConnectedWalletDocument } from './schemas/connected-wallet.schema';
import { ChallengeService } from './services/challenge.service';
import { SignatureVerificationService } from './services/signature-verification.service';
import { TurnstileVerificationService } from './services/turnstile-verification.service';
import { AccessControlService } from '../access/access-control.service';
import { Wallet, WalletDocument } from '../wallet/schemas/wallet.schema';
import { Transaction, TransactionDocument } from '../wallet/schemas/transaction.schema';
import {
  AdminAuditEvent,
  AdminAuditEventDocument,
} from '../notifications/schemas/admin-audit-event.schema';

interface GoogleTokenInfoResponse {
  aud: string;
  email?: string;
  email_verified?: string | boolean;
  exp?: string;
}

@Injectable()
export class AuthService {
  constructor(
    @InjectModel(User.name) private userModel: Model<UserDocument>,
    @InjectModel(RefreshToken.name) private refreshTokenModel: Model<RefreshTokenDocument>,
    @InjectModel(PasswordReset.name) private passwordResetModel: Model<PasswordResetDocument>,
    @InjectModel(ConnectedWallet.name) private connectedWalletModel: Model<ConnectedWalletDocument>,
    @InjectModel(Wallet.name) private walletModel: Model<WalletDocument>,
    @InjectModel(Transaction.name) private transactionModel: Model<TransactionDocument>,
    @InjectModel(AdminAuditEvent.name)
    private adminAuditEventModel: Model<AdminAuditEventDocument>,
    private jwtService: JwtService,
    private configService: ConfigService,
    private challengeService: ChallengeService,
    private signatureService: SignatureVerificationService,
    private turnstileVerificationService: TurnstileVerificationService,
    private accessControlService: AccessControlService,
  ) {}

  async generateWalletChallenge(dto: WalletChallengeDto) {
    const publicKey = dto.publicKey.trim();
    return this.challengeService.generateChallenge(publicKey);
  }

  async getConnectedWalletSummaries(userId: string): Promise<
    Array<{
      publicKey: string;
      chain: 'stellar' | 'evm';
      walletType: string;
      isPrimary: boolean;
      label?: string;
    }>
  > {
    const wallets = (await this.connectedWalletModel
      .find({ userId } as any)
      .select('publicKey walletType isPrimary label createdAt')
      .sort({ isPrimary: -1, createdAt: 1 })
      .lean()
      .exec()) as Array<Record<string, unknown>>;

    return wallets.map((wallet) => {
      const publicKey = typeof wallet.publicKey === 'string' ? wallet.publicKey : '';
      const walletType = typeof wallet.walletType === 'string' ? wallet.walletType : 'unknown';
      const label = typeof wallet.label === 'string' ? wallet.label.trim() : '';

      return {
        publicKey,
        chain: this.resolveConnectedWalletChain(walletType, publicKey),
        walletType,
        isPrimary: wallet.isPrimary === true,
        ...(label ? { label } : {}),
      };
    });
  }

  async linkWalletForUser(userId: string, dto: WalletLoginDto) {
    const publicKey = dto.publicKey.trim();
    const challengeMessage =
      dto.challenge ?? (await this.challengeService.getLastChallengeMessage(publicKey));

    if (!challengeMessage) {
      throw new UnauthorizedException('No active challenge found');
    }

    const isValid = this.signatureService.verifyEd25519Signature(
      publicKey,
      challengeMessage,
      dto.signature,
    );
    if (!isValid) {
      throw new UnauthorizedException('Invalid signature');
    }

    const consumed = await this.challengeService.consumeChallenge(publicKey, challengeMessage);
    if (!consumed) {
      throw new UnauthorizedException('Challenge expired or already used');
    }

    const user = await this.userModel.findById(userId).select('_id email isSuspended').exec();
    if (!user) {
      throw new UnauthorizedException('User not found');
    }
    this.assertUserNotSuspended(user);

    const walletType = this.resolveWalletType(publicKey);
    const existingWallet = await this.connectedWalletModel.findOne({ publicKey } as any).exec();
    const hasPrimaryWallet =
      (await this.connectedWalletModel.exists({ userId, isPrimary: true } as any)) !== null;

    let linkedWallet: ConnectedWalletDocument;
    let linked = false;

    if (existingWallet) {
      const existingWalletUserId = String((existingWallet as { userId: unknown }).userId);
      if (existingWalletUserId !== userId) {
        throw new ForbiddenException('Wallet is already linked to another account');
      }

      existingWallet.walletType = walletType;
      existingWallet.lastUsedAt = new Date();
      if (!existingWallet.isPrimary && !hasPrimaryWallet) {
        existingWallet.isPrimary = true;
      }
      linkedWallet = await existingWallet.save();
    } else {
      linkedWallet = await this.connectedWalletModel.create({
        userId,
        publicKey,
        walletType,
        isPrimary: !hasPrimaryWallet,
        lastUsedAt: new Date(),
      } as any);
      linked = true;
    }

    return {
      linked,
      wallet: {
        publicKey: linkedWallet.publicKey,
        chain: this.resolveConnectedWalletChain(linkedWallet.walletType, linkedWallet.publicKey),
        walletType: linkedWallet.walletType,
        isPrimary: linkedWallet.isPrimary === true,
        label: linkedWallet.label || null,
      },
      wallets: await this.getConnectedWalletSummaries(userId),
    };
  }

  async authenticateWithWallet(dto: WalletLoginDto, remoteIp?: string) {
    await this.turnstileVerificationService.verify(dto.turnstileToken, remoteIp);
    const publicKey = dto.publicKey.trim();

    const challengeMessage =
      dto.challenge ??
      (await this.challengeService.getLastChallengeMessage(publicKey));

    if (!challengeMessage) {
      throw new UnauthorizedException('No active challenge found');
    }

    const isValid = this.signatureService.verifyEd25519Signature(
      publicKey,
      challengeMessage,
      dto.signature,
    );

    if (!isValid) {
      throw new UnauthorizedException('Invalid signature');
    }

    const consumed = await this.challengeService.consumeChallenge(
      publicKey,
      challengeMessage,
    );

    if (!consumed) {
      throw new UnauthorizedException('Challenge expired or already used');
    }

    let connectedWallet = await this.connectedWalletModel
      .findOne({ publicKey } as any)
      .populate('userId');

    let user: UserDocument | null = null;

    if (connectedWallet) {
      connectedWallet.lastUsedAt = new Date();
      connectedWallet.walletType = this.resolveWalletType(publicKey);
      await connectedWallet.save();
      user = connectedWallet.userId as UserDocument;
    } else {
      const createdUser = await this.userModel.create({
        registrationType: 'wallet',
        role: 'user',
      });

      user = createdUser as UserDocument;

      await this.connectedWalletModel.create({
        userId: user._id,
        publicKey,
        walletType: this.resolveWalletType(publicKey),
        isPrimary: true,
        lastUsedAt: new Date(),
      } as any);
    }

    if (!user) {
      throw new UnauthorizedException('User resolution failed');
    }

    this.assertUserNotSuspended(user);
    return this.generateTokens(user);
  }

  async register(dto: RegisterDto, remoteIp?: string) {
    await this.turnstileVerificationService.verify(dto.turnstileToken, remoteIp);

    const existing = await this.userModel.findOne({ email: dto.email });
    if (existing) throw new BadRequestException('User already exists');

    const passwordHash = await argon2.hash(dto.password);
    const user = await this.userModel.create({
      email: dto.email,
      passwordHash,
      role: 'user',
      registrationType: 'email',
    });

    return this.generateTokens(user);
  }

  async login(dto: LoginDto, remoteIp?: string) {
    await this.turnstileVerificationService.verify(dto.turnstileToken, remoteIp);

    const user = await this.userModel.findOne({ email: dto.email });
    if (!user) throw new UnauthorizedException('Invalid credentials');
    this.assertUserNotSuspended(user);

    if (!user.passwordHash) {
      if (user.registrationType === 'google') {
        throw new UnauthorizedException('Please login with Google');
      }
      throw new UnauthorizedException('Please login with your wallet');
    }

    const valid = await argon2.verify(user.passwordHash, dto.password);
    if (!valid) throw new UnauthorizedException('Invalid credentials');

    return this.generateTokens(user);
  }

  async loginWithGoogle(dto: GoogleLoginDto, remoteIp?: string) {
    await this.turnstileVerificationService.verify(dto.turnstileToken, remoteIp);

    const googleProfile = await this.verifyGoogleIdToken(dto.idToken);
    let user = await this.userModel.findOne({ email: googleProfile.email });

    if (!user) {
      user = await this.userModel.create({
        email: googleProfile.email,
        role: 'user',
        registrationType: 'google',
        emailVerified: true,
      });
    } else if (!user.emailVerified) {
      user.emailVerified = true;
      await user.save();
    }

    this.assertUserNotSuspended(user);
    return this.generateTokens(user);
  }

  async refresh(dto: RefreshTokenDto) {
    const parsed = this.parseRefreshToken(dto.refreshToken);
    if (!parsed) {
      throw new UnauthorizedException('Invalid refresh token');
    }

    const refreshToken = await this.refreshTokenModel
      .findOne({
        _id: parsed.tokenId,
        expiresAt: { $gt: new Date() },
      })
      .populate('user');

    if (!refreshToken) {
      throw new UnauthorizedException('Invalid refresh token');
    }

    const valid = await argon2.verify(refreshToken.tokenHash, parsed.secret);
    if (!valid) {
      throw new UnauthorizedException('Invalid refresh token');
    }

    const user = refreshToken.user as UserDocument;
    this.assertUserNotSuspended(user);

    await this.refreshTokenModel.deleteOne({ _id: refreshToken._id });
    return this.generateTokens(user);
  }

  async logout(dto: RefreshTokenDto) {
    const parsed = this.parseRefreshToken(dto.refreshToken);
    if (!parsed) {
      return { success: true };
    }

    const refreshToken = await this.refreshTokenModel.findById(parsed.tokenId);
    if (!refreshToken) {
      return { success: true };
    }

    const valid = await argon2.verify(refreshToken.tokenHash, parsed.secret);
    if (!valid) {
      return { success: true };
    }

    await this.refreshTokenModel.deleteOne({ _id: refreshToken._id });
    return { success: true };
  }

  async forgotPassword(dto: ForgotPasswordDto) {
    const includeDebugToken = this.configService.get<string>('NODE_ENV') !== 'production';
    const response: { message: string; token?: string } = {
      message: 'If email exists, reset link sent',
    };

    const user = await this.userModel.findOne({ email: dto.email });
    if (!user) {
      if (includeDebugToken) {
        // Keep response shape identical in non-production to avoid account enumeration.
        response.token = randomBytes(32).toString('hex');
      }
      return response;
    }

    const token = randomBytes(32).toString('hex');
    await this.passwordResetModel.create({
      user: user._id,
      token: this.hashPasswordResetToken(token),
      expiresAt: new Date(Date.now() + 3600000),
    });

    if (includeDebugToken) {
      response.token = token;
    }

    return response;
  }

  async resetPassword(dto: ResetPasswordDto) {
    const tokenHash = this.hashPasswordResetToken(dto.token);
    const reset = await this.passwordResetModel.findOne({
      token: tokenHash,
      used: false,
      expiresAt: { $gt: new Date() },
    });

    if (!reset) throw new BadRequestException('Invalid or expired token');

    const passwordHash = await argon2.hash(dto.newPassword);
    await this.userModel.updateOne({ _id: reset.user }, { passwordHash });
    await this.passwordResetModel.updateOne({ _id: reset._id }, { used: true });
    await this.refreshTokenModel.deleteMany({ user: reset.user });

    return { message: 'Password reset successful' };
  }

  async validateUser(userId: string) {
    return this.userModel.findById(userId);
  }

  async listUsersForAdmin(params: {
    search?: string;
    limit?: number;
  }): Promise<{
    items: Array<{
      id: string;
      email: string | null;
      role: string;
      registrationType: string;
      subscriptionTier: 'free' | 'premium' | 'enterprise';
      isSuspended: boolean;
      twoFactorEnabled: boolean;
      tokenVersion: number;
      createdAt: string | null;
      updatedAt: string | null;
      lastLoginAt: string | null;
      suspendedAt: string | null;
    }>;
  }> {
    const normalizedLimit = Number.isFinite(params.limit)
      ? Math.min(Math.max(Math.floor(params.limit || 20), 1), 5000)
      : 20;
    const normalizedSearch =
      typeof params.search === 'string' ? params.search.trim() : '';

    const filter: Record<string, unknown> = {};
    if (normalizedSearch.length > 0) {
      const orFilters: Record<string, unknown>[] = [
        { email: { $regex: normalizedSearch, $options: 'i' } },
      ];

      if (Types.ObjectId.isValid(normalizedSearch)) {
        orFilters.push({ _id: new Types.ObjectId(normalizedSearch) });
      }

      filter.$or = orFilters;
    }

    const users = await this.userModel
      .find(filter)
      .sort({ updatedAt: -1, createdAt: -1 })
      .limit(normalizedLimit)
      .select(
        '_id email role registrationType subscriptionTier isSuspended twoFactorEnabled tokenVersion createdAt updatedAt lastLoginAt suspendedAt',
      )
      .lean()
      .exec();

    const toIsoDateOrNull = (value: unknown): string | null => {
      if (value instanceof Date && !Number.isNaN(value.getTime())) {
        return value.toISOString();
      }
      if (typeof value === 'string') {
        const parsed = new Date(value);
        return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
      }
      return null;
    };

    return {
      items: users.map((user) => {
        const record = user as unknown as {
          _id: unknown;
          email?: unknown;
          role?: unknown;
          registrationType?: unknown;
          subscriptionTier?: unknown;
          isSuspended?: unknown;
          twoFactorEnabled?: unknown;
          tokenVersion?: unknown;
          createdAt?: unknown;
          updatedAt?: unknown;
          lastLoginAt?: unknown;
          suspendedAt?: unknown;
        };

        return {
          id: String(record._id),
          email: typeof record.email === 'string' ? record.email : null,
          role:
            typeof record.role === 'string' && record.role.trim().length > 0
              ? record.role
              : 'user',
          registrationType:
            typeof record.registrationType === 'string' && record.registrationType.trim().length > 0
              ? record.registrationType
              : 'wallet',
          subscriptionTier: this.accessControlService.normalizeTier(record.subscriptionTier),
          isSuspended: record.isSuspended === true,
          twoFactorEnabled: record.twoFactorEnabled === true,
          tokenVersion:
            typeof record.tokenVersion === 'number' && Number.isFinite(record.tokenVersion)
              ? Math.max(0, Math.floor(record.tokenVersion))
              : 0,
          createdAt: toIsoDateOrNull(record.createdAt),
          updatedAt: toIsoDateOrNull(record.updatedAt),
          lastLoginAt: toIsoDateOrNull(record.lastLoginAt),
          suspendedAt: toIsoDateOrNull(record.suspendedAt),
        };
      }),
    };
  }

  async updateUserRole(params: {
    targetUserId: string;
    role: 'user' | 'admin' | 'owner';
    actorRole: 'user' | 'admin' | 'owner';
  }): Promise<{
    userId: string;
    role: 'user' | 'admin' | 'owner';
  }> {
    const nextRole = this.normalizeRole(params.role);
    const currentUser = await this.userModel
      .findById(params.targetUserId)
      .select('_id role')
      .lean()
      .exec();
    if (!currentUser) {
      throw new NotFoundException('User not found');
    }

    const currentRole = this.normalizeRole((currentUser as { role?: unknown }).role);
    this.assertRoleMutationAllowed(params.actorRole, currentRole, nextRole);

    const updatedUser = await this.userModel
      .findByIdAndUpdate(
        params.targetUserId,
        { $set: { role: nextRole } },
        { new: true },
      )
      .select('_id role')
      .lean()
      .exec();

    if (!updatedUser) {
      throw new NotFoundException('User not found');
    }

    return {
      userId: String(updatedUser._id),
      role: this.normalizeRole(updatedUser.role),
    };
  }

  async updateUserRoleByEmail(params: {
    email: string;
    role: 'user' | 'admin' | 'owner';
    actorRole: 'user' | 'admin' | 'owner';
  }): Promise<{
    userId: string;
    email: string;
    role: 'user' | 'admin' | 'owner';
  }> {
    const normalizedEmail = params.email.trim().toLowerCase();
    if (!normalizedEmail) {
      throw new NotFoundException('User not found');
    }

    const nextRole = this.normalizeRole(params.role);
    const currentUser = await this.userModel
      .findOne({ email: normalizedEmail })
      .select('_id role')
      .lean()
      .exec();
    if (!currentUser) {
      throw new NotFoundException('User not found');
    }

    const currentRole = this.normalizeRole((currentUser as { role?: unknown }).role);
    this.assertRoleMutationAllowed(params.actorRole, currentRole, nextRole);

    const updatedUser = await this.userModel
      .findOneAndUpdate(
        { email: normalizedEmail },
        { $set: { role: nextRole } },
        { new: true },
      )
      .select('_id email role')
      .lean()
      .exec();

    if (!updatedUser) {
      throw new NotFoundException('User not found');
    }

    return {
      userId: String(updatedUser._id),
      email: typeof updatedUser.email === 'string' ? updatedUser.email : normalizedEmail,
      role: this.normalizeRole(updatedUser.role),
    };
  }

  async getAdminUserSnapshot(userId: string): Promise<{
    userId: string;
    email: string | null;
    role: 'user' | 'admin' | 'owner';
    isSuspended: boolean;
  }> {
    const user = await this.userModel
      .findById(userId)
      .select('_id email role isSuspended')
      .lean()
      .exec();
    if (!user) {
      throw new NotFoundException('User not found');
    }

    return {
      userId: String((user as { _id: unknown })._id),
      email: typeof user.email === 'string' ? user.email : null,
      role: this.normalizeRole((user as { role?: unknown }).role),
      isSuspended: (user as { isSuspended?: unknown }).isSuspended === true,
    };
  }

  async getAdminUserSnapshotByEmail(email: string): Promise<{
    userId: string;
    email: string | null;
    role: 'user' | 'admin' | 'owner';
    isSuspended: boolean;
  }> {
    const normalizedEmail = email.trim().toLowerCase();
    if (!normalizedEmail) {
      throw new NotFoundException('User not found');
    }
    const user = await this.userModel
      .findOne({ email: normalizedEmail })
      .select('_id email role isSuspended')
      .lean()
      .exec();
    if (!user) {
      throw new NotFoundException('User not found');
    }
    return {
      userId: String((user as { _id: unknown })._id),
      email: typeof user.email === 'string' ? user.email : normalizedEmail,
      role: this.normalizeRole((user as { role?: unknown }).role),
      isSuspended: (user as { isSuspended?: unknown }).isSuspended === true,
    };
  }

  async setUserSuspension(params: {
    targetUserId: string;
    suspended: boolean;
    reason?: string;
    updatedByUserId: string;
    forceLogout?: boolean;
  }): Promise<{
    userId: string;
    email: string | null;
    isSuspended: boolean;
    suspendedAt: string | null;
    suspendedReason: string | null;
    tokenVersion: number;
    revokedRefreshTokens: number;
  }> {
    const now = new Date();
    const shouldRevoke = params.suspended || params.forceLogout === true;
    const update: Record<string, unknown> = {
      $set: {
        isSuspended: params.suspended,
      },
    };

    if (params.suspended) {
      const trimmedReason = typeof params.reason === 'string' ? params.reason.trim() : '';
      (update.$set as Record<string, unknown>).suspendedAt = now;
      (update.$set as Record<string, unknown>).suspendedBy = params.updatedByUserId;
      (update.$set as Record<string, unknown>).suspendedReason =
        trimmedReason.length > 0 ? trimmedReason : 'Suspended by admin';
    } else {
      (update.$set as Record<string, unknown>).suspendedAt = null;
      (update.$set as Record<string, unknown>).suspendedBy = null;
      (update.$set as Record<string, unknown>).suspendedReason = null;
    }

    if (shouldRevoke) {
      update.$inc = { tokenVersion: 1 };
      (update.$set as Record<string, unknown>).sessionsRevokedAt = now;
    }

    const updatedUser = await this.userModel
      .findByIdAndUpdate(params.targetUserId, update as any, { new: true })
      .select('_id email isSuspended suspendedAt suspendedReason tokenVersion')
      .lean()
      .exec();

    if (!updatedUser) {
      throw new NotFoundException('User not found');
    }

    let revokedRefreshTokens = 0;
    if (shouldRevoke) {
      revokedRefreshTokens = await this.revokeSessionsInternal(params.targetUserId);
    }

    return {
      userId: String(updatedUser._id),
      email: typeof updatedUser.email === 'string' ? updatedUser.email : null,
      isSuspended: updatedUser.isSuspended === true,
      suspendedAt: this.toIsoDateOrNull(updatedUser.suspendedAt),
      suspendedReason:
        typeof updatedUser.suspendedReason === 'string' && updatedUser.suspendedReason.trim().length > 0
          ? updatedUser.suspendedReason
          : null,
      tokenVersion:
        typeof updatedUser.tokenVersion === 'number' && Number.isFinite(updatedUser.tokenVersion)
          ? Math.max(0, Math.floor(updatedUser.tokenVersion))
          : 0,
      revokedRefreshTokens,
    };
  }

  async revokeUserSessions(params: {
    targetUserId: string;
  }): Promise<{
    userId: string;
    tokenVersion: number;
    sessionsRevokedAt: string;
    revokedRefreshTokens: number;
  }> {
    const now = new Date();
    const updatedUser = await this.userModel
      .findByIdAndUpdate(
        params.targetUserId,
        {
          $inc: { tokenVersion: 1 },
          $set: { sessionsRevokedAt: now },
        },
        { new: true },
      )
      .select('_id tokenVersion sessionsRevokedAt')
      .lean()
      .exec();

    if (!updatedUser) {
      throw new NotFoundException('User not found');
    }

    const revokedRefreshTokens = await this.revokeSessionsInternal(params.targetUserId);

    return {
      userId: String(updatedUser._id),
      tokenVersion:
        typeof updatedUser.tokenVersion === 'number' && Number.isFinite(updatedUser.tokenVersion)
          ? Math.max(0, Math.floor(updatedUser.tokenVersion))
          : 0,
      sessionsRevokedAt: (this.toIsoDateOrNull(updatedUser.sessionsRevokedAt) || now.toISOString()),
      revokedRefreshTokens,
    };
  }

  async triggerAdminPasswordReset(params: {
    targetUserId: string;
    requestedByUserId: string;
    invalidateSessions?: boolean;
  }): Promise<{
    userId: string;
    email: string | null;
    expiresAt: string;
    invalidatedSessions: boolean;
    revokedRefreshTokens: number;
    token?: string;
  }> {
    const user = await this.userModel
      .findById(params.targetUserId)
      .select('_id email')
      .lean()
      .exec();
    if (!user) {
      throw new NotFoundException('User not found');
    }

    const token = randomBytes(32).toString('hex');
    const expiresAt = new Date(Date.now() + 60 * 60 * 1000);
    await this.passwordResetModel.create({
      user: user._id,
      token: this.hashPasswordResetToken(token),
      expiresAt,
      used: false,
    });

    let revokedRefreshTokens = 0;
    const invalidateSessions = params.invalidateSessions !== false;
    if (invalidateSessions) {
      const now = new Date();
      await this.userModel
        .updateOne(
          { _id: params.targetUserId } as any,
          {
            $inc: { tokenVersion: 1 },
            $set: {
              sessionsRevokedAt: now,
            },
          },
        )
        .exec();
      revokedRefreshTokens = await this.revokeSessionsInternal(params.targetUserId);
    }

    const includeDebugToken = this.configService.get<string>('NODE_ENV') !== 'production';

    return {
      userId: String(user._id),
      email: typeof user.email === 'string' ? user.email : null,
      expiresAt: expiresAt.toISOString(),
      invalidatedSessions: invalidateSessions,
      revokedRefreshTokens,
      ...(includeDebugToken ? { token } : {}),
    };
  }

  async resetUserTwoFactor(params: {
    targetUserId: string;
    resetByUserId: string;
    revokeSessions?: boolean;
  }): Promise<{
    userId: string;
    twoFactorEnabled: boolean;
    twoFactorResetAt: string;
    revokedRefreshTokens: number;
  }> {
    const now = new Date();
    const shouldRevokeSessions = params.revokeSessions !== false;

    const update: Record<string, unknown> = {
      $set: {
        twoFactorEnabled: false,
        twoFactorSecret: null,
        twoFactorResetAt: now,
        twoFactorResetBy: params.resetByUserId,
      },
    };
    if (shouldRevokeSessions) {
      update.$inc = { tokenVersion: 1 };
      (update.$set as Record<string, unknown>).sessionsRevokedAt = now;
    }

    const updatedUser = await this.userModel
      .findByIdAndUpdate(params.targetUserId, update as any, { new: true })
      .select('_id twoFactorEnabled twoFactorResetAt')
      .lean()
      .exec();

    if (!updatedUser) {
      throw new NotFoundException('User not found');
    }

    let revokedRefreshTokens = 0;
    if (shouldRevokeSessions) {
      revokedRefreshTokens = await this.revokeSessionsInternal(params.targetUserId);
    }

    return {
      userId: String(updatedUser._id),
      twoFactorEnabled: updatedUser.twoFactorEnabled === true,
      twoFactorResetAt: this.toIsoDateOrNull(updatedUser.twoFactorResetAt) || now.toISOString(),
      revokedRefreshTokens,
    };
  }

  async getAdminUserDetails(params: {
    targetUserId: string;
    txLimit?: number;
  }): Promise<{
    user: Record<string, unknown>;
    security: Record<string, unknown>;
    wallets: Array<Record<string, unknown>>;
    connectedWallets: Array<Record<string, unknown>>;
    recentTransactions: Array<Record<string, unknown>>;
    recentAdminEvents: Array<Record<string, unknown>>;
  }> {
    const txLimit = Number.isFinite(params.txLimit)
      ? Math.min(Math.max(Math.floor(params.txLimit || 30), 1), 100)
      : 30;

    const user = await this.userModel
      .findById(params.targetUserId)
      .select(
        '_id email role registrationType subscriptionTier isSuspended suspendedAt suspendedReason suspendedBy lastLoginAt createdAt updatedAt tokenVersion sessionsRevokedAt twoFactorEnabled twoFactorResetAt twoFactorResetBy',
      )
      .lean()
      .exec();
    if (!user) {
      throw new NotFoundException('User not found');
    }

    const targetUserId = String((user as { _id: unknown })._id);
    const targetEmail = typeof user.email === 'string' ? user.email : null;

    const [wallets, connectedWallets, activeRefreshTokens, lastResetRequest, resetRequestsLast30d] =
      await Promise.all([
        this.walletModel
          .find({ userId: targetUserId, isArchived: false } as any)
          .select('_id chain address label isWatchOnly balances createdAt updatedAt')
          .lean()
          .exec(),
        this.connectedWalletModel
          .find({ userId: targetUserId } as any)
          .select('_id publicKey walletType isPrimary label lastUsedAt createdAt updatedAt')
          .lean()
          .exec(),
        this.refreshTokenModel
          .countDocuments({
            user: targetUserId,
            expiresAt: { $gt: new Date() },
          } as any)
          .exec(),
        this.passwordResetModel
          .findOne({ user: targetUserId } as any)
          .sort({ createdAt: -1 })
          .select('createdAt expiresAt used')
          .lean()
          .exec(),
        this.passwordResetModel
          .countDocuments({
            user: targetUserId,
            createdAt: { $gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) },
          } as any)
          .exec(),
      ]);

    const walletIds = wallets.map((wallet) => String((wallet as { _id: unknown })._id));
    const recentTransactions =
      walletIds.length > 0
        ? await this.transactionModel
            .find({ walletId: { $in: walletIds } } as any)
            .sort({ timestamp: -1, createdAt: -1 })
            .limit(txLimit)
            .select('_id walletId hash chain type from to amount asset fee status blockNumber timestamp createdAt')
            .lean()
            .exec()
        : [];

    const recentAdminEvents = (await this.adminAuditEventModel
      .find(
        {
          $or: [
            { actorUserId: targetUserId },
            { target: targetUserId },
            ...(targetEmail ? [{ target: targetEmail }] : []),
          ],
        } as any,
      )
      .sort({ createdAt: -1 })
      .limit(40)
      .lean()
      .exec()) as Array<Record<string, unknown>>;

    const walletMap = new Map(
      wallets.map((wallet) => [String((wallet as { _id: unknown })._id), wallet]),
    );

    return {
      user: {
        id: targetUserId,
        email: targetEmail,
        role: this.normalizeRole((user as { role?: unknown }).role),
        registrationType:
          typeof user.registrationType === 'string' && user.registrationType.trim().length > 0
            ? user.registrationType
            : 'wallet',
        subscriptionTier: this.accessControlService.normalizeTier(user.subscriptionTier),
        isSuspended: user.isSuspended === true,
        suspendedAt: this.toIsoDateOrNull(user.suspendedAt),
        suspendedReason:
          typeof user.suspendedReason === 'string' && user.suspendedReason.trim().length > 0
            ? user.suspendedReason
            : null,
        suspendedBy:
          typeof user.suspendedBy === 'string' && user.suspendedBy.trim().length > 0
            ? user.suspendedBy
            : null,
        createdAt: this.toIsoDateOrNull((user as { createdAt?: unknown }).createdAt),
        updatedAt: this.toIsoDateOrNull((user as { updatedAt?: unknown }).updatedAt),
        lastLoginAt: this.toIsoDateOrNull((user as { lastLoginAt?: unknown }).lastLoginAt),
      },
      security: {
        activeRefreshTokens: Number(activeRefreshTokens || 0),
        tokenVersion:
          typeof user.tokenVersion === 'number' && Number.isFinite(user.tokenVersion)
            ? Math.max(0, Math.floor(user.tokenVersion))
            : 0,
        sessionsRevokedAt: this.toIsoDateOrNull(user.sessionsRevokedAt),
        twoFactorEnabled: user.twoFactorEnabled === true,
        twoFactorResetAt: this.toIsoDateOrNull(user.twoFactorResetAt),
        twoFactorResetBy:
          typeof user.twoFactorResetBy === 'string' && user.twoFactorResetBy.trim().length > 0
            ? user.twoFactorResetBy
            : null,
        passwordResetRequestsLast30d: Number(resetRequestsLast30d || 0),
        lastPasswordResetRequestedAt: this.toIsoDateOrNull(
          (lastResetRequest as { createdAt?: unknown } | null)?.createdAt,
        ),
        lastPasswordResetExpiresAt: this.toIsoDateOrNull(
          (lastResetRequest as { expiresAt?: unknown } | null)?.expiresAt,
        ),
        lastPasswordResetUsed:
          (lastResetRequest as { used?: unknown } | null)?.used === true,
      },
      wallets: wallets.map((wallet) => ({
        id: String((wallet as { _id: unknown })._id),
        chain: typeof wallet.chain === 'string' ? wallet.chain : 'stellar',
        address: typeof wallet.address === 'string' ? wallet.address : '',
        label: typeof wallet.label === 'string' ? wallet.label : '',
        isWatchOnly: wallet.isWatchOnly === true,
        balances: Array.isArray(wallet.balances) ? wallet.balances : [],
        createdAt: this.toIsoDateOrNull((wallet as { createdAt?: unknown }).createdAt),
        updatedAt: this.toIsoDateOrNull((wallet as { updatedAt?: unknown }).updatedAt),
      })),
      connectedWallets: connectedWallets.map((wallet) => ({
        id: String((wallet as { _id: unknown })._id),
        publicKey: typeof wallet.publicKey === 'string' ? wallet.publicKey : '',
        walletType: typeof wallet.walletType === 'string' ? wallet.walletType : 'unknown',
        label: typeof wallet.label === 'string' ? wallet.label : '',
        isPrimary: wallet.isPrimary === true,
        lastUsedAt: this.toIsoDateOrNull(wallet.lastUsedAt),
        createdAt: this.toIsoDateOrNull((wallet as { createdAt?: unknown }).createdAt),
      })),
      recentTransactions: recentTransactions.map((tx) => {
        const walletId = String((tx as { walletId?: unknown }).walletId || '');
        const wallet = walletMap.get(walletId) as { chain?: unknown; address?: unknown } | undefined;
        return {
          id: String((tx as { _id: unknown })._id),
          walletId,
          walletAddress:
            wallet && typeof wallet.address === 'string' ? wallet.address : null,
          chain:
            typeof tx.chain === 'string'
              ? tx.chain
              : wallet && typeof wallet.chain === 'string'
                ? wallet.chain
                : 'stellar',
          hash: typeof tx.hash === 'string' ? tx.hash : '',
          type: typeof tx.type === 'string' ? tx.type : 'transaction',
          from: typeof tx.from === 'string' ? tx.from : null,
          to: typeof tx.to === 'string' ? tx.to : null,
          amount: typeof tx.amount === 'string' ? tx.amount : null,
          asset: typeof tx.asset === 'string' ? tx.asset : null,
          fee: typeof tx.fee === 'string' ? tx.fee : null,
          status: typeof tx.status === 'string' ? tx.status : 'unknown',
          blockNumber:
            typeof tx.blockNumber === 'number' && Number.isFinite(tx.blockNumber)
              ? tx.blockNumber
              : null,
          timestamp: this.toIsoDateOrNull(tx.timestamp),
          createdAt: this.toIsoDateOrNull((tx as { createdAt?: unknown }).createdAt),
        };
      }),
      recentAdminEvents: recentAdminEvents.map((event) => ({
        id: String((event as { _id: unknown })._id),
        action: typeof event.action === 'string' ? event.action : 'admin_action',
        status: typeof event.status === 'string' ? event.status : 'success',
        target: typeof event.target === 'string' ? event.target : 'unknown',
        details: typeof event.details === 'string' ? event.details : '',
        actorUserId: typeof event.actorUserId === 'string' ? event.actorUserId : '',
        actorEmail: typeof event.actorEmail === 'string' ? event.actorEmail : null,
        createdAt: this.toIsoDateOrNull(event.createdAt),
      })),
    };
  }

  private async generateTokens(user: UserDocument) {
    const userId = (user as any)._id.toString();
    this.assertUserNotSuspended(user);
    const tokenVersion =
      typeof (user as unknown as { tokenVersion?: unknown }).tokenVersion === 'number' &&
      Number.isFinite((user as unknown as { tokenVersion?: unknown }).tokenVersion)
        ? Math.max(0, Math.floor((user as unknown as { tokenVersion: number }).tokenVersion))
        : 0;
    const subscriptionTier = this.accessControlService.normalizeTier(
      (user as unknown as { subscriptionTier?: unknown }).subscriptionTier,
    );
    const subscription = this.accessControlService.buildSubscriptionInfoFromUser({
      subscriptionTier,
      apiUsage: (user as unknown as { apiUsage?: unknown }).apiUsage,
    });
    const wallets = await this.getConnectedWalletSummaries(userId);
    const payload = {
      sub: userId,
      email: user.email,
      role: user.role,
      subscriptionTier,
      tokenVersion,
    };

    const accessToken = this.jwtService.sign(payload);
    const refreshToken = await this.issueRefreshToken(userId);

    await this.userModel.updateOne(
      { _id: userId },
      { lastLoginAt: new Date() },
    );

    return {
      accessToken,
      refreshToken,
      user: {
        id: userId,
        email: user.email,
        role: user.role,
        subscription,
        wallets,
        isSuspended: (user as unknown as { isSuspended?: unknown }).isSuspended === true,
      },
    };
  }

  private async issueRefreshToken(userId: string): Promise<string> {
    const secret = randomBytes(64).toString('hex');
    const tokenHash = await argon2.hash(secret);

    const tokenRecord = await this.refreshTokenModel.create({
      user: userId,
      tokenHash,
      expiresAt: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
    });

    return `${(tokenRecord as any)._id.toString()}.${secret}`;
  }

  private parseRefreshToken(token: string): { tokenId: string; secret: string } | null {
    const [tokenId, secret] = token.split('.');
    if (!tokenId || !secret) return null;
    if (!Types.ObjectId.isValid(tokenId)) return null;
    if (secret.length < 32) return null;
    return { tokenId, secret };
  }

  private hashPasswordResetToken(token: string): string {
    return createHash('sha256').update(token).digest('hex');
  }

  private async verifyGoogleIdToken(idToken: string): Promise<{ email: string }> {
    const googleClientId = this.configService.get<string>('GOOGLE_CLIENT_ID');
    if (!googleClientId) {
      throw new BadRequestException('Google login is not configured');
    }

    try {
      const { data } = await axios.get<GoogleTokenInfoResponse>(
        'https://oauth2.googleapis.com/tokeninfo',
        {
          params: { id_token: idToken },
          timeout: 10000,
        },
      );

      if (data.aud !== googleClientId) {
        throw new UnauthorizedException('Google token audience mismatch');
      }

      const emailVerified =
        data.email_verified === true || data.email_verified === 'true';

      if (!data.email || !emailVerified) {
        throw new UnauthorizedException('Google email is not verified');
      }

      if (data.exp && Number(data.exp) * 1000 <= Date.now()) {
        throw new UnauthorizedException('Google token has expired');
      }

      return { email: data.email.toLowerCase() };
    } catch (error) {
      if (
        error instanceof UnauthorizedException ||
        error instanceof BadRequestException
      ) {
        throw error;
      }

      throw new UnauthorizedException('Invalid Google credential');
    }
  }

  private async revokeSessionsInternal(userId: string): Promise<number> {
    const result = await this.refreshTokenModel.deleteMany({ user: userId } as any).exec();
    return Number(result.deletedCount || 0);
  }

  private assertUserNotSuspended(
    user: Partial<{
      isSuspended?: unknown;
      email?: unknown;
    }>,
  ): void {
    if (user.isSuspended !== true) {
      return;
    }

    const email =
      typeof user.email === 'string' && user.email.trim().length > 0
        ? user.email.trim()
        : null;
    throw new UnauthorizedException(
      email ? `Account suspended: ${email}` : 'Account is suspended',
    );
  }

  private assertRoleMutationAllowed(
    actorRole: 'user' | 'admin' | 'owner',
    currentRole: 'user' | 'admin' | 'owner',
    nextRole: 'user' | 'admin' | 'owner',
  ): void {
    if (actorRole === 'owner') {
      return;
    }
    if (currentRole === 'owner') {
      throw new ForbiddenException('Only owner can modify owner accounts');
    }
    if (nextRole === 'owner') {
      throw new ForbiddenException('Only owner can assign owner role');
    }
  }

  private toIsoDateOrNull(value: unknown): string | null {
    if (value instanceof Date && !Number.isNaN(value.getTime())) {
      return value.toISOString();
    }
    if (typeof value === 'string') {
      const parsed = new Date(value);
      return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
    }
    return null;
  }

  private normalizeRole(role: unknown): 'user' | 'admin' | 'owner' {
    const normalized = typeof role === 'string' ? role.trim().toLowerCase() : '';
    if (normalized === 'admin' || normalized === 'owner') {
      return normalized;
    }
    return 'user';
  }

  private resolveWalletType(publicKey: string): string {
    return publicKey.startsWith('0x') ? 'evm' : 'stellar';
  }

  private resolveConnectedWalletChain(
    walletTypeRaw: unknown,
    publicKeyRaw: unknown,
  ): 'stellar' | 'evm' {
    const walletType = typeof walletTypeRaw === 'string' ? walletTypeRaw.trim().toLowerCase() : '';
    if (
      walletType === 'evm' ||
      walletType === 'metamask' ||
      walletType === 'coinbase' ||
      walletType === 'trust' ||
      walletType === 'phantom' ||
      walletType === 'walletconnect' ||
      walletType === 'ethereum' ||
      walletType === 'sepolia' ||
      walletType === 'polygon' ||
      walletType === 'arbitrum' ||
      walletType === 'base' ||
      walletType === 'axelar'
    ) {
      return 'evm';
    }

    const publicKey = typeof publicKeyRaw === 'string' ? publicKeyRaw.trim().toLowerCase() : '';
    return publicKey.startsWith('0x') ? 'evm' : 'stellar';
  }
}
