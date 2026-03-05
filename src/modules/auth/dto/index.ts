import { IsEmail, IsIn, IsMongoId, IsOptional, IsString, MinLength } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class RegisterDto {
  @ApiProperty()
  @IsEmail()
  email: string;

  @ApiProperty()
  @IsString()
  @MinLength(8)
  password: string;

  @ApiProperty({ required: false, description: 'Cloudflare Turnstile token' })
  @IsOptional()
  @IsString()
  turnstileToken?: string;
}

export class LoginDto {
  @ApiProperty()
  @IsEmail()
  email: string;

  @ApiProperty()
  @IsString()
  password: string;

  @ApiProperty({ required: false, description: 'Cloudflare Turnstile token' })
  @IsOptional()
  @IsString()
  turnstileToken?: string;
}

export class GoogleLoginDto {
  @ApiProperty({ description: 'Google ID token credential' })
  @IsString()
  idToken: string;

  @ApiProperty({ required: false, description: 'Cloudflare Turnstile token' })
  @IsOptional()
  @IsString()
  turnstileToken?: string;
}

export class RefreshTokenDto {
  @ApiProperty()
  @IsString()
  refreshToken: string;
}

export class ForgotPasswordDto {
  @ApiProperty()
  @IsEmail()
  email: string;
}

export class ResetPasswordDto {
  @ApiProperty()
  @IsString()
  token: string;

  @ApiProperty()
  @IsString()
  @MinLength(8)
  newPassword: string;
}

export class UpdateSubscriptionTierDto {
  @ApiProperty()
  @IsMongoId()
  userId: string;

  @ApiProperty({ enum: ['free', 'premium', 'enterprise'] })
  @IsString()
  @IsIn(['free', 'premium', 'enterprise'])
  tier: 'free' | 'premium' | 'enterprise';
}

export class UpdateSubscriptionTierByEmailDto {
  @ApiProperty()
  @IsEmail()
  email: string;

  @ApiProperty({ enum: ['free', 'premium', 'enterprise'] })
  @IsString()
  @IsIn(['free', 'premium', 'enterprise'])
  tier: 'free' | 'premium' | 'enterprise';
}

export class UpdateUserRoleDto {
  @ApiProperty()
  @IsMongoId()
  userId: string;

  @ApiProperty({ enum: ['user', 'admin', 'owner'] })
  @IsString()
  @IsIn(['user', 'admin', 'owner'])
  role: 'user' | 'admin' | 'owner';
}

export class UpdateUserRoleByEmailDto {
  @ApiProperty()
  @IsEmail()
  email: string;

  @ApiProperty({ enum: ['user', 'admin', 'owner'] })
  @IsString()
  @IsIn(['user', 'admin', 'owner'])
  role: 'user' | 'admin' | 'owner';
}
