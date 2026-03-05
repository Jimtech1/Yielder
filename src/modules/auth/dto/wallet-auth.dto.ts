import { IsString, IsNotEmpty, IsOptional } from 'class-validator';

export class WalletChallengeDto {
  @IsString()
  @IsNotEmpty()
  publicKey: string;
}

export class WalletLoginDto {
  @IsString()
  @IsNotEmpty()
  publicKey: string;

  @IsString()
  @IsNotEmpty()
  signature: string;

  @IsOptional()
  @IsString()
  challenge?: string;

  @IsOptional()
  @IsString()
  turnstileToken?: string;
}
