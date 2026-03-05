import { Type } from 'class-transformer';
import { IsBoolean, IsNotEmpty, IsOptional, IsString, Matches } from 'class-validator';

export class ExecuteBorrowDto {
  @IsString()
  @IsNotEmpty()
  protocol: string;

  @IsString()
  @IsNotEmpty()
  assetSymbol: string;

  @Matches(/^\d+(\.\d+)?$/, { message: 'amount must be a numeric string' })
  amount: string;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  chainKey?: string;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  walletAddress?: string;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  protocolUrl?: string;

  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  allowExternalRedirect?: boolean;
}
