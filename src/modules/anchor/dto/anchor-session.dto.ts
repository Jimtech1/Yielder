import { IsEnum, IsNotEmpty, IsOptional, IsString, Matches } from 'class-validator';

export enum AnchorFlow {
  DEPOSIT = 'deposit',
  WITHDRAW = 'withdraw',
}

export class AnchorSessionDto {
  @IsEnum(AnchorFlow)
  flow: AnchorFlow;

  @IsString()
  @IsNotEmpty()
  assetCode: string;

  @IsString()
  @IsNotEmpty()
  account: string;

  @IsOptional()
  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'amount must be a numeric string' })
  amount?: string;

  @IsOptional()
  @IsString()
  quoteId?: string;

  @IsOptional()
  @IsString()
  lang?: string;

  @IsOptional()
  @IsString()
  countryCode?: string;

  @IsOptional()
  @IsString()
  authToken?: string;
}
