import { IsArray, IsIn, IsNotEmpty, IsOptional, IsString, Matches } from 'class-validator';

export class BuildPathPaymentDto {
  @IsString()
  @IsNotEmpty()
  sourcePublicKey: string;

  @IsString()
  @IsNotEmpty()
  destinationPublicKey: string;

  @IsString()
  @IsNotEmpty()
  sourceAsset: string;

  @IsString()
  @IsNotEmpty()
  destinationAsset: string;

  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'sourceAmount must be a numeric string' })
  sourceAmount: string;

  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'destMin must be a numeric string' })
  destMin: string;

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  path?: string[];

  @IsOptional()
  @IsString()
  memo?: string;

  @IsOptional()
  @IsString()
  @IsIn(['testnet', 'mainnet', 'public'])
  network?: string;
}
