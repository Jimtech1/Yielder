import { Type } from 'class-transformer';
import {
  IsIn,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  Matches,
  Max,
  Min,
} from 'class-validator';

export class PathPaymentQuoteDto {
  @IsString()
  @IsNotEmpty()
  sourceAsset: string;

  @IsString()
  @IsNotEmpty()
  destinationAsset: string;

  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'sourceAmount must be a numeric string' })
  sourceAmount: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(25)
  limit?: number;

  @IsOptional()
  @IsString()
  @IsIn(['testnet', 'mainnet', 'public'])
  network?: string;
}
