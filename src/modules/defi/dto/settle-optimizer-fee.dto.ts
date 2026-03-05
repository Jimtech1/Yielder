import { Type } from 'class-transformer';
import {
  IsBoolean,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  Matches,
  Max,
  Min,
} from 'class-validator';

export class SettleOptimizerFeeDto {
  @IsString()
  @IsNotEmpty()
  optimizerExecutionId: string;

  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'realizedProfitAmount must be a numeric string' })
  realizedProfitAmount: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  @Max(5000)
  performanceFeeBps?: number;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  assetSymbol?: string;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  chainKey?: string;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  payerAddress?: string;

  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  settleOnBackend?: boolean;
}
