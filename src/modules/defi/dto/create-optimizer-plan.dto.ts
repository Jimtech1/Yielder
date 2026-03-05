import { Type } from 'class-transformer';
import {
  IsBoolean,
  IsIn,
  IsInt,
  IsNotEmpty,
  IsNumber,
  IsOptional,
  IsString,
  Matches,
  Max,
  Min,
} from 'class-validator';

export class CreateOptimizerPlanDto {
  @IsOptional()
  @IsString()
  @IsNotEmpty()
  strategyName?: string;

  @IsString()
  @IsNotEmpty()
  assetSymbol: string;

  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'amount must be a numeric string' })
  amount: string;

  @IsOptional()
  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'sourceBalance must be a numeric string' })
  sourceBalance?: string;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  sourceChainKey?: string;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  sourceAddress?: string;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  destinationAddress?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  @Max(5000)
  performanceFeeBps?: number;

  @IsOptional()
  @IsString()
  @IsIn(['lending', 'liquidity', 'staking'])
  category?: 'lending' | 'liquidity' | 'staking';

  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  inAppOnly?: boolean;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  preferredProtocol?: string;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  preferredChain?: string;

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(0)
  @Max(1000)
  preferredApy?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(0)
  preferredTvlUsd?: number;

  @IsOptional()
  @Type(() => Number)
  @IsNumber()
  @Min(0)
  @Max(100)
  preferredRiskScore?: number;
}
