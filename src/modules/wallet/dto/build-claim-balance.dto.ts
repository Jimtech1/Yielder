import { Type } from 'class-transformer';
import { IsBoolean, IsInt, IsNotEmpty, IsOptional, IsString, Matches, Max, Min } from 'class-validator';

export class BuildClaimBalanceDto {
  @IsString()
  @IsNotEmpty()
  sourcePublicKey: string;

  @IsString()
  @IsNotEmpty()
  balanceId: string;

  @IsOptional()
  @IsString()
  memo?: string;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  optimizerExecutionId?: string;

  @IsOptional()
  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'realizedProfitAmount must be a numeric string' })
  realizedProfitAmount?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  @Max(5000)
  performanceFeeBps?: number;

  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  settleFeeOnBackend?: boolean;

  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  autoSettleOptimizerFee?: boolean;
}
