import { Type } from 'class-transformer';
import { IsBoolean, IsNotEmpty, IsOptional, IsString, Matches } from 'class-validator';
import { CreateOptimizerPlanDto } from './create-optimizer-plan.dto';

export class ExecuteOptimizerDto extends CreateOptimizerPlanDto {
  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  autoExecuteBridge?: boolean;

  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  executeBridgeOnBackend?: boolean;

  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  openPositionImmediately?: boolean;

  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  allowExternalBridgeRedirect?: boolean;

  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  autoSettleFee?: boolean;

  @IsOptional()
  @Matches(/^\d+(\.\d+)?$/, { message: 'realizedProfitAmount must be a numeric string' })
  realizedProfitAmount?: string;

  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  settleFeeOnBackend?: boolean;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  depositTxHash?: string;
}
