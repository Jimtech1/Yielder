import { Type } from 'class-transformer';
import { IsBoolean, IsNotEmpty, IsOptional, IsString } from 'class-validator';

export class CompleteOptimizerDepositDto {
  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  autoExecute?: boolean;

  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  allowExternalRedirect?: boolean;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  depositTxHash?: string;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  bridgeTxHash?: string;
}
