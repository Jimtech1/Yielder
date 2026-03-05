import { IsNotEmpty, IsOptional, IsString } from 'class-validator';

export class BuildOptimizerStellarDepositTxDto {
  @IsOptional()
  @IsString()
  @IsNotEmpty()
  sourcePublicKey?: string;

  @IsOptional()
  @IsString()
  memo?: string;
}
