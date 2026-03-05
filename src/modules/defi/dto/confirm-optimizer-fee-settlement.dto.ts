import { IsNotEmpty, IsString } from 'class-validator';

export class ConfirmOptimizerFeeSettlementDto {
  @IsString()
  @IsNotEmpty()
  txHash: string;
}
