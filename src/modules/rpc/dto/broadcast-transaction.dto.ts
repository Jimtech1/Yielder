import { IsIn, IsNotEmpty, IsOptional, IsString } from 'class-validator';

export class BroadcastTransactionDto {
  @IsString()
  @IsNotEmpty()
  transaction: string;

  @IsOptional()
  @IsString()
  @IsIn(['testnet', 'mainnet', 'public'])
  network?: string;
}
