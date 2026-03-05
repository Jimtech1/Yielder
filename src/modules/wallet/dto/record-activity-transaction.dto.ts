import { IsIn, IsNotEmpty, IsObject, IsOptional, IsString, Matches } from 'class-validator';

export class RecordActivityTransactionDto {
  @IsOptional()
  @IsString()
  chain?: string;

  @IsString()
  @IsNotEmpty()
  address: string;

  @IsString()
  @IsIn([
    'deposit',
    'withdrawal',
    'swap',
    'bridge',
    'stake',
    'unstake',
    'transfer',
    'fee',
    'claim_rewards',
  ])
  type: string;

  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'amount must be a numeric string' })
  amount: string;

  @IsString()
  @IsNotEmpty()
  asset: string;

  @IsOptional()
  @IsString()
  @IsIn(['pending', 'completed', 'failed', 'submitted', 'confirmed', 'processing'])
  status?: string;

  @IsOptional()
  @IsString()
  txHash?: string;

  @IsOptional()
  @IsString()
  referenceId?: string;

  @IsOptional()
  @IsString()
  from?: string;

  @IsOptional()
  @IsString()
  to?: string;

  @IsOptional()
  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'fee must be a numeric string' })
  fee?: string;

  @IsOptional()
  @IsObject()
  metadata?: Record<string, unknown>;
}
