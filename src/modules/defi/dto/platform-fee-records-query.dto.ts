import { Type } from 'class-transformer';
import { IsIn, IsInt, IsOptional, Max, Min } from 'class-validator';

export class PlatformFeeRecordsQueryDto {
  @IsOptional()
  @IsIn(['accrued', 'collected'])
  status?: 'accrued' | 'collected';

  @IsOptional()
  @IsIn(['swap', 'bridge', 'claim_rewards', 'deposit', 'withdrawal'])
  action?: 'swap' | 'bridge' | 'claim_rewards' | 'deposit' | 'withdrawal';

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(200)
  limit?: number;
}
