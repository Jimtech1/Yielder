import { IsString, IsNotEmpty, IsOptional } from 'class-validator';

export class AddWatchWalletDto {
  @IsString()
  @IsNotEmpty()
  address: string;

  @IsString()
  @IsNotEmpty()
  chain: string; // 'stellar'

  @IsOptional()
  @IsString()
  label?: string;
}
