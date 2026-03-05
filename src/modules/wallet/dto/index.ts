import { IsString, IsOptional, IsEnum } from 'class-validator';
import { ApiProperty } from '@nestjs/swagger';

export class CreateWalletDto {
  @ApiProperty()
  @IsEnum(['stellar', 'evm', 'ethereum', 'polygon', 'arbitrum', 'base', 'axelar'])
  chain: string;

  @ApiProperty({ required: false })
  @IsOptional()
  @IsString()
  label?: string;
}

export class ImportWalletDto {
  @ApiProperty()
  @IsEnum(['stellar', 'evm', 'ethereum', 'polygon', 'arbitrum', 'base', 'axelar'])
  chain: string;

  @ApiProperty()
  @IsString()
  address: string;

  @ApiProperty({ required: false })
  @IsOptional()
  @IsString()
  publicKey?: string;

  @ApiProperty({ required: false })
  @IsOptional()
  @IsString()
  label?: string;
}
