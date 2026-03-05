import { Type } from 'class-transformer';
import { IsArray, IsInt, IsOptional, IsString, Matches, Max, Min } from 'class-validator';

export class CollectPlatformFeesDto {
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  feeIds?: string[];

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(500)
  limit?: number;

  @IsOptional()
  @IsString()
  @Matches(/^(0x)?[a-fA-F0-9]{64}$/, {
    message: 'collectionTxHash must be a 64-byte hex hash',
  })
  collectionTxHash?: string;
}
