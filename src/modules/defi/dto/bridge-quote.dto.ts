import { Type } from 'class-transformer';
import {
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  Matches,
  Max,
  Min,
} from 'class-validator';

export class BridgeQuoteDto {
  @IsString()
  @IsNotEmpty()
  srcChainKey: string;

  @IsString()
  @IsNotEmpty()
  dstChainKey: string;

  @IsString()
  @IsNotEmpty()
  srcToken: string;

  @IsString()
  @IsNotEmpty()
  dstToken: string;

  @IsString()
  @IsNotEmpty()
  srcAddress: string;

  @IsOptional()
  @IsString()
  dstAddress?: string;

  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'srcAmount must be a numeric string' })
  srcAmount: string;

  @IsOptional()
  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'dstAmountMin must be a numeric string' })
  dstAmountMin?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(5000)
  slippageBps?: number;

  @IsOptional()
  @IsString()
  route?: string;
}
