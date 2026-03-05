import {
  IsInt,
  IsNotEmpty,
  IsObject,
  IsOptional,
  IsString,
  Matches,
  Max,
  Min,
} from 'class-validator';

export class RecordBridgeHistoryDto {
  @IsString()
  @IsNotEmpty()
  srcChainKey: string;

  @IsString()
  @IsNotEmpty()
  dstChainKey: string;

  @IsString()
  @IsNotEmpty()
  srcAddress: string;

  @IsString()
  @IsNotEmpty()
  dstAddress: string;

  @IsString()
  @IsNotEmpty()
  srcTokenSymbol: string;

  @IsString()
  @IsNotEmpty()
  dstTokenSymbol: string;

  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'srcAmount must be a numeric string' })
  srcAmount: string;

  @IsOptional()
  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'dstAmount must be a numeric string' })
  dstAmount?: string;

  @IsOptional()
  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'dstAmountMin must be a numeric string' })
  dstAmountMin?: string;

  @IsOptional()
  @IsString()
  route?: string;

  @IsOptional()
  @IsString()
  @Matches(/^0x[a-fA-F0-9]{64}$/, {
    message: 'approvalTxHash must be a valid tx hash',
  })
  approvalTxHash?: string;

  @IsString()
  @IsNotEmpty()
  bridgeTxHash: string;

  @IsOptional()
  @IsString()
  status?: string;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(7 * 24 * 60 * 60)
  estimatedDurationSeconds?: number;

  @IsOptional()
  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'feeAmount must be a numeric string' })
  feeAmount?: string;

  @IsOptional()
  @IsString()
  feeTokenSymbol?: string;

  @IsOptional()
  @IsObject()
  metadata?: Record<string, unknown>;
}
