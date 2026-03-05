import { BridgeQuoteDto } from './bridge-quote.dto';
import { Type } from 'class-transformer';
import { IsBoolean, IsOptional } from 'class-validator';

export class BuildBridgeTransactionDto extends BridgeQuoteDto {
  @IsOptional()
  @Type(() => Boolean)
  @IsBoolean()
  executeOnBackend?: boolean;
}
