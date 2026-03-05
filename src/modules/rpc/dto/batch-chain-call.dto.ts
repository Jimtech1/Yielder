import { Type } from 'class-transformer';
import { ArrayMinSize, IsArray, ValidateNested } from 'class-validator';
import { ChainCallDto } from './chain-call.dto';

export class BatchChainCallDto {
  @IsArray()
  @ArrayMinSize(1)
  @ValidateNested({ each: true })
  @Type(() => ChainCallDto)
  calls: ChainCallDto[];
}
