import { IsArray, IsOptional, IsString } from 'class-validator';

export class ChainCallDto {
  @IsString()
  method: string;

  @IsArray()
  @IsOptional()
  params?: unknown[];
}
