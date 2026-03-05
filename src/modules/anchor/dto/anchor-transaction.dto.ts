import { IsOptional, IsString } from 'class-validator';

export class AnchorTransactionDto {
  @IsOptional()
  @IsString()
  id?: string;

  @IsOptional()
  @IsString()
  externalTransactionId?: string;

  @IsOptional()
  @IsString()
  stellarTransactionId?: string;

  @IsOptional()
  @IsString()
  lang?: string;

  @IsOptional()
  @IsString()
  authToken?: string;
}
