import { IsNotEmpty, IsOptional, IsString, Matches } from 'class-validator';

export class AnchorQuoteDto {
  @IsString()
  @IsNotEmpty()
  sellAsset: string;

  @IsString()
  @IsNotEmpty()
  buyAsset: string;

  @IsOptional()
  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'sellAmount must be a numeric string' })
  sellAmount?: string;

  @IsOptional()
  @IsString()
  @Matches(/^\d+(\.\d+)?$/, { message: 'buyAmount must be a numeric string' })
  buyAmount?: string;

  @IsOptional()
  @IsString()
  countryCode?: string;

  @IsOptional()
  @IsString()
  authToken?: string;
}
