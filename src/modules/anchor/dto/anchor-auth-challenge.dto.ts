import { IsNotEmpty, IsOptional, IsString } from 'class-validator';

export class AnchorAuthChallengeDto {
  @IsString()
  @IsNotEmpty()
  account: string;

  @IsOptional()
  @IsString()
  memo?: string;
}

