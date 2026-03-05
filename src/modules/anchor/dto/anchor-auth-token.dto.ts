import { IsNotEmpty, IsString } from 'class-validator';

export class AnchorAuthTokenDto {
  @IsString()
  @IsNotEmpty()
  transaction: string;
}

