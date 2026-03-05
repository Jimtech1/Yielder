import { ApiPropertyOptional } from '@nestjs/swagger';
import { IsIn, IsOptional, IsString, MaxLength } from 'class-validator';

export class CreateCheckoutSessionDto {
  @ApiPropertyOptional({ enum: ['premium', 'enterprise'], default: 'premium' })
  @IsOptional()
  @IsString()
  @IsIn(['premium', 'enterprise'])
  plan?: 'premium' | 'enterprise';

  @ApiPropertyOptional({
    description: 'Optional application success URL passed to billing provider',
  })
  @IsOptional()
  @IsString()
  @MaxLength(2048)
  successUrl?: string;

  @ApiPropertyOptional({
    description: 'Optional application cancel URL passed to billing provider',
  })
  @IsOptional()
  @IsString()
  @MaxLength(2048)
  cancelUrl?: string;
}

export class CreateBillingPortalDto {
  @ApiPropertyOptional({
    description: 'Optional application return URL passed to billing portal',
  })
  @IsOptional()
  @IsString()
  @MaxLength(2048)
  returnUrl?: string;
}
