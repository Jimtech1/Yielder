import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import { IsInt, IsOptional, IsString, Max, MaxLength, Min } from 'class-validator';

export class AdminSuspendUserDto {
  @ApiPropertyOptional({ description: 'Optional suspension reason' })
  @IsOptional()
  @IsString()
  @MaxLength(240)
  reason?: string;
}

export class AdminUserDetailsQueryDto {
  @ApiPropertyOptional({
    description: 'Number of recent transactions to include',
    minimum: 1,
    maximum: 100,
  })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(100)
  txLimit?: number;
}

export class AdminTargetUserPathDto {
  @ApiProperty()
  @IsString()
  userId: string;
}

