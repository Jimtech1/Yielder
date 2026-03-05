import { ApiProperty } from '@nestjs/swagger';
import { IsIn, IsString, MaxLength, MinLength } from 'class-validator';

export class CreateAdminAuditEventDto {
  @ApiProperty()
  @IsString()
  @MinLength(2)
  @MaxLength(80)
  action: string;

  @ApiProperty({ enum: ['success', 'error', 'partial'] })
  @IsString()
  @IsIn(['success', 'error', 'partial'])
  status: 'success' | 'error' | 'partial';

  @ApiProperty()
  @IsString()
  @MinLength(1)
  @MaxLength(240)
  target: string;

  @ApiProperty()
  @IsString()
  @MinLength(1)
  @MaxLength(2000)
  details: string;
}
