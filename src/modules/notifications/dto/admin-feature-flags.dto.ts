import { ApiProperty } from '@nestjs/swagger';
import { IsBoolean } from 'class-validator';

export class UpdateAdminFeatureFlagsDto {
  @ApiProperty()
  @IsBoolean()
  autoRefreshUsers: boolean;

  @ApiProperty()
  @IsBoolean()
  requireBulkConfirmation: boolean;

  @ApiProperty()
  @IsBoolean()
  compactUserRows: boolean;
}
