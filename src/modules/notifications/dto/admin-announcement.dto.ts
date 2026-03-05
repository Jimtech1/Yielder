import { ApiProperty } from '@nestjs/swagger';
import { IsIn, IsOptional, IsString, MaxLength, MinLength } from 'class-validator';
import type { AnnouncementStatus } from '../schemas/announcement.schema';

export class CreateAdminAnnouncementDto {
  @ApiProperty({ minLength: 2, maxLength: 140 })
  @IsString()
  @MinLength(2)
  @MaxLength(140)
  title: string;

  @ApiProperty({ minLength: 3, maxLength: 4000 })
  @IsString()
  @MinLength(3)
  @MaxLength(4000)
  message: string;
}

export class UpdateAdminAnnouncementStatusDto {
  @ApiProperty({ enum: ['active', 'archived'] })
  @IsString()
  @IsIn(['active', 'archived'])
  status: AnnouncementStatus;
}

export class ListAdminAnnouncementsQueryDto {
  @ApiProperty({ required: false, enum: ['all', 'active', 'archived'] })
  @IsOptional()
  @IsString()
  @IsIn(['all', 'active', 'archived'])
  status?: 'all' | AnnouncementStatus;
}
