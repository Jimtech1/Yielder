import { Body, Controller, Post, Req, UseGuards } from '@nestjs/common';
import { ApiBearerAuth, ApiTags } from '@nestjs/swagger';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { AuthenticatedUser } from '../auth/interfaces/authenticated-user.interface';
import { BillingService } from './billing.service';
import {
  CreateBillingPortalDto,
  CreateCheckoutSessionDto,
} from './dto/create-checkout-session.dto';

@ApiTags('billing')
@Controller('billing')
@UseGuards(JwtAuthGuard)
@ApiBearerAuth()
export class BillingController {
  constructor(private readonly billingService: BillingService) {}

  @Post('checkout')
  createCheckout(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: CreateCheckoutSessionDto,
  ) {
    return this.billingService.createCheckoutSession(req.user, dto);
  }

  @Post('portal')
  createPortal(
    @Req() req: { user: AuthenticatedUser },
    @Body() dto: CreateBillingPortalDto,
  ) {
    return this.billingService.createPortalSession(req.user, dto);
  }
}
