import { Body, Controller, Get, Post, Query, UseGuards } from '@nestjs/common';
import { ApiBearerAuth, ApiTags } from '@nestjs/swagger';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { AnchorService } from './anchor.service';
import { AnchorQuoteDto } from './dto/anchor-quote.dto';
import { AnchorSessionDto } from './dto/anchor-session.dto';
import { AnchorAuthChallengeDto } from './dto/anchor-auth-challenge.dto';
import { AnchorAuthTokenDto } from './dto/anchor-auth-token.dto';
import { AnchorTransactionDto } from './dto/anchor-transaction.dto';

@ApiTags('anchor')
@Controller('anchor')
@UseGuards(JwtAuthGuard)
@ApiBearerAuth()
export class AnchorController {
  constructor(private readonly anchorService: AnchorService) {}

  @Get('health')
  getHealth() {
    return this.anchorService.getHealth();
  }

  @Get('info')
  getInfo() {
    return this.anchorService.getInfo();
  }

  @Get('quote')
  getQuote(@Query() dto: AnchorQuoteDto) {
    return this.anchorService.getQuote(dto);
  }

  @Get('auth/challenge')
  getAuthChallenge(@Query() dto: AnchorAuthChallengeDto) {
    return this.anchorService.getWebAuthChallenge(dto);
  }

  @Post('auth/token')
  exchangeAuthToken(@Body() dto: AnchorAuthTokenDto) {
    return this.anchorService.exchangeWebAuthToken(dto);
  }

  @Post('session')
  createSession(@Body() dto: AnchorSessionDto) {
    return this.anchorService.createInteractiveSession(dto);
  }

  @Get('transaction')
  getTransaction(@Query() dto: AnchorTransactionDto) {
    return this.anchorService.getTransaction(dto);
  }
}
