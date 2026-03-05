import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, NestFastifyApplication } from '@nestjs/platform-fastify';
import { ValidationPipe } from '@nestjs/common';
import { SwaggerModule, DocumentBuilder } from '@nestjs/swagger';
import { AppModule } from './modules/app.module';
import { PinoLoggerService } from './shared/logger';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter({ logger: false }),
    { bufferLogs: true }
  );

  const logger = app.get(PinoLoggerService);
  app.useLogger(logger);

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: false,
      transform: true,
    })
  );

  const rawCorsOrigin = String(process.env.CORS_ORIGIN ?? '').trim();
  const parsedCorsOrigins = rawCorsOrigin
    .split(',')
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
  const corsOrigin =
    parsedCorsOrigins.length === 0
      ? true // reflect request origin when unset
      : parsedCorsOrigins.length === 1
        ? parsedCorsOrigins[0]
        : parsedCorsOrigins;

  app.enableCors({
    origin: corsOrigin,
    credentials: true,
  });

  const config = new DocumentBuilder()
    .setTitle('Yielder Portfolio API')
    .setDescription('Multi-chain portfolio platform backend')
    .setVersion('1.0')
    .addBearerAuth()
    .build();
  
  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('api/docs', app, document);

  const port = process.env.PORT || 3000;
  await app.listen(port, '0.0.0.0');
  
  logger.log(`🚀 Yielder Backend running on port ${port}`);
}

bootstrap();
