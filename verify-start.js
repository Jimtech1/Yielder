
const { NestFactory } = require('@nestjs/core');
const { AppModule } = require('./dist/modules/app.module');

async function bootstrap() {
  try {
    const app = await NestFactory.create(AppModule);
    await app.init();
    console.log('App initialized successfully');
    await app.close();
    process.exit(0);
  } catch (error) {
    console.error('App failed to start', error);
    process.exit(1);
  }
}
bootstrap();
