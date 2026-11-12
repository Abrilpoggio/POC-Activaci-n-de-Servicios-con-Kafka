import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  app.enableCors({ origin: true });

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: { clientId: process.env.KAFKA_CLIENT_ID ?? 'notification-svc', brokers },
      consumer: { groupId: process.env.KAFKA_GROUP_ID ?? 'notification-svc' },
      subscribe: { fromBeginning: true },
      postfixId: '',
    },
  });

  await app.startAllMicroservices();
  // Sin outbox: este servicio no publica eventos de negocio (solo usa el
  // publisher de ReliableConsumer para la DLQ).

  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port);
  console.log(
    `notification-service listo en http://localhost:${port} (consumer group: ${
      process.env.KAFKA_GROUP_ID ?? 'notification-svc'
    })`,
  );
}

void bootstrap();
