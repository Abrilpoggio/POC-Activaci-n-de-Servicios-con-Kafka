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
      client: { clientId: process.env.KAFKA_CLIENT_ID ?? 'loyalty-svc', brokers },
      consumer: { groupId: process.env.KAFKA_GROUP_ID ?? 'loyalty-svc' },
      // Escenario 5: grupo nuevo desde el inicio, relea todo el historial.
      subscribe: { fromBeginning: true },
      postfixId: '',
    },
  });

  await app.startAllMicroservices();
  // Sin outbox: solo acumula puntos; el publisher es para la DLQ.

  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port);
  console.log(
    `loyalty-service listo en http://localhost:${port} (consumer group: ${
      process.env.KAFKA_GROUP_ID ?? 'loyalty-svc'
    })`,
  );
}

void bootstrap();
