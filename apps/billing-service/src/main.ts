import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { OutboxRelay } from '@poc/kafka-toolkit';
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
      client: { clientId: process.env.KAFKA_CLIENT_ID ?? 'billing-svc', brokers },
      consumer: { groupId: process.env.KAFKA_GROUP_ID ?? 'billing-svc' },
      subscribe: { fromBeginning: true },
      postfixId: '',
    },
  });

  await app.startAllMicroservices();
  app.get(OutboxRelay).start(Number(process.env.OUTBOX_INTERVAL_MS ?? 500));

  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port);
  console.log(`billing-service listo en http://localhost:${port} (consumer group: ${process.env.KAFKA_GROUP_ID ?? 'billing-svc'})`);
}

void bootstrap();
