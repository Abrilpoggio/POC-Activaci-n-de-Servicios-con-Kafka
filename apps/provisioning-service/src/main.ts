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
      client: { clientId: process.env.KAFKA_CLIENT_ID ?? 'provisioning-svc', brokers },
      consumer: { groupId: process.env.KAFKA_GROUP_ID ?? 'provisioning-svc' },
      subscribe: { fromBeginning: true },
      postfixId: '',
    },
  });

  await app.startAllMicroservices();

  // Outbox transaccional: publica cada 500 ms lo que se guardo en la transaccion.
  app.get(OutboxRelay).start(Number(process.env.OUTBOX_INTERVAL_MS ?? 500));

  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port);
  console.log(
    `provisioning-service listo en http://localhost:${port} (consumer group: ${
      process.env.KAFKA_GROUP_ID ?? 'provisioning-svc'
    })`,
  );
}

void bootstrap();
