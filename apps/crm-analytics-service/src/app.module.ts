import { Module } from '@nestjs/common';
import {
  connectMongo,
  EventPublisher,
  Idempotency,
  MongoHandle,
  ReliableConsumer,
} from '@poc/kafka-toolkit';
import { CrmController } from './crm.controller';
import { CrmService } from './crm.service';
import { HealthController } from './health.controller';
import { ReplayController, ReplayService } from './replay.service';

const MONGO_URI = process.env.MONGO_URI ?? 'mongodb://localhost:27017/?replicaSet=rs0&directConnection=true';
const MONGO_DB = process.env.MONGO_DB ?? 'crm_analytics';
const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? 'crm-analytics';

@Module({
  controllers: [CrmController, ReplayController, HealthController],
  providers: [
    {
      provide: 'MONGO_HANDLE',
      useFactory: async (): Promise<MongoHandle> => connectMongo(MONGO_URI, MONGO_DB),
    },
    {
      // El publisher se necesita igual: es el que publica a la DLQ.
      provide: EventPublisher,
      useFactory: async (): Promise<EventPublisher> => {
        const publisher = new EventPublisher({ brokers: BROKERS, clientId: `${CLIENT_ID}-producer` });
        await publisher.connect();
        return publisher;
      },
    },
    {
      provide: Idempotency,
      useFactory: (mongo: MongoHandle): Idempotency => new Idempotency(mongo.db, mongo.client),
      inject: ['MONGO_HANDLE'],
    },
    {
      provide: ReliableConsumer,
      useFactory: (idempotency: Idempotency, publisher: EventPublisher): ReliableConsumer =>
        new ReliableConsumer({ idempotency, publisher }),
      inject: [Idempotency, EventPublisher],
    },
    CrmService,
    ReplayService,
  ],
})
export class AppModule {}
