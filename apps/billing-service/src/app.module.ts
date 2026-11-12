import { Module } from '@nestjs/common';
import { connectMongo, EventPublisher, Idempotency, MongoHandle, OutboxRelay, ReliableConsumer } from '@poc/kafka-toolkit';
import { BillingController } from './billing.controller';
import { BillingService } from './billing.service';
import { HealthController } from './health.controller';

const MONGO_URI = process.env.MONGO_URI ?? 'mongodb://localhost:27017/?replicaSet=rs0&directConnection=true';
const MONGO_DB = process.env.MONGO_DB ?? 'billing';
const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? 'billing-svc';

@Module({
  controllers: [BillingController, HealthController],
  providers: [
    {
      provide: 'MONGO_HANDLE',
      useFactory: async (): Promise<MongoHandle> => connectMongo(MONGO_URI, MONGO_DB),
    },
    {
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
      provide: OutboxRelay,
      useFactory: (mongo: MongoHandle, publisher: EventPublisher): OutboxRelay =>
        new OutboxRelay(mongo.db, publisher),
      inject: ['MONGO_HANDLE', EventPublisher],
    },
    {
      provide: ReliableConsumer,
      useFactory: (idempotency: Idempotency, publisher: EventPublisher): ReliableConsumer =>
        new ReliableConsumer({ idempotency, publisher }),
      inject: [Idempotency, EventPublisher],
    },
    BillingService,
  ],
})
export class AppModule {}
