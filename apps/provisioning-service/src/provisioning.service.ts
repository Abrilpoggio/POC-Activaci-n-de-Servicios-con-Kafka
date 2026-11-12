import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { createEvent, EventEnvelope, TOPICS } from '@poc/contracts';
import { MongoHandle, OutboxRelay, sleep } from '@poc/kafka-toolkit';
import { Collection, Db } from 'mongodb';

export interface ProvisioningOrder {
  activationId: string;
  customerId: string;
  planId: string;
  status: 'PROVISIONED';
  provisionedAt: string;
}

/**
 * provisioning-service:
 *  - consume ActivationRequested y simula el aprovisionamiento (1-3 s);
 *  - guarda la orden y publica el resultado via outbox en provisioning.events.
 */
@Injectable()
export class ProvisioningService implements OnModuleInit {
  constructor(
    @Inject('MONGO_HANDLE') private readonly mongo: MongoHandle,
    private readonly outbox: OutboxRelay,
  ) {}

  private get db(): Db {
    return this.mongo.db;
  }

  private get orders(): Collection<ProvisioningOrder> {
    return this.db.collection<ProvisioningOrder>('provisioning_orders');
  }

  /** Orden unica por activacion. */
  async onModuleInit(): Promise<void> {
    await this.orders.createIndex({ activationId: 1 }, { unique: true });
  }

  /** RF-05: aprovisiona (o falla a pedido) y publica el resultado. */
  async onActivationRequested(event: EventEnvelope<'ActivationRequested'>): Promise<void> {
    const activationId = event.correlationId;
    const { planId, simulateFailure } = event.payload;

    // Retardo simulado del aprovisionamiento: entre 1 y 3 segundos.
    await sleep(1000 + Math.floor(Math.random() * 2001));

    const session = this.mongo.client.startSession();
    try {
      await session.withTransaction(async () => {
        if (simulateFailure === 'provisioning') {
          // Error de NEGOCIO simulado: no es tecnico, no va a la DLQ (Decisiones #2).
          await this.outbox.enqueue(
            TOPICS.PROVISIONING_EVENTS,
            createEvent({
              eventType: 'ProvisioningFailed',
              correlationId: activationId,
              customerId: event.customerId,
              source: 'provisioning-svc',
              payload: {
                reason: 'Fallo de aprovisionamiento simulado (simulateFailure=provisioning)',
              },
            }),
            session,
          );
          return;
        }

        await this.orders.insertOne(
          {
            activationId,
            customerId: event.customerId,
            planId,
            status: 'PROVISIONED',
            provisionedAt: new Date().toISOString(),
          },
          { session },
        );

        await this.outbox.enqueue(
          TOPICS.PROVISIONING_EVENTS,
          createEvent({
            eventType: 'ProvisioningCompleted',
            correlationId: activationId,
            customerId: event.customerId,
            source: 'provisioning-svc',
            payload: { planId },
          }),
          session,
        );
      });
    } finally {
      await session.endSession();
    }
  }

  /** Ultimas 50 ordenes, para la demo. */
  async listOrders(): Promise<ProvisioningOrder[]> {
    return this.orders.find({}).sort({ provisionedAt: -1 }).limit(50).toArray();
  }
}
