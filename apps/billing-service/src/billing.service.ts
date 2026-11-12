import { Inject, Injectable } from '@nestjs/common';
import { createEvent, EventEnvelope, TOPICS } from '@poc/contracts';
import { MongoHandle, OutboxRelay } from '@poc/kafka-toolkit';
import { Collection, Db } from 'mongodb';

export interface BillingAccount {
  activationId: string;
  customerId: string;
  planId: string;
  status: 'CREATED' | 'CANCELLED';
  createdAt: string;
  cancelledAt?: string;
  cancelReason?: string;
}

/**
 * billing-service:
 *  - consume ActivationRequested y crea la cuenta de facturacion simulada;
 *  - consume ActivationFailed y anula la cuenta (compensacion);
 *  - publica sus resultados via outbox en billing.events.
 */
@Injectable()
export class BillingService {
  constructor(
    @Inject('MONGO_HANDLE') private readonly mongo: MongoHandle,
    private readonly outbox: OutboxRelay,
  ) {}

  private get db(): Db {
    return this.mongo.db;
  }

  private get accounts(): Collection<BillingAccount> {
    return this.db.collection<BillingAccount>('billing_accounts');
  }

  private get failures(): Collection<{ correlationId: string; reason: string; failedStep: string; at: string }> {
    return this.db.collection('saga_failures');
  }

  /** RF-04: crea la cuenta (o falla a pedido) y publica el resultado. */
  async onActivationRequested(event: EventEnvelope<'ActivationRequested'>): Promise<void> {
    const activationId = event.correlationId;
    const { planId, simulateFailure } = event.payload;
    const now = new Date().toISOString();

    const session = this.mongo.client.startSession();
    try {
      await session.withTransaction(async () => {
        // Caso borde: llego ActivationFailed antes de terminar de procesar.
        const failureSeen = await this.failures.findOne({ correlationId: activationId }, { session });

        if (simulateFailure === 'billing') {
          // Error de NEGOCIO simulado: no es tecnico, no va a la DLQ (Decisiones #2).
          await this.outbox.enqueue(
            TOPICS.BILLING_EVENTS,
            createEvent({
              eventType: 'BillingFailed',
              correlationId: activationId,
              customerId: event.customerId,
              source: 'billing-svc',
              payload: { reason: 'Fallo de facturacion simulado (simulateFailure=billing)' },
            }),
            session,
          );
          return;
        }

        const accountStatus: BillingAccount['status'] = failureSeen ? 'CANCELLED' : 'CREATED';
        await this.accounts.insertOne(
          {
            activationId,
            customerId: event.customerId,
            planId,
            status: accountStatus,
            createdAt: now,
            ...(failureSeen
              ? { cancelledAt: now, cancelReason: 'La activacion fallo antes de crear la cuenta' }
              : {}),
          },
          { session },
        );

        if (failureSeen) {
          await this.outbox.enqueue(
            TOPICS.BILLING_EVENTS,
            createEvent({
              eventType: 'BillingAccountCancelled',
              correlationId: activationId,
              customerId: event.customerId,
              source: 'billing-svc',
              payload: { reason: 'Compensacion: la cuenta se creo despues de ver ActivationFailed' },
            }),
            session,
          );
        } else {
          await this.outbox.enqueue(
            TOPICS.BILLING_EVENTS,
            createEvent({
              eventType: 'BillingAccountCreated',
              correlationId: activationId,
              customerId: event.customerId,
              source: 'billing-svc',
              payload: { planId },
            }),
            session,
          );
        }
      });
    } finally {
      await session.endSession();
    }
  }

  /** RF-06: compensacion - anula la cuenta si la activacion fallo. */
  async onActivationOutcome(event: EventEnvelope<'ActivationFailed' | 'ActivationCompleted'>): Promise<void> {
    const activationId = event.correlationId;

    if (event.eventType === 'ActivationCompleted') {
      // Nada que compensar.
      return;
    }

    const { reason, failedStep } = event.payload;
    const now = new Date().toISOString();

    const session = this.mongo.client.startSession();
    try {
      await session.withTransaction(async () => {
        // Recordar el fallo por si la cuenta todavia no existe (caso borde).
        await this.failures.updateOne(
          { correlationId: activationId },
          {
            $set: {
              correlationId: activationId,
              reason,
              failedStep,
              at: event.occurredAt,
            },
          },
          { upsert: true, session },
        );

        const account = await this.accounts.findOne({ activationId }, { session });
        if (!account || account.status === 'CANCELLED') {
          return;
        }

        await this.accounts.updateOne(
          { activationId },
          { $set: { status: 'CANCELLED', cancelledAt: now, cancelReason: reason } },
          { session },
        );
        await this.outbox.enqueue(
          TOPICS.BILLING_EVENTS,
          createEvent({
            eventType: 'BillingAccountCancelled',
            correlationId: activationId,
            customerId: event.customerId,
            source: 'billing-svc',
            payload: { reason: `Compensacion de ActivationFailed (${failedStep}): ${reason}` },
          }),
          session,
        );
      });
    } finally {
      await session.endSession();
    }
  }

  async listAccounts(): Promise<BillingAccount[]> {
    return this.accounts.find({}).sort({ createdAt: -1 }).limit(50).toArray();
  }
}
