import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { EventEnvelope } from '@poc/contracts';
import { MongoHandle } from '@poc/kafka-toolkit';
import { Collection, Db } from 'mongodb';

export interface LoyaltyPointsDoc {
  customerId: string;
  points: number;
  updatedAt: string;
  lastActivationId: string;
}

export interface LoyaltyAwardDoc {
  eventId: string;
  customerId: string;
  planId: string;
  points: number;
  at: string;
}

/** Puntaje por plan (Escenario 5). */
const POINTS_BY_PLAN: Record<string, number> = {
  'plan basico': 50,
  'plan digital': 100,
  'plan premium': 250,
};
const DEFAULT_POINTS = 100;

/**
 * loyalty-service: acumula puntos por cliente cuando la activacion termina
 * bien. Los puntos se suman dentro de la transaccion de idempotencia, asi
 * un replay del historial (grupo nuevo desde el inicio) no duplica saldos.
 */
@Injectable()
export class LoyaltyService implements OnModuleInit {
  // @Inject porque MongoHandle es una interface: sin esto tsc emite `Object`
  // como token y Nest no puede resolverlo.
  constructor(@Inject('MONGO_HANDLE') private readonly mongo: MongoHandle) {}

  private get db(): Db {
    return this.mongo.db;
  }

  private get points(): Collection<LoyaltyPointsDoc> {
    return this.db.collection<LoyaltyPointsDoc>('loyalty_points');
  }

  private get awards(): Collection<LoyaltyAwardDoc> {
    return this.db.collection<LoyaltyAwardDoc>('loyalty_awards');
  }

  /** Saldo por cliente y premio por evento (unico). */
  async onModuleInit(): Promise<void> {
    await this.points.createIndex({ customerId: 1 }, { unique: true });
    await this.awards.createIndex({ eventId: 1 }, { unique: true });
  }

  /** Suma los puntos del plan; ActivationFailed no suma nada. */
  async onActivationOutcome(
    event: EventEnvelope<'ActivationCompleted' | 'ActivationFailed'>,
  ): Promise<void> {
    if (event.eventType !== 'ActivationCompleted') {
      return;
    }

    const { planId } = event.payload as { planId: string };
    const points =
      POINTS_BY_PLAN[(planId ?? '').trim().toLowerCase()] ?? DEFAULT_POINTS;
    const now = new Date().toISOString();

    const session = this.mongo.client.startSession();
    try {
      await session.withTransaction(async () => {
        await this.points.updateOne(
          { customerId: event.customerId },
          {
            $inc: { points },
            $set: { updatedAt: now, lastActivationId: event.correlationId },
          },
          { upsert: true, session },
        );
        await this.awards.insertOne(
          {
            eventId: event.eventId,
            customerId: event.customerId,
            planId,
            points,
            at: now,
          },
          { session },
        );
      });
    } catch (err) {
      // E11000 en awards: ya se premio este evento. No se suma dos veces.
      if ((err as { code?: number }).code !== 11000) throw err;
    } finally {
      await session.endSession();
    }
  }

  /** Saldos ordenados de mayor a menor, para /points. */
  async listPoints(): Promise<Array<{ customerId: string; points: number; updatedAt: string }>> {
    return this.points
      .find({}, { projection: { _id: 0, customerId: 1, points: 1, updatedAt: 1 } })
      .sort({ points: -1 })
      .limit(50)
      .toArray();
  }
}
