import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { EventEnvelope } from '@poc/contracts';
import { MongoHandle } from '@poc/kafka-toolkit';
import { Collection, Db, Filter } from 'mongodb';

export interface EventLogDoc {
  eventId: string;
  correlationId: string;
  eventType: string;
  topic: string;
  occurredAt: string;
  source: string;
  payload: unknown;
  receivedAt: string;
}

/**
 * crm-analytics-service (RF-10): proyecta cada sobre consumido en
 * `event_log`, la fuente de /event-log y /stats.
 */
@Injectable()
export class CrmService implements OnModuleInit {
  // @Inject porque MongoHandle es una interface: sin esto tsc emite `Object`
  // como token y Nest no puede resolverlo.
  constructor(@Inject('MONGO_HANDLE') private readonly mongo: MongoHandle) {}

  private get db(): Db {
    return this.mongo.db;
  }

  private get eventLog(): Collection<EventLogDoc> {
    return this.db.collection<EventLogDoc>('event_log');
  }

  /** eventId unico (evita duplicados en el replay) + correlationId para filtrar. */
  async onModuleInit(): Promise<void> {
    await this.eventLog.createIndex({ eventId: 1 }, { unique: true });
    await this.eventLog.createIndex({ correlationId: 1 });
  }

  /** Guarda el sobre dentro de la transaccion de idempotencia. */
  async logEvent(event: EventEnvelope, topic: string): Promise<void> {
    const session = this.mongo.client.startSession();
    try {
      await session.withTransaction(async () => {
        await this.eventLog.insertOne(
          {
            eventId: event.eventId,
            correlationId: event.correlationId,
            eventType: event.eventType,
            topic,
            occurredAt: event.occurredAt,
            source: event.source,
            payload: event.payload,
            receivedAt: new Date().toISOString(),
          },
          { session },
        );
      });
    } catch (err) {
      // E11000: ya estaba registrado (replay concurrente). No es un error.
      if ((err as { code?: number }).code !== 11000) throw err;
    } finally {
      await session.endSession();
    }
  }

  /** Ultimos N eventos, opcionalmente de una sola activacion. */
  async listEvents(limit: number, correlationId?: string): Promise<EventLogDoc[]> {
    const filter: Filter<EventLogDoc> = correlationId ? { correlationId } : {};
    return this.eventLog.find(filter).sort({ receivedAt: -1 }).limit(limit).toArray();
  }

  /** Total de documentos y conteo por tipo (aggregate). */
  async stats(): Promise<{ total: number; porTipo: Array<{ eventType: string; count: number }> }> {
    const total = await this.eventLog.countDocuments({});
    const agregado = await this.eventLog
      .aggregate<{ _id: string; count: number }>([
        { $group: { _id: '$eventType', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
      ])
      .toArray();

    return {
      total,
      porTipo: agregado.map((r) => ({ eventType: r._id, count: r.count })),
    };
  }
}
