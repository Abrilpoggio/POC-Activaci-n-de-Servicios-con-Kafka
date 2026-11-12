import { Controller, Inject, Injectable, Post } from '@nestjs/common';
import { ALL_TOPICS, parseEnvelope } from '@poc/contracts';
import { MongoHandle } from '@poc/kafka-toolkit';
import { Kafka, logLevel } from 'kafkajs';
import { Collection, Db } from 'mongodb';
import { EventLogDoc } from './crm.service';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

export interface ReplayResult {
  replayed: number;
  group: string;
  durationMs: number;
}

/**
 * RF-10: reprocesar el historial desde el inicio.
 *
 * Abre un consumidor kafkajs CON GROUP ID NUEVO (crm-replay-<ts>) y
 * fromBeginning en los cuatro topics: relee todo el backlog sin tocar a
 * los grupos viejos. Cada sobre se sube con upsert por eventId (idempotente)
 * y el consumidor se corta solo cuando el log deja de entregar.
 */
@Injectable()
export class ReplayService {
  constructor(@Inject('MONGO_HANDLE') private readonly mongo: MongoHandle) {}

  private get db(): Db {
    return this.mongo.db;
  }

  private get eventLog(): Collection<EventLogDoc> {
    return this.db.collection<EventLogDoc>('event_log');
  }

  async replay(): Promise<ReplayResult> {
    const group = `crm-replay-${Date.now()}`;
    const startedAt = Date.now();
    let replayed = 0;
    let firstActivityAt = 0;
    let lastMessageAt = 0;

    const kafka = new Kafka({ clientId: 'crm-replay', brokers, logLevel: logLevel.NOTHING });
    const consumer = kafka.consumer({ groupId: group });

    try {
      await consumer.connect();
      for (const topic of ALL_TOPICS) {
        await consumer.subscribe({ topic, fromBeginning: true });
      }

      const runPromise = consumer.run({
        eachMessage: async ({ topic, message }) => {
          const now = Date.now();
          if (!firstActivityAt) firstActivityAt = now;
          lastMessageAt = now;

          try {
            const raw = message.value?.toString();
            if (!raw) return;
            const event = parseEnvelope(JSON.parse(raw));
            await this.eventLog.updateOne(
              { eventId: event.eventId },
              {
                $setOnInsert: {
                  eventId: event.eventId,
                  correlationId: event.correlationId,
                  eventType: event.eventType,
                  topic,
                  occurredAt: event.occurredAt,
                  source: event.source,
                  payload: event.payload,
                  receivedAt: new Date().toISOString(),
                },
              },
              { upsert: true },
            );
            replayed++;
          } catch (err) {
            // Sobre roto o duplicado concurrente: se salta, no corta el replay.
            console.error(`[crm-replay] mensaje descartado: ${(err as Error).message}`);
          }
        },
      });
      // Si el run se rechaza tras el disconnect, se ignora.
      runPromise.catch(() => undefined);

      await new Promise<void>((resolve) => {
        const timer = setInterval(() => {
          const now = Date.now();
          // Con mensajes: corta a los 1.5 s de quietud. Sin ninguno: a los ~2 s.
          const seenAny = firstActivityAt > 0;
          const quietMs = seenAny ? now - lastMessageAt : now - startedAt;
          const limitMs = seenAny ? 1500 : 2000;
          const timedOut = now - startedAt >= 60000; // Limite de seguridad.

          if (quietMs >= limitMs || timedOut) {
            clearInterval(timer);
            void consumer
              .disconnect()
              .catch(() => undefined)
              .then(() => resolve());
          }
        }, 200);
      });
    } catch (err) {
      // Nunca se propaga: el endpoint responde igual con lo que alcanzo.
      console.error(`[crm-replay] fallo: ${(err as Error).message}`);
      try {
        await consumer.disconnect();
      } catch {
        // Ya estaba desconectado.
      }
    }

    return { replayed, group, durationMs: Date.now() - startedAt };
  }
}

/** POST /event-log/replay -> relanza la proyeccion con un grupo nuevo. */
@Controller()
export class ReplayController {
  constructor(private readonly replayService: ReplayService) {}

  @Post('event-log/replay')
  async replay(): Promise<ReplayResult> {
    return this.replayService.replay();
  }
}
