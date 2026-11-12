import { EventEnvelope } from '@poc/contracts';
import { ClientSession, Collection, Db, ObjectId } from 'mongodb';
import { EventPublisher } from './publisher';
import { LoggerLike } from './mongo';

export type OutboxStatus = 'PENDING' | 'SENDING' | 'SENT';

export interface OutboxDoc {
  _id: ObjectId;
  topic: string;
  event: EventEnvelope;
  status: OutboxStatus;
  createdAt: Date;
  claimedAt?: Date;
  sentAt?: Date;
  attempts: number;
}

const CLAIM_STALE_MS = 15000;

/**
 * Outbox transaccional (Decisiones tecnicas #3).
 *
 * El evento se guarda en la coleccion `outbox` en la MISMA transaccion de Mongo
 * que el cambio de estado que lo origina. Si el servicio se cae antes de
 * publicar, el evento no se pierde: `tick()` lo retoma y publica.
 *
 * Se publica en orden (sort createdAt) cada 500 ms. Si se cae despues de
 * publicar y antes de marcar SENT, se vuelve a publicar: los consumidores son
 * idempotentes, asi que el duplicado no tiene efectos.
 */
export class OutboxRelay {
  private timer?: ReturnType<typeof setInterval>;
  private ticking = false;
  private readonly logger: LoggerLike;

  constructor(
    private readonly db: Db,
    private readonly publisher: EventPublisher,
    logger?: LoggerLike,
  ) {
    this.logger = logger ?? console;
  }

  private get collection(): Collection<OutboxDoc> {
    return this.db.collection<OutboxDoc>('outbox');
  }

  /** Encola dentro de la transaccion abierta del llamador. */
  async enqueue(topic: string, event: EventEnvelope, session?: ClientSession): Promise<void> {
    await this.collection.insertOne(
      {
        _id: new ObjectId(),
        topic,
        event,
        status: 'PENDING',
        createdAt: new Date(),
        attempts: 0,
      },
      session ? { session } : {},
    );
  }

  start(intervalMs = 500): void {
    if (this.timer) return;
    this.timer = setInterval(() => void this.tick(), intervalMs);
    if (typeof this.timer.unref === 'function') this.timer.unref();
    this.logger.log(`[outbox] relay iniciado (cada ${intervalMs} ms)`);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }

  /** Publica pendientes. `claim` evita que dos instancias publiquen el mismo evento. */
  async tick(): Promise<void> {
    if (this.ticking) return;
    this.ticking = true;
    try {
      for (let i = 0; i < 25; i++) {
        const staleBefore = new Date(Date.now() - CLAIM_STALE_MS);
        const doc = await this.collection.findOneAndUpdate(
          {
            $or: [
              { status: 'PENDING' },
              { status: 'SENDING', claimedAt: { $lt: staleBefore } },
            ],
          },
          { $set: { status: 'SENDING', claimedAt: new Date() }, $inc: { attempts: 1 } },
          { sort: { createdAt: 1 }, returnDocument: 'after' },
        );
        if (!doc) break;

        try {
          await this.publisher.publish(doc.topic, doc.event);
          await this.collection.updateOne(
            { _id: doc._id },
            { $set: { status: 'SENT', sentAt: new Date() } },
          );
        } catch (err) {
          this.logger.error(
            `[outbox] fallo publicando ${doc.event.eventId} (${doc.event.eventType}): ${
              (err as Error).message
            }; se reintenta en el proximo tick`,
          );
          await this.collection
            .updateOne({ _id: doc._id }, { $set: { status: 'PENDING' } })
            .catch(() => undefined);
          break;
        }
      }
    } catch (err) {
      this.logger.error(`[outbox] tick falló: ${(err as Error).message}`);
    } finally {
      this.ticking = false;
    }
  }
}
