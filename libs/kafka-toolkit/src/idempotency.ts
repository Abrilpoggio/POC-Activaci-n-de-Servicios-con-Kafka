import { ClientSession, Db, MongoClient } from 'mongodb';

export type IdempotencyResult = 'processed' | 'skipped';

const DUPLICATE_CODES = [11000, 11001];

function isDuplicateKeyError(err: unknown): boolean {
  const e = err as { code?: number; cause?: { code?: number }; message?: string };
  if (!e) return false;
  if (e.code !== undefined && DUPLICATE_CODES.includes(e.code)) return true;
  if (e.cause && e.cause.code !== undefined && DUPLICATE_CODES.includes(e.cause.code)) return true;
  if (typeof e.message === 'string' && e.message.includes('E11000')) return true;
  return false;
}

/**
 * Idempotencia de consumidor (RNF-03).
 *
 * El eventId se inserta en processed_events (indice unico) y el efecto se
 * ejecutan en la MISMA transaccion de Mongo:
 *  - evento repetido -> la insercion falla por duplicado -> se descarta ('skipped').
 *  - efecto falla    -> la transaccion se revierte -> el reintento puede
 *                       volver a intentarlo (el offset se confirma despues).
 */
export class Idempotency {
  constructor(
    private readonly db: Db,
    private readonly client: MongoClient,
  ) {}

  async run(eventId: string, work: (session: ClientSession) => Promise<void>): Promise<IdempotencyResult> {
    const processed = this.db.collection('processed_events');
    const session = this.client.startSession();
    try {
      await session.withTransaction(async () => {
        await processed.insertOne({ eventId, processedAt: new Date() }, { session });
        await work(session);
      });
      return 'processed';
    } catch (err) {
      if (isDuplicateKeyError(err)) return 'skipped';
      throw err;
    } finally {
      await session.endSession();
    }
  }
}
