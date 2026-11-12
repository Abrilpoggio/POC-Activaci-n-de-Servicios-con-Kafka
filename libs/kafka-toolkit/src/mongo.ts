import { Db, MongoClient } from 'mongodb';

export interface MongoHandle {
  client: MongoClient;
  db: Db;
  close(): Promise<void>;
}

/**
 * Coneccion a la base propia del servicio (una base por servicio).
 * Los indices comunes (processed_events, outbox) se crean siempre al conectar.
 * El URI debe apuntar a un replica set (aunque sea de 1 nodo) para transacciones.
 */
export async function connectMongo(uri: string, dbName: string): Promise<MongoHandle> {
  const client = new MongoClient(uri, { serverSelectionTimeoutMS: 15000 });
  await client.connect();
  const db = client.db(dbName);
  await ensureIndexes(db);
  return { client, db, close: () => client.close() };
}

export async function ensureIndexes(db: Db): Promise<void> {
  const processed = db.collection('processed_events');
  // Indice unico: base de la idempotencia (RNF-03).
  await processed.createIndex({ eventId: 1 }, { unique: true });
  // TTL de 7 dias: coincide con la retencion de los topics (RNF-05).
  await processed.createIndex(
    { processedAt: 1 },
    { expireAfterSeconds: 7 * 24 * 60 * 60 },
  );
  await db.collection('outbox').createIndex({ status: 1, createdAt: 1 });
}

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

export interface LoggerLike {
  log(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}
