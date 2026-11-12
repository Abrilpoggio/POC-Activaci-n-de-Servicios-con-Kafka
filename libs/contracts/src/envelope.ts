import { randomUUID } from 'crypto';
import { EVENT_TYPES, EVENT_VERSIONS, EventType, PayloadByEventType } from './events';

/**
 * Sobre comun de todos los eventos.
 * El payload cambia segun el tipo de evento; el sobre no.
 */
export interface EventEnvelope<TEventType extends EventType = EventType> {
  /** Identificador unico; base de la idempotencia. */
  eventId: string;
  eventType: TEventType;
  /** Version del contrato; permite evolucionar sin romper consumidores. */
  version: number;
  occurredAt: string;
  /** El activationId; agrupa todos los eventos de una activacion. */
  correlationId: string;
  /** Tambien es la clave del mensaje en Kafka; garantiza orden por cliente. */
  customerId: string;
  /** Servicio que publico el evento. */
  source: string;
  payload: PayloadByEventType[TEventType];
}

export interface CreateEventInput<TEventType extends EventType = EventType> {
  eventType: TEventType;
  correlationId: string;
  customerId: string;
  source: string;
  payload: PayloadByEventType[TEventType];
  /** Override solo para pruebas/escenarios de demo. */
  eventId?: string;
  occurredAt?: string;
}

/** Crea un sobre de evento con eventId y occurredAt nuevos. */
export function createEvent<TEventType extends EventType>(
  input: CreateEventInput<TEventType>,
): EventEnvelope<TEventType> {
  return {
    eventId: input.eventId ?? randomUUID(),
    eventType: input.eventType,
    version: EVENT_VERSIONS[input.eventType] ?? 1,
    occurredAt: input.occurredAt ?? new Date().toISOString(),
    correlationId: input.correlationId,
    customerId: input.customerId,
    source: input.source,
    payload: input.payload,
  };
}

/**
 * Valida un mensaje recibido y lo devuelve como sobre tipado.
 * Lanza Error si el mensaje es invalido: el consumidor lo manda a la DLQ.
 */
export function parseEnvelope<TEventType extends EventType = EventType>(
  raw: unknown,
): EventEnvelope<TEventType> {
  const fail = (reason: string): never => {
    throw new Error(`Evento invalido: ${reason}`);
  };

  if (raw === null || typeof raw !== 'object') return fail('el valor no es un objeto JSON');
  const e = raw as Record<string, unknown>;

  if (typeof e.eventId !== 'string' || e.eventId.length === 0) return fail('falta eventId');
  if (typeof e.eventType !== 'string' || !EVENT_TYPES.includes(e.eventType as EventType)) {
    return fail(`eventType desconocido: ${String(e.eventType)}`);
  }
  if (typeof e.version !== 'number') return fail('falta version');
  if (typeof e.correlationId !== 'string' || e.correlationId.length === 0) {
    return fail('falta correlationId');
  }
  if (typeof e.customerId !== 'string' || e.customerId.length === 0) {
    return fail('falta customerId');
  }
  if (typeof e.source !== 'string' || e.source.length === 0) return fail('falta source');
  if (typeof e.occurredAt !== 'string' || Number.isNaN(Date.parse(e.occurredAt))) {
    return fail('occurredAt invalido');
  }
  if (e.payload === null || typeof e.payload !== 'object') return fail('falta payload');

  return e as unknown as EventEnvelope<TEventType>;
}
