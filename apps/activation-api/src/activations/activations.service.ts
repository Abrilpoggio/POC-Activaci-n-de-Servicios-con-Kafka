import { Inject, Injectable } from '@nestjs/common';
import { createEvent, EventEnvelope, EventType, TOPICS } from '@poc/contracts';
import { MongoHandle, OutboxRelay } from '@poc/kafka-toolkit';
import { Collection, Db } from 'mongodb';
import { TimelineGateway } from './timeline.gateway';

export interface CreateActivationDto {
  customerId?: string;
  planId?: string;
  channel?: string;
  simulateFailure?: 'none' | 'billing' | 'provisioning';
}

export interface ActivationStep {
  status: 'OK' | 'FAILED';
  at: string;
  eventType: EventType;
}

export interface ActivationDoc {
  _id: string;
  customerId: string;
  planId: string;
  status: 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';
  simulateFailure: string;
  steps: { billing?: ActivationStep; provisioning?: ActivationStep };
  history: Array<{
    eventType: EventType;
    at: string;
    source: string;
    payload: Record<string, unknown>;
  }>;
  createdAt: string;
  updatedAt: string;
}

/**
 * Agregador de la saga (coreografia con agregador):
 * solo guarda estado y junta resultados; nunca llama a otro servicio.
 */
@Injectable()
export class ActivationsService {
  constructor(
    @Inject('MONGO_HANDLE') private readonly mongo: MongoHandle,
    private readonly outbox: OutboxRelay,
    private readonly gateway: TimelineGateway,
  ) {}

  private get db(): Db {
    return this.mongo.db;
  }

  private get activations(): Collection<ActivationDoc> {
    return this.db.collection<ActivationDoc>('activations');
  }

  /** RF-02 / RF-03: crea la activacion, responde 202 y encola ActivationRequested. */
  async create(dto: CreateActivationDto): Promise<{ activationId: string; status: string }> {
    const customerId = (dto.customerId ?? '').trim();
    const planId = (dto.planId ?? '').trim();

    const activationId = `act-${Math.random().toString(36).slice(2, 10)}`;
    const now = new Date().toISOString();
    const simulateFailure = dto.simulateFailure ?? 'none';

    const requested = new ActivationRequestedFactory().create(activationId, customerId, planId, dto.channel ?? 'web', simulateFailure);

    const doc: ActivationDoc = {
      _id: activationId,
      customerId,
      planId,
      status: 'PENDING',
      simulateFailure,
      steps: {},
      history: [
        {
          eventType: 'ActivationRequested',
          at: now,
          source: requested.source,
          payload: requested.payload as unknown as Record<string, unknown>,
        },
      ],
      createdAt: now,
      updatedAt: now,
    };

    // Activacion + evento en la MISMA transaccion (outbox transaccional).
    const session = this.mongo.client.startSession();
    try {
      await session.withTransaction(async () => {
        await this.activations.insertOne(doc, { session });
        await this.outbox.enqueue(TOPICS.ACTIVATION_REQUESTED, requested, session);
      });
    } finally {
      await session.endSession();
    }

    this.gateway.broadcast({
      activationId,
      correlationId: activationId,
      eventType: 'ActivationRequested',
      occurredAt: now,
      source: requested.source,
      status: 'PENDING',
      payload: requested.payload,
    });

    return { activationId, status: 'PENDING' };
  }

  /**
   * RF-05 / RF-06: guarda el resultado de billing o provisioning y decide si
   * la activacion termino bien (ACTIVE + ActivationCompleted) o mal
   * (FAILED + ActivationFailed). Todo dentro de una transaccion.
   */
  async applyResult(event: EventEnvelope): Promise<void> {
    const activationId = event.correlationId;
    const isBilling = event.eventType.startsWith('Billing');
    const step: 'billing' | 'provisioning' = isBilling ? 'billing' : 'provisioning';
    const stepStatus: 'OK' | 'FAILED' =
      event.eventType === 'BillingAccountCreated' || event.eventType === 'ProvisioningCompleted'
        ? 'OK'
        : 'FAILED';
    const at = new Date().toISOString();

    let completionEvent: EventEnvelope | null = null;
    let failureEvent: EventEnvelope | null = null;
    let finalStatus = '';
    let found = false;

    const session = this.mongo.client.startSession();
    try {
      await session.withTransaction(async () => {
        const current = await this.activations.findOne({ _id: activationId }, { session });
        if (!current) {
          // No existe (replay con correlationId desconocido): no es un error tecnico.
          return;
        }
        found = true;
        finalStatus = current.status;

        const historyEntry = {
          eventType: event.eventType,
          at,
          source: event.source,
          payload: (event.payload ?? {}) as Record<string, unknown>,
        };

        // Estado final: se registra en el historial y se ignora el resultado
        // (RF-06: "ignora resultados posteriores, salvo registrarlos").
        if (current.status === 'ACTIVE' || current.status === 'FAILED') {
          await this.activations.updateOne(
            { _id: activationId },
            { $push: { history: historyEntry }, $set: { updatedAt: at } },
            { session },
          );
          return;
        }

        const steps = { ...current.steps, [step]: { status: stepStatus, at, eventType: event.eventType } };
        let status: ActivationDoc['status'] = current.status === 'PENDING' ? 'IN_PROGRESS' : current.status;
        const terminal: EventEnvelope[] = [];

        if (stepStatus === 'FAILED') {
          status = 'FAILED';
          failureEvent = new ActivationOutcomeFactory().failed(activationId, current.customerId, step, event);
          terminal.push(failureEvent);
        } else if (steps.billing?.status === 'OK' && steps.provisioning?.status === 'OK') {
          status = 'ACTIVE';
          completionEvent = new ActivationOutcomeFactory().completed(activationId, current.customerId, current.planId);
          terminal.push(completionEvent);
        }

        // El evento final (ActivationCompleted/Failed) tambien entra al historial.
        const terminalEntries = terminal.map((e) => ({
          eventType: e.eventType,
          at: e.occurredAt,
          source: e.source,
          payload: (e.payload ?? {}) as Record<string, unknown>,
        }));

        await this.activations.updateOne(
          { _id: activationId },
          {
            $set: { steps, status, updatedAt: at },
            $push: { history: { $each: [historyEntry, ...terminalEntries] } },
          },
          { session },
        );

        for (const e of terminal) {
          await this.outbox.enqueue(TOPICS.ACTIVATION_EVENTS, e, session);
        }
        finalStatus = status;
      });
    } finally {
      await session.endSession();
    }

    if (!found) {
      return;
    }

    this.gateway.broadcast({
      activationId,
      correlationId: activationId,
      eventType: event.eventType,
      occurredAt: at,
      source: event.source,
      status: finalStatus,
      payload: (event.payload ?? {}) as Record<string, unknown>,
    });

    if (completionEvent) {
      this.gateway.broadcast({
        activationId,
        correlationId: activationId,
        eventType: 'ActivationCompleted',
        occurredAt: completionEvent.occurredAt,
        source: 'activation-api',
        status: 'ACTIVE',
        payload: completionEvent.payload as unknown as Record<string, unknown>,
      });
    }
    if (failureEvent) {
      this.gateway.broadcast({
        activationId,
        correlationId: activationId,
        eventType: 'ActivationFailed',
        occurredAt: failureEvent.occurredAt,
        source: 'activation-api',
        status: 'FAILED',
        payload: failureEvent.payload as unknown as Record<string, unknown>,
      });
    }
  }

  async getById(id: string): Promise<ActivationDoc | null> {
    return this.activations.findOne({ _id: id });
  }

  async list(limit = 20): Promise<Array<Record<string, unknown>>> {
    const docs = await this.activations
      .find({})
      .sort({ createdAt: -1 })
      .limit(Math.min(limit, 100))
      .toArray();
    return docs.map((d) => ({
      activationId: d._id,
      customerId: d.customerId,
      planId: d.planId,
      status: d.status,
      simulateFailure: d.simulateFailure,
      createdAt: d.createdAt,
      updatedAt: d.updatedAt,
      events: d.history.length,
    }));
  }
}

/** Factory chico para no ensuciar la logica de la saga. */
class ActivationRequestedFactory {
  create(
    activationId: string,
    customerId: string,
    planId: string,
    channel: string,
    simulateFailure: 'none' | 'billing' | 'provisioning',
  ): EventEnvelope<'ActivationRequested'> {
    return createEvent({
      eventType: 'ActivationRequested',
      correlationId: activationId,
      customerId,
      source: 'activation-api',
      payload: { planId, channel, simulateFailure },
    });
  }
}

class ActivationOutcomeFactory {
  completed(activationId: string, customerId: string, planId: string): EventEnvelope<'ActivationCompleted'> {
    return createEvent({
      eventType: 'ActivationCompleted',
      correlationId: activationId,
      customerId,
      source: 'activation-api',
      payload: { planId },
    });
  }

  failed(
    activationId: string,
    customerId: string,
    failedStep: 'billing' | 'provisioning',
    original: EventEnvelope,
  ): EventEnvelope<'ActivationFailed'> {
    const payload = (original.payload ?? {}) as { reason?: string };
    return createEvent({
      eventType: 'ActivationFailed',
      correlationId: activationId,
      customerId,
      source: 'activation-api',
      payload: { failedStep, reason: payload.reason ?? `${failedStep} fallo` },
    });
  }
}
