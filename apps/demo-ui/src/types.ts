/** Estado posible de una activación (saga). */
export type ActivationStatus = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

/** Fallo simulado pedido al contratar. */
export type SimulateFailure = 'none' | 'billing' | 'provisioning';

/** Item resumido del listado GET /activations. */
export interface ActivationSummary {
  activationId: string;
  customerId: string;
  planId: string;
  status: ActivationStatus;
  simulateFailure: string;
  createdAt: string;
  updatedAt: string;
  events: number;
}

/** Paso intermedio (billing / provisioning) del agregador. */
export interface ActivationStep {
  status: 'OK' | 'FAILED';
  at: string;
  eventType: string;
}

/** Entrada del historial persistido. */
export interface HistoryEntry {
  eventType: string;
  at: string;
  source: string;
  payload: Record<string, unknown>;
}

/** Documento completo GET /activations/:id. */
export interface ActivationDoc {
  activationId: string;
  customerId: string;
  planId: string;
  status: ActivationStatus;
  simulateFailure?: string;
  steps?: {
    billing?: ActivationStep;
    provisioning?: ActivationStep;
  };
  history: HistoryEntry[];
  createdAt: string;
  updatedAt: string;
}

/** Payload del evento WebSocket 'timeline' (igual al TimelineGateway). */
export interface TimelineEvent {
  activationId: string;
  correlationId: string;
  eventType: string;
  occurredAt: string;
  source: string;
  status: string;
  payload: Record<string, unknown>;
}

/** Cuerpo del POST /activations. */
export interface CreateActivationBody {
  customerId: string;
  planId: string;
  channel: 'web';
  simulateFailure: SimulateFailure;
}
