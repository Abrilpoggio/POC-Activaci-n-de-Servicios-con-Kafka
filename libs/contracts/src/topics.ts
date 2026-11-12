/**
 * Nombres de topics. Unico lugar donde se definen.
 * Cuatro topics de negocio + sus DLQ (<topic>.dlq).
 * Todas las particiones usan customerId como clave.
 */
export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',
} as const;

export type TopicName = (typeof TOPICS)[keyof typeof TOPICS];

export const ALL_TOPICS: TopicName[] = [
  TOPICS.ACTIVATION_REQUESTED,
  TOPICS.BILLING_EVENTS,
  TOPICS.PROVISIONING_EVENTS,
  TOPICS.ACTIVATION_EVENTS,
];

/** Nombre de la cola DLQ de un topic. */
export const dlqTopic = (topic: string): string => `${topic}.dlq`;
