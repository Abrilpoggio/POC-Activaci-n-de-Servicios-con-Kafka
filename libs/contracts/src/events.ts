/** Tipos de negocio de la POC. */

export type SimulateFailure = 'none' | 'billing' | 'provisioning';

export type ActivationStatus = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

export type StepStatus = 'OK' | 'FAILED';

/** activation-api -> billing y provisioning */
export interface ActivationRequestedPayload {
  planId: string;
  channel: string;
  simulateFailure: SimulateFailure;
}

/** billing -> activation-api */
export interface BillingAccountCreatedPayload {
  planId: string;
}

/** billing -> activation-api */
export interface BillingFailedPayload {
  reason: string;
}

/** billing -> activation-api (compensacion) */
export interface BillingAccountCancelledPayload {
  reason: string;
}

/** provisioning -> activation-api */
export interface ProvisioningCompletedPayload {
  planId: string;
}

/** provisioning -> activation-api */
export interface ProvisioningFailedPayload {
  reason: string;
}

/** activation-api -> notification, billing, crm */
export interface ActivationCompletedPayload {
  planId: string;
}

/** activation-api -> notification, billing, crm */
export interface ActivationFailedPayload {
  failedStep: 'billing' | 'provisioning';
  reason: string;
}

export type EventType =
  | 'ActivationRequested'
  | 'BillingAccountCreated'
  | 'BillingFailed'
  | 'BillingAccountCancelled'
  | 'ProvisioningCompleted'
  | 'ProvisioningFailed'
  | 'ActivationCompleted'
  | 'ActivationFailed';

export type PayloadByEventType = {
  ActivationRequested: ActivationRequestedPayload;
  BillingAccountCreated: BillingAccountCreatedPayload;
  BillingFailed: BillingFailedPayload;
  BillingAccountCancelled: BillingAccountCancelledPayload;
  ProvisioningCompleted: ProvisioningCompletedPayload;
  ProvisioningFailed: ProvisioningFailedPayload;
  ActivationCompleted: ActivationCompletedPayload;
  ActivationFailed: ActivationFailedPayload;
};

export const EVENT_TYPES: EventType[] = [
  'ActivationRequested',
  'BillingAccountCreated',
  'BillingFailed',
  'BillingAccountCancelled',
  'ProvisioningCompleted',
  'ProvisioningFailed',
  'ActivationCompleted',
  'ActivationFailed',
];

/** Version actual del contrato de cada tipo de evento (RNF-07). */
export const EVENT_VERSIONS: Record<EventType, number> = {
  ActivationRequested: 1,
  BillingAccountCreated: 1,
  BillingFailed: 1,
  BillingAccountCancelled: 1,
  ProvisioningCompleted: 1,
  ProvisioningFailed: 1,
  ActivationCompleted: 1,
  ActivationFailed: 1,
};
