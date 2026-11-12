import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, Payload } from '@nestjs/microservices';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { ReliableConsumer } from '@poc/kafka-toolkit';
import { CrmService } from './crm.service';

/**
 * crm-analytics consume los cuatro topics de negocio y proyecta cada
 * sobre en `event_log` (RF-10: historial consultable + replay).
 */
@Controller()
export class CrmController {
  constructor(
    private readonly crm: CrmService,
    private readonly reliable: ReliableConsumer,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async onActivationRequested(@Payload() payload: unknown, @Ctx() context: any): Promise<void> {
    try {
      await this.reliable.handle(context as never, payload, (event: EventEnvelope) =>
        this.crm.logEvent(event, TOPICS.ACTIVATION_REQUESTED),
      );
    } catch (err) {
      // Nunca se propaga: un error aca romperia el consumo de kafkajs.
      console.error(`[crm] error inesperado: ${(err as Error).message}`);
    }
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  async onBillingEvents(@Payload() payload: unknown, @Ctx() context: any): Promise<void> {
    try {
      await this.reliable.handle(context as never, payload, (event: EventEnvelope) =>
        this.crm.logEvent(event, TOPICS.BILLING_EVENTS),
      );
    } catch (err) {
      console.error(`[crm] error inesperado: ${(err as Error).message}`);
    }
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async onProvisioningEvents(@Payload() payload: unknown, @Ctx() context: any): Promise<void> {
    try {
      await this.reliable.handle(context as never, payload, (event: EventEnvelope) =>
        this.crm.logEvent(event, TOPICS.PROVISIONING_EVENTS),
      );
    } catch (err) {
      console.error(`[crm] error inesperado: ${(err as Error).message}`);
    }
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async onActivationEvents(@Payload() payload: unknown, @Ctx() context: any): Promise<void> {
    try {
      await this.reliable.handle(context as never, payload, (event: EventEnvelope) =>
        this.crm.logEvent(event, TOPICS.ACTIVATION_EVENTS),
      );
    } catch (err) {
      console.error(`[crm] error inesperado: ${(err as Error).message}`);
    }
  }
}
