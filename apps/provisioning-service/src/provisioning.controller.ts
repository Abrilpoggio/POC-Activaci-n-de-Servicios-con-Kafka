import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, Payload } from '@nestjs/microservices';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { ReliableConsumer } from '@poc/kafka-toolkit';
import { ProvisioningService } from './provisioning.service';

/**
 * provisioning-svc consume:
 *  - activation.requested -> aprovisiona el plan y publica el resultado (RF-05)
 */
@Controller()
export class ProvisioningController {
  constructor(
    private readonly provisioning: ProvisioningService,
    private readonly reliable: ReliableConsumer,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async onActivationRequested(@Payload() payload: unknown, @Ctx() context: any): Promise<void> {
    try {
      await this.reliable.handle(context as never, payload, (event: EventEnvelope) =>
        this.provisioning.onActivationRequested(event as EventEnvelope<'ActivationRequested'>),
      );
    } catch (err) {
      // Nunca se propaga: un error aca romperia el consumo de kafkajs.
      console.error(`[provisioning] error inesperado: ${(err as Error).message}`);
    }
  }
}
