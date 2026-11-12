import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, Payload } from '@nestjs/microservices';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { ReliableConsumer } from '@poc/kafka-toolkit';
import { ActivationsService } from './activations.service';

/**
 * Consume los resultados de billing (billing.events) y provisioning
 * (provisioning.events) y los junta en la saga (RF-04, RF-05, RF-06).
 */
@Controller()
export class KafkaResultsController {
  constructor(
    private readonly activations: ActivationsService,
    private readonly reliable: ReliableConsumer,
  ) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  async onBillingEvent(@Payload() payload: unknown, @Ctx() context: any): Promise<void> {
    await this.handle(payload, context);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async onProvisioningEvent(@Payload() payload: unknown, @Ctx() context: any): Promise<void> {
    await this.handle(payload, context);
  }

  private async handle(payload: unknown, context: unknown): Promise<void> {
    try {
      await this.reliable.handle(context as never, payload, (event: EventEnvelope) =>
        this.activations.applyResult(event),
      );
    } catch (err) {
      // ReliableConsumer nunca lanza; esto es una red de seguridad para no
      // romper el bucle de consumo de kafkajs.
      // eslint-disable-next-line no-console
      console.error(`[activation-api] error inesperado manejando mensaje: ${(err as Error).message}`);
    }
  }
}
