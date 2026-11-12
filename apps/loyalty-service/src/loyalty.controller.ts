import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, Payload } from '@nestjs/microservices';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { ReliableConsumer } from '@poc/kafka-toolkit';
import { LoyaltyService } from './loyalty.service';

/**
 * loyalty-svc (Escenario 5) consume unicamente activation.events:
 * grupo nuevo + fromBeginning relea todo el historial sin tocar a los demas.
 */
@Controller()
export class LoyaltyController {
  constructor(
    private readonly loyalty: LoyaltyService,
    private readonly reliable: ReliableConsumer,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async onActivationEvents(@Payload() payload: unknown, @Ctx() context: any): Promise<void> {
    try {
      await this.reliable.handle(context as never, payload, (event: EventEnvelope) =>
        this.loyalty.onActivationOutcome(
          event as EventEnvelope<'ActivationCompleted' | 'ActivationFailed'>,
        ),
      );
    } catch (err) {
      // Nunca se propaga: un error aca romperia el consumo de kafkajs.
      console.error(`[loyalty] error inesperado: ${(err as Error).message}`);
    }
  }
}
