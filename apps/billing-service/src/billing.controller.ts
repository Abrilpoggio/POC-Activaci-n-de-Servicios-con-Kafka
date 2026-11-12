import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, Payload } from '@nestjs/microservices';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { ReliableConsumer } from '@poc/kafka-toolkit';
import { BillingService } from './billing.service';

/**
 * billing-svc consume:
 *  - activation.requested -> crea la cuenta (RF-04)
 *  - activation.events    -> compensa cuando hay un ActivationFailed (RF-06)
 */
@Controller()
export class BillingController {
  constructor(
    private readonly billing: BillingService,
    private readonly reliable: ReliableConsumer,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async onActivationRequested(@Payload() payload: unknown, @Ctx() context: any): Promise<void> {
    try {
      await this.reliable.handle(context as never, payload, (event: EventEnvelope) =>
        this.billing.onActivationRequested(event as EventEnvelope<'ActivationRequested'>),
      );
    } catch (err) {
      console.error(`[billing] error inesperado: ${(err as Error).message}`);
    }
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async onActivationEvents(@Payload() payload: unknown, @Ctx() context: any): Promise<void> {
    try {
      await this.reliable.handle(context as never, payload, (event: EventEnvelope) =>
        this.billing.onActivationOutcome(event as EventEnvelope<'ActivationFailed' | 'ActivationCompleted'>),
      );
    } catch (err) {
      console.error(`[billing] error inesperado: ${(err as Error).message}`);
    }
  }
}
