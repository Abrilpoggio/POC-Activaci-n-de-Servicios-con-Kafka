import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, Payload } from '@nestjs/microservices';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { ReliableConsumer } from '@poc/kafka-toolkit';
import { NotificationService } from './notification.service';

/**
 * notification-svc consume:
 *  - activation.events -> envia el mail de bienvenida o de error (RF-07)
 */
@Controller()
export class NotificationController {
  constructor(
    private readonly notification: NotificationService,
    private readonly reliable: ReliableConsumer,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async onActivationEvents(@Payload() payload: unknown, @Ctx() context: any): Promise<void> {
    try {
      await this.reliable.handle(context as never, payload, (event: EventEnvelope) =>
        this.notification.onActivationOutcome(
          event as EventEnvelope<'ActivationCompleted' | 'ActivationFailed'>,
        ),
      );
    } catch (err) {
      // Nunca se propaga: un error aca romperia el consumo de kafkajs.
      console.error(`[notification] error inesperado: ${(err as Error).message}`);
    }
  }
}
