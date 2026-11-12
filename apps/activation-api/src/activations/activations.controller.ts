import { BadRequestException, Body, Controller, Get, HttpCode, HttpStatus, NotFoundException, Param, Post, Query } from '@nestjs/common';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { EventPublisher } from '@poc/kafka-toolkit';
import { ActivationsService, CreateActivationDto } from './activations.service';

@Controller()
export class ActivationsController {
  constructor(
    private readonly activations: ActivationsService,
    private readonly publisher: EventPublisher,
  ) {}

  /** RF-02: responde 202 Accepted con activationId y estado PENDING. */
  @Post('activations')
  @HttpCode(HttpStatus.ACCEPTED)
  async create(@Body() body: CreateActivationDto): Promise<{ activationId: string; status: string }> {
    if (!(body?.customerId ?? '').trim() || !(body?.planId ?? '').trim()) {
      throw new BadRequestException('customerId y planId son obligatorios');
    }
    return this.activations.create(body);
  }

  @Get('activations')
  async list(@Query('limit') limit?: string): Promise<Array<Record<string, unknown>>> {
    return this.activations.list(limit ? Number(limit) : 20);
  }

  /** RF-09: estado actual + historial de eventos. */
  @Get('activations/:id')
  async get(@Param('id') id: string): Promise<Record<string, unknown>> {
    const doc = await this.activations.getById(id);
    if (!doc) throw new NotFoundException(`No existe la activacion ${id}`);
    return { ...doc, activationId: doc._id };
  }

  @Get('health')
  health(): { ok: boolean; service: string } {
    return { ok: true, service: 'activation-api' };
  }

  /**
   * Utilidad de demo: publica un sobre invalido (JSON valido pero sin los
   * campos obligatorios) en activation.requested para mostrar la DLQ.
   */
  @Post('dev/poison')
  @HttpCode(HttpStatus.ACCEPTED)
  async poison(@Body() body?: { customerId?: string }): Promise<Record<string, unknown>> {
    const broken = {
      eventType: 'ActivationRequested',
      customerId: body?.customerId ?? 'C-POISON',
      payload: { planId: 'Plan Fantasma' },
      // sin eventId, correlationId, version, source, occurredAt -> invalido
    };
    await this.publisher.publish(TOPICS.ACTIVATION_REQUESTED, broken as unknown as EventEnvelope);
    return {
      published: true,
      topic: TOPICS.ACTIVATION_REQUESTED,
      note: 'sobre sin eventId/correlationId/version/source/occurredAt: debe terminar en activation.requested.dlq',
    };
  }

  /**
   * Utilidad de demo: reenvia un evento ya existente (mismo eventId) para
   * comprobar la idempotencia de los consumidores.
   */
  @Post('dev/replay')
  @HttpCode(HttpStatus.ACCEPTED)
  async replay(@Body() body: { topic?: string; event?: EventEnvelope }): Promise<Record<string, unknown>> {
    if (!body?.event || !body?.topic) {
      throw new BadRequestException('Se necesita { topic, event }');
    }
    await this.publisher.publish(body.topic, body.event);
    return { published: true, topic: body.topic, eventId: body.event.eventId };
  }
}
