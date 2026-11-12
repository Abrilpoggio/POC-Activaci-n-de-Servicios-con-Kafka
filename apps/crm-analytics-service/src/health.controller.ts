import { Controller, Get, Query } from '@nestjs/common';
import { CrmService } from './crm.service';

@Controller()
export class HealthController {
  constructor(private readonly crm: CrmService) {}

  @Get('health')
  health(): { ok: boolean; service: string; at: string } {
    return { ok: true, service: 'crm-analytics-service', at: new Date().toISOString() };
  }

  /** Historial de eventos (RF-10): ultimos N, opcionalmente por activacion. */
  @Get('event-log')
  async eventLog(
    @Query('limit') limit?: string,
    @Query('correlationId') correlationId?: string,
  ): Promise<unknown> {
    const n = Math.max(1, Number(limit) || 50);
    return this.crm.listEvents(n, correlationId);
  }

  /** Total de documentos y conteo por tipo de evento. */
  @Get('stats')
  async stats(): Promise<unknown> {
    return this.crm.stats();
  }
}
